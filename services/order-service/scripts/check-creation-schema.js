import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";

const operationId = randomUUID();

let client;
let transactionActive = false;

async function expectSqlError(sql, values, code, constraint) {
    await client.query("SAVEPOINT schema_guard");

    try {
        await assert.rejects(
            () => client.query(sql, values),
            (error) => {
                assert.equal(error.code, code);

                if (constraint) {
                    assert.equal(error.constraint, constraint);
                }

                return true;
            }
        );
    } finally {
        await client.query("ROLLBACK TO SAVEPOINT schema_guard");
        await client.query("RELEASE SAVEPOINT schema_guard");
    }
}

async function main() {
    const columns = await pool.query(`
        SELECT
            operation_id,
            uid,
            actor_role,
            items,
            score,
            status,
            failure_code,
            attempts,
            last_error_code,
            next_attempt_at,
            created_at,
            updated_at
        FROM order_creation_tasks
        LIMIT 0
    `);

    assert.deepEqual(
        columns.fields.map((field) => field.name),
        [
            "operation_id",
            "uid",
            "actor_role",
            "items",
            "score",
            "status",
            "failure_code",
            "attempts",
            "last_error_code",
            "next_attempt_at",
            "created_at",
            "updated_at",
        ]
    );

    client = await pool.connect();

    await client.query("BEGIN");
    transactionActive = true;

    const items = [{ pid: 1, qty: 2 }];

    const inserted = await client.query(
        `
        INSERT INTO order_creation_tasks (
            operation_id,
            uid,
            actor_role,
            items
        )
        VALUES ($1, $2, $3, $4::jsonb)
        RETURNING *
        `,
        [
            operationId,
            13,
            "CUSTOMER",
            JSON.stringify(items),
        ]
    );

    const task = inserted.rows[0];

    assert.equal(task.operation_id, operationId);
    assert.equal(task.uid, 13);
    assert.equal(task.actor_role, "CUSTOMER");
    assert.deepEqual(task.items, items);
    assert.equal(task.score, null);
    assert.equal(task.status, "PROCESSING");
    assert.equal(task.failure_code, null);
    assert.equal(task.attempts, 0);
    assert.equal(task.last_error_code, null);
    assert.ok(task.next_attempt_at instanceof Date);
    assert.ok(task.created_at instanceof Date);
    assert.ok(task.updated_at instanceof Date);

    console.log("Order creation task schema and defaults: OK");

    await client.query(
        `
        INSERT INTO orders (uid, discount, operation_id)
        VALUES ($1, $2, $3)
        `,
        [13, "0.00", operationId]
    );

    await expectSqlError(
        `
        INSERT INTO orders (uid, discount, operation_id)
        VALUES ($1, $2, $3)
        `,
        [13, "0.00", operationId],
        "23505",
        "orders_operation_id_unique"
    );

    await expectSqlError(
        `
        INSERT INTO orders (uid, discount, operation_id)
        VALUES ($1, $2, $3)
        `,
        [13, "0.00", randomUUID()],
        "23503"
    );

    console.log("Order operation ID uniqueness and foreign key: OK");

    const index = await client.query(`
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = 'order_creation_tasks'
          AND indexname = 'order_creation_tasks_pending_idx'
    `);

    assert.equal(index.rowCount, 1);

    const definition = index.rows[0].indexdef;

    assert.match(
        definition,
        /next_attempt_at,\s*created_at/i
    );

    assert.match(definition, /WHERE/i);

    for (const status of [
        "PROCESSING",
        "CONFIRM_PENDING",
        "RELEASE_PENDING",
    ]) {
        assert.ok(definition.includes(status));
    }

    assert.ok(!definition.includes("COMPLETED"));
    assert.ok(!definition.includes("FAILED"));

    console.log("Pending creation task index: OK");

    const invalidTaskSql = `
        INSERT INTO order_creation_tasks (
            operation_id,
            uid,
            actor_role,
            items,
            attempts
        )
        VALUES ($1, $2, $3, $4::jsonb, $5)
    `;

    for (const [uid, role, invalidItems, attempts] of [
        [0, "CUSTOMER", items, 0],
        [13, "UNKNOWN", items, 0],
        [13, "CUSTOMER", [], 0],
        [13, "CUSTOMER", items, -1],
    ]) {
        await expectSqlError(
            invalidTaskSql,
            [
                randomUUID(),
                uid,
                role,
                JSON.stringify(invalidItems),
                attempts,
            ],
            "23514"
        );
    }

    console.log("Invalid task data rejected: OK");

    await client.query("ROLLBACK");
    transactionActive = false;

    const remaining = await client.query(
        `
        SELECT
            (SELECT COUNT(*)::int
             FROM order_creation_tasks
             WHERE operation_id = $1) AS tasks_count,
            (SELECT COUNT(*)::int
             FROM orders
             WHERE operation_id = $1) AS orders_count
        `,
        [operationId]
    );

    assert.deepEqual(remaining.rows[0], {
        tasks_count: 0,
        orders_count: 0,
    });

    console.log("Rollback removes task schema fixtures: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Creation task schema check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    if (client) {
        let releaseError;

        if (transactionActive) {
            try {
                await client.query("ROLLBACK");
            } catch (error) {
                releaseError = error;
                process.exitCode = 1;

                console.error(
                    "Schema fixture rollback failed:",
                    error.code ?? error.message
                );
            }
        }

        client.release(releaseError);
    }

    await pool.end();
}