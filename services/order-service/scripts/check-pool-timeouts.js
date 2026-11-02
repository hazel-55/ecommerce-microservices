import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    createOrderCreationRepository,
} from "../src/infrastructure/repositories/order-creation.repository.js";
import {
    createOrderCreationTransaction,
} from "../src/infrastructure/database/order-creation.transaction.js";

const operationId = randomUUID();
const items = [{ pid: 1, qty: 1 }];

const nextAttemptAt = new Date(
    Date.now() + 24 * 60 * 60 * 1000,
);

let session;
let holder;
let sessionTransaction = false;
let holderTransaction = false;

async function readSettings(client) {
    const result = await client.query(
        `
        SELECT name, setting::integer AS milliseconds
        FROM pg_settings
        WHERE name = ANY($1::text[])
        `,
        [["statement_timeout", "lock_timeout"]],
    );

    return Object.fromEntries(
        result.rows.map((row) => [
            row.name,
            row.milliseconds,
        ]),
    );
}

async function expectSqlState(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    session = await pool.connect();

    const defaults = {
        statement_timeout: 5000,
        lock_timeout: 2000,
    };

    assert.deepEqual(
        await readSettings(session),
        defaults,
    );

    console.log("Order pool SQL timeout defaults: OK");

    // Không mở transaction: kiểm tra timeout của câu SQL độc lập.
    await expectSqlState(
        () => session.query("SELECT pg_sleep(10)"),
        "57014",
    );

    const healthy = await session.query(
        "SELECT 1 AS value",
    );

    assert.equal(healthy.rows[0].value, 1);

    console.log("Autocommit statement timeout cancels slow SQL: OK");

    const repository = createOrderCreationRepository({
        database: {
            query(...args) {
                return session.query(...args);
            },
        },
    });

    holder = await pool.connect();

    await holder.query("BEGIN");
    holderTransaction = true;

    await holder.query(
        `
        INSERT INTO order_creation_tasks (
            operation_id,
            uid,
            actor_role,
            items,
            next_attempt_at
        )
        VALUES ($1, 13, 'CUSTOMER', $2::jsonb, $3)
        `,
        [
            operationId,
            JSON.stringify(items),
            nextAttemptAt,
        ],
    );

    try {
        // INSERT cùng operation ID phải chờ transaction holder.
        await expectSqlState(
            () => repository.createTask(
                {
                    operationId,
                    uid: 13,
                    actorRole: "CUSTOMER",
                    items,
                },
                { nextAttemptAt },
            ),
            "55P03",
        );
    } finally {
        await holder.query("ROLLBACK");
        holderTransaction = false;
    }

    assert.equal(
        await repository.findByOperationId(operationId),
        null,
    );

    console.log("Contended task insert has bounded lock wait: OK");

    const task = await repository.createTask(
        {
            operationId,
            uid: 13,
            actorRole: "CUSTOMER",
            items,
        },
        { nextAttemptAt },
    );

    assert.equal(task.operation_id, operationId);
    assert.equal(task.uid, 13);
    assert.equal(task.status, "PROCESSING");
    assert.equal(task.score, null);
    assert.deepEqual(task.items, items);

    const count = await session.query(
        `
        SELECT COUNT(*)::integer AS count
        FROM order_creation_tasks
        WHERE operation_id = $1
        `,
        [operationId],
    );

    assert.equal(count.rows[0].count, 1);

    console.log("Same operation remains usable after lock failure: OK");

    const releases = [];

    const transactions = createOrderCreationTransaction({
        lockTimeoutMs: 50,
        statementTimeoutMs: 100,

        database: {
            async connect() {
                return {
                    async query(...args) {
                        const result = await session.query(...args);

                        if (args[0] === "BEGIN") {
                            sessionTransaction = true;
                        }

                        if (
                            args[0] === "COMMIT" ||
                            args[0] === "ROLLBACK"
                        ) {
                            sessionTransaction = false;
                        }

                        return result;
                    },

                    release(error) {
                        // Giữ connection để kiểm tra sau COMMIT.
                        // finally sẽ trả connection thật về pool.
                        releases.push(error);
                    },
                };
            },
        },
    });

    await transactions.withLockedTask(
        operationId,
        async () => {
            assert.deepEqual(
                await readSettings(session),
                {
                    statement_timeout: 100,
                    lock_timeout: 50,
                },
            );
        },
    );

    assert.deepEqual(
        await readSettings(session),
        defaults,
    );

    assert.equal(releases.length, 1);
    assert.equal(releases[0], undefined);

    console.log("Creation transaction overrides restore pool defaults: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Order pool timeout check failed:",
        error.code ?? error.name,
    );

    process.exitCode = 1;
} finally {
    if (holder) {
        let releaseError;

        try {
            if (holderTransaction) {
                await holder.query("ROLLBACK");
            }
        } catch (error) {
            releaseError = error;
            process.exitCode = 1;
        } finally {
            holder.release(releaseError);
        }
    }

    if (session) {
        let releaseError;

        try {
            if (sessionTransaction) {
                await session.query("ROLLBACK");
            }
        } catch (error) {
            releaseError = error;
            process.exitCode = 1;
        } finally {
            session.release(releaseError);
        }
    }

    try {
        await pool.query(
            `
            DELETE FROM order_creation_tasks
            WHERE operation_id = $1
            `,
            [operationId],
        );

        const remaining = await pool.query(
            `
            SELECT COUNT(*)::integer AS count
            FROM order_creation_tasks
            WHERE operation_id = $1
            `,
            [operationId],
        );

        assert.equal(remaining.rows[0].count, 0);

        console.log("Scoped pool timeout fixtures removed: OK");
    } catch (error) {
        console.error(
            "Pool timeout fixture cleanup failed:",
            error.code ?? error.name,
        );

        console.error("Fixture operation ID:", operationId);
        process.exitCode = 1;
    } finally {
        await pool.end();
    }
}