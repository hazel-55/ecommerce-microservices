import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import { DomainError } from "../src/domain/errors.js";
import {
    createOrderCreationRepository,
} from "../src/infrastructure/repositories/order-creation.repository.js";

const operationIds = [];

const schedule = {
    nextAttemptAt: new Date(Date.now() + 24 * 60 * 60 * 1000),
};

const repository = createOrderCreationRepository();

function newOperationId() {
    const id = randomUUID();
    operationIds.push(id);
    return id;
}

async function expectDomainError(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    const input = {
        operationId: newOperationId(),
        uid: 13,
        actorRole: "CUSTOMER",
        score: 999,
        status: "COMPLETED",
        items: [
            { pid: 2, qty: 1, unit_price: "0.00" },
            { pid: 1, qty: 2 },
        ],
    };

    const originalInput = JSON.stringify(input);

    const created = await repository.createTask(input, schedule);

    assert.equal(created.operation_id, input.operationId);
    assert.equal(created.uid, 13);
    assert.equal(created.actor_role, "CUSTOMER");

    assert.deepEqual(created.items, [
        { pid: 1, qty: 2 },
        { pid: 2, qty: 1 },
    ]);

    assert.equal(created.score, null);
    assert.equal(created.status, "PROCESSING");
    assert.equal(created.attempts, 0);
    assert.equal(created.failure_code, null);
    assert.equal(created.last_error_code, null);

    assert.equal(
        created.next_attempt_at.getTime(),
        schedule.nextAttemptAt.getTime()
    );

    assert.equal(JSON.stringify(input), originalInput);

    console.log("Creation task insert and normalized snapshot: OK");

    const replay = await repository.createTask(
        {
            ...input,
            items: [...input.items].reverse(),
        },
        schedule
    );

    assert.deepEqual(replay, created);

    await expectDomainError(
        () => repository.createTask(
            { ...input, uid: 14 },
            schedule
        ),
        "FORBIDDEN"
    );

    await expectDomainError(
        () => repository.createTask(
            { ...input, actorRole: "ADMIN" },
            schedule
        ),
        "CONFLICT"
    );

    await expectDomainError(
        () => repository.createTask(
            {
                ...input,
                items: [{ pid: 1, qty: 3 }],
            },
            schedule
        ),
        "CONFLICT"
    );

    assert.deepEqual(
        await repository.findByOperationId(input.operationId),
        created
    );

    console.log("Creation task replay and request binding: OK");

    const reader = createOrderCreationRepository();

    assert.deepEqual(
        await reader.findByOperationId(input.operationId),
        created
    );

    assert.equal(
        await reader.findByOperationId(newOperationId()),
        null
    );

    await expectDomainError(
        () => reader.findByOperationId("invalid"),
        "INVALID_ARGUMENT"
    );

    console.log("Creation task persists across repository instances: OK");

    const concurrentInput = {
        operationId: newOperationId(),
        uid: 13,
        actorRole: "CUSTOMER",
        items: [{ pid: 3, qty: 1 }],
    };

    const results = await Promise.allSettled(
        Array.from(
            { length: 8 },
            () => repository.createTask(concurrentInput, schedule)
        )
    );

    for (const result of results) {
        if (result.status === "rejected") {
            throw result.reason;
        }

        assert.deepEqual(result.value, results[0].value);
    }

    const count = await pool.query(
        `
        SELECT COUNT(*)::int AS count
        FROM order_creation_tasks
        WHERE operation_id = $1
        `,
        [concurrentInput.operationId]
    );

    assert.equal(count.rows[0].count, 1);

    console.log("Concurrent creation uses one task: OK");
}

async function cleanup() {
    await pool.query(
        `
        DELETE FROM order_creation_tasks
        WHERE operation_id = ANY($1::uuid[])
        `,
        [operationIds]
    );

    const remaining = await pool.query(
        `
        SELECT COUNT(*)::int AS count
        FROM order_creation_tasks
        WHERE operation_id = ANY($1::uuid[])
        `,
        [operationIds]
    );

    assert.equal(remaining.rows[0].count, 0);
}

try {
    await main();
} catch (error) {
    console.error(
        "Creation task repository check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        await cleanup();

        console.log("Scoped creation task fixtures removed: OK");
    } catch (error) {
        console.error(
            "Creation task fixture cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture operation IDs:", operationIds);
        process.exitCode = 1;
    } finally {
        await pool.end();
    }
}