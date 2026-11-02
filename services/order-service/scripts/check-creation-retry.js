import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import { DomainError } from "../src/domain/errors.js";
import {
    creationRetryDelaySeconds,
} from "../src/domain/order-creation-retry.rules.js";
import {
    orderCreationRepository,
} from "../src/infrastructure/repositories/order-creation.repository.js";
import {
    orderCreationTransaction,
} from "../src/infrastructure/database/order-creation.transaction.js";

const operationIds = [];
const cleanupOrderIds = [];

const future = new Date(Date.now() + 86_400_000);

const items = [
    { pid: 1, qty: 2, unit_price: "25.50" },
    { pid: 2, qty: 1, unit_price: "10.00" },
];

function locked(operationId, operation) {
    return orderCreationTransaction.withLockedTask(
        operationId,
        operation,
    );
}

function taskOf(operationId) {
    return orderCreationRepository.findByOperationId(
        operationId,
    );
}

async function expectDomainError(operation, code) {
    await assert.rejects(
        operation,
        (error) =>
            error instanceof DomainError &&
            error.code === code,
    );
}

function businessFields(task) {
    const copy = structuredClone(task);

    delete copy.attempts;
    delete copy.last_error_code;
    delete copy.next_attempt_at;
    delete copy.updated_at;

    return copy;
}

async function fixture(status = "PROCESSING") {
    const operationId = randomUUID();
    operationIds.push(operationId);

    await orderCreationRepository.createTask(
        {
            operationId,
            uid: 13,
            actorRole: "CUSTOMER",
            items,
        },
        { nextAttemptAt: future },
    );

    const reservation = {
        operationId,
        uid: 13,
        status: "RESERVED",
        items: structuredClone(items),
    };

    if (
        status === "CONFIRM_PENDING" ||
        status === "COMPLETED"
    ) {
        await locked(
            operationId,
            (_, context) => context.freezeScore(100),
        );

        await locked(
            operationId,
            (_, context) => context.commitReservedOrder(
                reservation,
                { nextAttemptAt: future },
            ),
        );

        if (status === "COMPLETED") {
            await locked(
                operationId,
                (_, context) => context.completeOrder({
                    ...reservation,
                    status: "CONFIRMED",
                }),
            );
        }
    }

    if (
        status === "RELEASE_PENDING" ||
        status === "FAILED"
    ) {
        await locked(
            operationId,
            (_, context) => context.requestRelease(
                "NOT_FOUND",
                { nextAttemptAt: future },
            ),
        );

        if (status === "FAILED") {
            await locked(
                operationId,
                (_, context) => context.finishRelease({
                    operationId,
                    uid: 13,
                    status: "RELEASED",
                    items: [],
                }),
            );
        }
    }

    return operationId;
}

async function aggregateSnapshot(operationId) {
    const order = await pool.query(
        `
        SELECT *
        FROM orders
        WHERE operation_id = $1::uuid
        `,
        [operationId],
    );

    const details = await pool.query(
        `
        SELECT d.*
        FROM order_details d
        JOIN orders o ON o.oid = d.oid
        WHERE o.operation_id = $1::uuid
        ORDER BY d.pid
        `,
        [operationId],
    );

    const outbox = await pool.query(
        `
        SELECT e.*
        FROM outbox_events e
        JOIN orders o ON o.oid = e.oid
        WHERE o.operation_id = $1::uuid
        ORDER BY e.event_id
        `,
        [operationId],
    );

    return {
        orders: order.rows,
        details: details.rows,
        outbox: outbox.rows,
    };
}

async function main() {
    const processing = await fixture();
    const confirming = await fixture("CONFIRM_PENDING");
    const releasing = await fixture("RELEASE_PENDING");

    const before = new Map();

    for (const id of [
        processing,
        confirming,
        releasing,
    ]) {
        before.set(id, businessFields(await taskOf(id)));
    }

    const aggregateBefore = await aggregateSnapshot(
        confirming,
    );

    for (const id of [
        processing,
        confirming,
        releasing,
    ]) {
        const updated = await locked(
            id,
            (_, context) => context.scheduleRetry(
                "SERVICE_UNAVAILABLE",
                { now: future },
            ),
        );

        assert.equal(updated.attempts, 1);
        assert.equal(
            updated.last_error_code,
            "SERVICE_UNAVAILABLE",
        );
        assert.equal(
            updated.next_attempt_at.getTime(),
            future.getTime() + 1000,
        );
    }

    const delays = [1, 2, 4, 8, 16, 32, 60, 60];

    for (let index = 1; index < delays.length; index++) {
        const updated = await locked(
            processing,
            (_, context) => context.scheduleRetry(
                "INTERNAL",
                { now: future },
            ),
        );

        assert.equal(updated.attempts, index + 1);
        assert.equal(
            updated.next_attempt_at.getTime(),
            future.getTime() + delays[index] * 1000,
        );
    }

    console.log(
        "Pending task retry metadata and bounded backoff: OK",
    );

    for (const id of [
        processing,
        confirming,
        releasing,
    ]) {
        assert.deepEqual(
            businessFields(await taskOf(id)),
            before.get(id),
        );
    }

    assert.deepEqual(
        await aggregateSnapshot(confirming),
        aggregateBefore,
    );

    assert.equal(
        (await taskOf(releasing)).failure_code,
        "NOT_FOUND",
    );

    console.log(
        "Retry scheduling preserves task decisions and order data: OK",
    );

    for (const status of ["COMPLETED", "FAILED"]) {
        const id = await fixture(status);
        const original = await taskOf(id);

        await expectDomainError(
            () => locked(
                id,
                (_, context) => context.scheduleRetry(
                    "INTERNAL",
                    { now: future },
                ),
            ),
            "CONFLICT",
        );

        assert.deepEqual(await taskOf(id), original);
    }

    const original = await taskOf(processing);

    await expectDomainError(
        () => locked(
            processing,
            (_, context) => context.scheduleRetry(
                "ECONNRESET",
            ),
        ),
        "INVALID_ARGUMENT",
    );

    await expectDomainError(
        () => locked(
            processing,
            (_, context) => context.scheduleRetry(
                "INTERNAL",
                { now: new Date("invalid") },
            ),
        ),
        "INVALID_ARGUMENT",
    );

    for (const attempt of [0, -1, 1.5, 2147483648]) {
        assert.throws(
            () => creationRetryDelaySeconds(attempt),
            (error) =>
                error instanceof DomainError &&
                error.code === "INVALID_ARGUMENT",
        );
    }

    assert.deepEqual(
        await taskOf(processing),
        original,
    );

    console.log(
        "Terminal tasks and invalid retry data rejected: OK",
    );

    const concurrent = await fixture();

    await Promise.all([
        locked(
            concurrent,
            (_, context) => context.scheduleRetry(
                "INTERNAL",
                { now: future },
            ),
        ),
        locked(
            concurrent,
            (_, context) => context.scheduleRetry(
                "SERVICE_UNAVAILABLE",
                { now: future },
            ),
        ),
    ]);

    assert.equal(
        (await taskOf(concurrent)).attempts,
        2,
    );

    await pool.query(
        `
        UPDATE order_creation_tasks
        SET attempts = 2147483647
        WHERE operation_id = $1::uuid
        `,
        [concurrent],
    );

    for (let index = 0; index < 2; index++) {
        const updated = await locked(
            concurrent,
            (_, context) => context.scheduleRetry(
                "INTERNAL",
                { now: future },
            ),
        );

        assert.equal(updated.attempts, 2147483647);
        assert.equal(
            updated.next_attempt_at.getTime(),
            future.getTime() + 60_000,
        );
    }

    console.log(
        "Concurrent retry updates and attempt bound: OK",
    );

    const beforeRollback = await taskOf(processing);
    const marker = new Error("TEST_ROLLBACK");

    await assert.rejects(
        () => locked(
            processing,
            async (_, context) => {
                await context.scheduleRetry(
                    "INTERNAL",
                    { now: future },
                );

                throw marker;
            },
        ),
        (error) => error === marker,
    );

    assert.deepEqual(
        await taskOf(processing),
        beforeRollback,
    );

    let closedContext;

    await locked(processing, (_, context) => {
        closedContext = context;
    });

    await expectDomainError(
        () => closedContext.scheduleRetry("INTERNAL"),
        "INTERNAL",
    );

    console.log(
        "Retry transaction rollback and closed context: OK",
    );
}

async function cleanup() {
    if (operationIds.length === 0) {
        return;
    }

    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        const selected = await client.query(
            `
            SELECT oid
            FROM orders
            WHERE operation_id = ANY($1::uuid[])
            `,
            [operationIds],
        );

        cleanupOrderIds.push(
            ...selected.rows.map((row) => row.oid),
        );

        await client.query(
            `DELETE FROM outbox_events
             WHERE oid = ANY($1::integer[])`,
            [cleanupOrderIds],
        );

        await client.query(
            `DELETE FROM order_details
             WHERE oid = ANY($1::integer[])`,
            [cleanupOrderIds],
        );

        await client.query(
            `DELETE FROM orders
             WHERE operation_id = ANY($1::uuid[])`,
            [operationIds],
        );

        await client.query(
            `DELETE FROM order_creation_tasks
             WHERE operation_id = ANY($1::uuid[])`,
            [operationIds],
        );

        await client.query("COMMIT");
    } catch (error) {
        await client.query("ROLLBACK");
        throw error;
    } finally {
        client.release();
    }

    const remaining = await pool.query(
        `
        SELECT
            (SELECT COUNT(*) FROM order_creation_tasks
             WHERE operation_id = ANY($1::uuid[]))
            +
            (SELECT COUNT(*) FROM orders
             WHERE operation_id = ANY($1::uuid[]))
            +
            (SELECT COUNT(*) FROM order_details
             WHERE oid = ANY($2::integer[]))
            +
            (SELECT COUNT(*) FROM outbox_events
             WHERE oid = ANY($2::integer[]))
            AS count
        `,
        [operationIds, cleanupOrderIds],
    );

    assert.equal(Number(remaining.rows[0].count), 0);

    console.log("Scoped retry fixtures removed: OK");
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Creation retry check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    try {
        await cleanup();
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Creation retry cleanup failed:",
            error.code ?? error.name,
        );
    }

    await pool.end();
}