import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import { DomainError } from "../src/domain/errors.js";
import {
    orderCreationRepository,
} from "../src/infrastructure/repositories/order-creation.repository.js";
import {
    orderCreationTransaction,
} from "../src/infrastructure/database/order-creation.transaction.js";

const operationIds = [];
let cleanupOrderIds = [];

const schedule = {
    nextAttemptAt: new Date(Date.now() + 24 * 60 * 60 * 1000),
};

const items = [
    { pid: 1, qty: 2, unit_price: "25.50" },
    { pid: 2, qty: 1, unit_price: "10.00" },
];

function locked(operationId, operation) {
    return orderCreationTransaction.withLockedTask(
        operationId,
        operation
    );
}

async function fixture(score = 100, withOrder = false) {
    const operationId = randomUUID();
    operationIds.push(operationId);

    await orderCreationRepository.createTask(
        {
            operationId,
            uid: 13,
            actorRole: "CUSTOMER",
            items,
        },
        schedule
    );

    if (score !== null) {
        await locked(
            operationId,
            async (task, context) => context.freezeScore(score)
        );
    }

    const reservation = {
        operationId,
        uid: 13,
        status: "RESERVED",
        items: items.map((item) => ({ ...item })),
    };

    let order = null;

    if (withOrder) {
        order = await locked(
            operationId,
            async (task, context) =>
                context.commitReservedOrder(reservation, schedule)
        );
    }

    return { reservation, order };
}

function acknowledgment(fixtureData, status, empty = false) {
    return {
        ...fixtureData.reservation,
        status,
        items: empty ? [] : fixtureData.reservation.items,
    };
}

async function taskOf(fixtureData) {
    return orderCreationRepository.findByOperationId(
        fixtureData.reservation.operationId
    );
}

async function expectDomainError(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    const success = await fixture(100, true);
    const confirmed = acknowledgment(success, "CONFIRMED");

    const completedOrder = await locked(
        confirmed.operationId,
        async (task, context) => context.completeOrder(confirmed)
    );

    assert.deepEqual(completedOrder, success.order);

    const completedTask = await taskOf(success);
    assert.equal(completedTask.status, "COMPLETED");

    assert.deepEqual(
        await locked(
            confirmed.operationId,
            async (task, context) => context.completeOrder(confirmed)
        ),
        success.order
    );

    assert.deepEqual(await taskOf(success), completedTask);

    console.log("Confirmed stock completes stored order once: OK");

    const failure = await fixture(100);
    const failureId = failure.reservation.operationId;

    const decision = await locked(
        failureId,
        async (task, context) =>
            context.requestRelease("NOT_FOUND", schedule)
    );

    assert.equal(decision.status, "RELEASE_PENDING");
    assert.equal(decision.failure_code, "NOT_FOUND");

    const repeatedDecision = await locked(
        failureId,
        async (task, context) =>
            context.requestRelease("CONFLICT", schedule)
    );

    assert.deepEqual(repeatedDecision, decision);

    await expectDomainError(
        () => locked(
            failureId,
            async (task, context) =>
                context.commitReservedOrder(
                    failure.reservation,
                    schedule
                )
        ),
        "CONFLICT"
    );

    console.log("Durable release decision preserves first failure: OK");

    const released = acknowledgment(failure, "RELEASED", true);

    const failed = await locked(
        failureId,
        async (task, context) => context.finishRelease(released)
    );

    assert.equal(failed.status, "FAILED");
    assert.equal(failed.failure_code, "NOT_FOUND");

    assert.deepEqual(
        await locked(
            failureId,
            async (task, context) => context.finishRelease(released)
        ),
        failed
    );

    assert.deepEqual(await taskOf(failure), failed);

    console.log("Released stock closes failed task once: OK");

    const pending = await fixture(500, true);
    const pendingId = pending.reservation.operationId;

    await expectDomainError(
        () => locked(
            pendingId,
            async (task, context) =>
                context.requestRelease("NOT_FOUND", schedule)
        ),
        "CONFLICT"
    );

    await expectDomainError(
        () => locked(
            pendingId,
            async (task, context) =>
                context.finishRelease(
                    acknowledgment(pending, "RELEASED")
                )
        ),
        "CONFLICT"
    );

    for (const [badAck, code] of [
        [
            {
                ...acknowledgment(pending, "CONFIRMED"),
                uid: 14,
            },
            "FORBIDDEN",
        ],
        [acknowledgment(pending, "RESERVED"), "CONFLICT"],
        [
            {
                ...acknowledgment(pending, "CONFIRMED"),
                items: items.map((item) => ({
                    ...item,
                    unit_price: "999.00",
                })),
            },
            "CONFLICT",
        ],
    ]) {
        await expectDomainError(
            () => locked(
                pendingId,
                async (task, context) =>
                    context.completeOrder(badAck)
            ),
            code
        );
    }

    const transient = await fixture(null);
    const transientId = transient.reservation.operationId;

    await expectDomainError(
        () => locked(
            transientId,
            async (task, context) =>
                context.requestRelease(
                    "SERVICE_UNAVAILABLE",
                    schedule
                )
        ),
        "INVALID_ARGUMENT"
    );

    assert.equal((await taskOf(transient)).status, "PROCESSING");
    assert.equal((await taskOf(pending)).status, "CONFIRM_PENDING");

    console.log("Stored orders and transient errors cannot enter release: OK");

    const completeFailure = new Error("Simulated completion failure");

    await assert.rejects(
        () => locked(
            pendingId,
            async (task, context) => {
                await context.completeOrder(
                    acknowledgment(pending, "CONFIRMED")
                );

                throw completeFailure;
            }
        ),
        (error) => error === completeFailure
    );

    assert.equal((await taskOf(pending)).status, "CONFIRM_PENDING");

    // A release decision must commit before finalization.
    await expectDomainError(
        () => locked(
            transientId,
            async (task, context) => {
                await context.requestRelease("NOT_FOUND", schedule);

                return context.finishRelease(
                    acknowledgment(transient, "RELEASED", true)
                );
            }
        ),
        "CONFLICT"
    );

    assert.equal((await taskOf(transient)).status, "PROCESSING");

    // Order commit and completion must use separate transactions.
    const early = await fixture(100);
    const earlyId = early.reservation.operationId;

    await expectDomainError(
        () => locked(
            earlyId,
            async (task, context) => {
                await context.commitReservedOrder(
                    early.reservation,
                    schedule
                );

                return context.completeOrder(
                    acknowledgment(early, "CONFIRMED")
                );
            }
        ),
        "CONFLICT"
    );

    assert.equal((await taskOf(early)).status, "PROCESSING");

    const earlyOrder = await pool.query(
        "SELECT COUNT(*)::int AS count FROM orders WHERE operation_id = $1",
        [earlyId]
    );

    assert.equal(earlyOrder.rows[0].count, 0);

    await locked(
        transientId,
        async (task, context) =>
            context.requestRelease("NOT_FOUND", schedule)
    );

    const releaseFailure = new Error("Simulated release finalization failure");

    await assert.rejects(
        () => locked(
            transientId,
            async (task, context) => {
                await context.finishRelease(
                    acknowledgment(transient, "RELEASED", true)
                );

                throw releaseFailure;
            }
        ),
        (error) => error === releaseFailure
    );

    const recoverable = await taskOf(transient);

    assert.equal(recoverable.status, "RELEASE_PENDING");
    assert.equal(recoverable.failure_code, "NOT_FOUND");

    console.log("Finalization failures keep recoverable task states: OK");
}

async function cleanup() {
    const client = await pool.connect();
    let releaseError;

    try {
        await client.query("BEGIN");

        const selected = await client.query(
            `
            SELECT oid
            FROM orders
            WHERE operation_id = ANY($1::uuid[])
            `,
            [operationIds]
        );

        cleanupOrderIds = selected.rows.map((row) => row.oid);

        await client.query(
            "DELETE FROM outbox_events WHERE oid = ANY($1::int[])",
            [cleanupOrderIds]
        );

        await client.query(
            "DELETE FROM order_details WHERE oid = ANY($1::int[])",
            [cleanupOrderIds]
        );

        await client.query(
            "DELETE FROM orders WHERE oid = ANY($1::int[])",
            [cleanupOrderIds]
        );

        await client.query(
            `
            DELETE FROM order_creation_tasks
            WHERE operation_id = ANY($1::uuid[])
            `,
            [operationIds]
        );

        await client.query("COMMIT");
    } catch (error) {
        try {
            await client.query("ROLLBACK");
        } catch (rollbackError) {
            releaseError = rollbackError;
        }

        throw error;
    } finally {
        client.release(releaseError);
    }

    const remaining = await pool.query(
        `
        SELECT
            (SELECT COUNT(*)::int FROM order_creation_tasks
             WHERE operation_id = ANY($1::uuid[])) AS tasks_count,
            (SELECT COUNT(*)::int FROM orders
             WHERE operation_id = ANY($1::uuid[])) AS orders_count,
            (SELECT COUNT(*)::int FROM order_details
             WHERE oid = ANY($2::int[])) AS details_count,
            (SELECT COUNT(*)::int FROM outbox_events
             WHERE oid = ANY($2::int[])) AS events_count
        `,
        [operationIds, cleanupOrderIds]
    );

    assert.deepEqual(remaining.rows[0], {
        tasks_count: 0,
        orders_count: 0,
        details_count: 0,
        events_count: 0,
    });
}

try {
    await main();
} catch (error) {
    console.error(
        "Creation lifecycle check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        await cleanup();

        console.log("Scoped lifecycle fixtures removed: OK");
    } catch (error) {
        console.error(
            "Lifecycle fixture cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture operation IDs:", operationIds);
        console.error("Fixture order IDs:", cleanupOrderIds);
        process.exitCode = 1;
    } finally {
        await pool.end();
    }
}