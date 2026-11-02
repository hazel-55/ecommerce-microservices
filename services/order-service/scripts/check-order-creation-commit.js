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

const defaultItems = [
    { pid: 1, qty: 2, unit_price: "25.50" },
    { pid: 2, qty: 1, unit_price: "10.00" },
];

async function fixture(score, items = defaultItems) {
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
        await orderCreationTransaction.withLockedTask(
            operationId,
            async (task, context) => context.freezeScore(score)
        );
    }

    return {
        operationId,
        uid: 13,
        status: "RESERVED",
        items: items.map((item) => ({ ...item })),
    };
}

function commit(reservation) {
    return orderCreationTransaction.withLockedTask(
        reservation.operationId,
        async (task, context) =>
            context.commitReservedOrder(reservation, schedule)
    );
}

async function expectDomainError(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function assertCounts(operationId, orders, details) {
    const result = await pool.query(
        `
        SELECT
            (SELECT COUNT(*)::int FROM orders
             WHERE operation_id = $1) AS orders_count,

            (SELECT COUNT(*)::int
             FROM order_details d
             JOIN orders o ON o.oid = d.oid
             WHERE o.operation_id = $1) AS details_count,

            (SELECT COUNT(*)::int FROM outbox_events
             WHERE event_id = $1
               AND event_type = 'OrderCreated') AS events_count
        `,
        [operationId]
    );

    assert.deepEqual(result.rows[0], {
        orders_count: orders,
        details_count: details,
        events_count: orders,
    });
}

async function main() {
    const first = await fixture(100);
    const created = await commit(first);

    assert.deepEqual(created, {
        oid: created.oid,
        uid: 13,
        discount: "3.05",
        items: defaultItems,
    });

    const task = await orderCreationRepository.findByOperationId(
        first.operationId
    );

    assert.equal(task.status, "CONFIRM_PENDING");
    assert.equal(task.score, 100);
    assert.equal(
        task.next_attempt_at.getTime(),
        schedule.nextAttemptAt.getTime()
    );

    const event = await pool.query(
        "SELECT * FROM outbox_events WHERE event_id = $1",
        [first.operationId]
    );

    assert.equal(event.rowCount, 1);
    assert.equal(event.rows[0].published_at, null);
    assert.equal(
        event.rows[0].next_attempt_at.getTime(),
        schedule.nextAttemptAt.getTime()
    );

    assert.deepEqual(event.rows[0].payload, {
        eventId: first.operationId,
        eventType: "OrderCreated",
        version: 1,
        oid: created.oid,
        uid: 13,
        discount: "3.05",
        items: defaultItems,
    });

    await assertCounts(first.operationId, 1, 2);

    console.log("Atomic order, details, outbox and task transition: OK");

    const replay = await commit({
        ...first,
        status: "CONFIRMED",
        items: first.items.map((item) => ({
            ...item,
            unit_price: "999.00",
        })),
    });

    assert.deepEqual(replay, created);
    await assertCounts(first.operationId, 1, 2);

    const concurrent = await fixture(
        500,
        [{ pid: 3, qty: 1, unit_price: "100.00" }]
    );

    const results = await Promise.allSettled([
        commit(concurrent),
        commit(concurrent),
    ]);

    for (const result of results) {
        if (result.status === "rejected") {
            throw result.reason;
        }
    }

    assert.deepEqual(results[0].value, results[1].value);
    assert.equal(results[0].value.discount, "10.00");

    await assertCounts(concurrent.operationId, 1, 1);

    console.log("Order creation replay and concurrency: OK");

    const collision = await fixture(100);

    // Published test-only event: future publishers will not pick it.
    const blocker = await pool.query(
        `
        WITH fixture_order AS (
            INSERT INTO orders (uid, discount)
            VALUES (13, 0)
            RETURNING oid
        ),
        fixture_detail AS (
            INSERT INTO order_details (oid, pid, qty, unit_price)
            SELECT oid, 1, 1, 0
            FROM fixture_order
            RETURNING oid
        )
        INSERT INTO outbox_events (
            event_id, event_type, oid, payload,
            published_at, next_attempt_at
        )
        SELECT
            $1, 'TEST_FIXTURE', oid, '{}'::jsonb,
            CURRENT_TIMESTAMP, $2::timestamptz
        FROM fixture_detail
        RETURNING oid
        `,
        [collision.operationId, schedule.nextAttemptAt]
    );

    assert.equal(blocker.rowCount, 1);

    await assert.rejects(
        () => commit(collision),
        (error) => {
            assert.equal(error.code, "23505");
            assert.equal(error.constraint, "outbox_events_pkey");
            return true;
        }
    );

    await assertCounts(collision.operationId, 0, 0);

    const afterCollision =
        await orderCreationRepository.findByOperationId(
            collision.operationId
        );

    assert.equal(afterCollision.status, "PROCESSING");
    assert.equal(afterCollision.score, 100);

    const preservedBlocker = await pool.query(
        `
        SELECT oid, event_type
        FROM outbox_events
        WHERE event_id = $1
        `,
        [collision.operationId]
    );

    assert.equal(
        preservedBlocker.rows[0].oid,
        blocker.rows[0].oid
    );

    assert.equal(
        preservedBlocker.rows[0].event_type,
        "TEST_FIXTURE"
    );

    console.log("Late outbox failure rolls back aggregate and task: OK");

    const unready = await fixture(
        null,
        [{ pid: 1, qty: 1, unit_price: "10.00" }]
    );

    await expectDomainError(
        () => orderCreationTransaction.withLockedTask(
            unready.operationId,
            async (task, context) => {
                await context.freezeScore(100);

                return context.commitReservedOrder(unready, schedule);
            }
        ),
        "CONFLICT"
    );

    assert.equal(
        (
            await orderCreationRepository.findByOperationId(
                unready.operationId
            )
        ).score,
        null
    );

    await orderCreationTransaction.withLockedTask(
        unready.operationId,
        async (task, context) => context.freezeScore(100)
    );

    await expectDomainError(
        () => commit({
            ...unready,
            items: [{ pid: 1, qty: 2, unit_price: "10.00" }],
        }),
        "CONFLICT"
    );

    await expectDomainError(
        () => commit({ ...unready, uid: 14 }),
        "FORBIDDEN"
    );

    await expectDomainError(
        () => commit({ ...unready, status: "CONFIRMED" }),
        "INTERNAL"
    );

    await assertCounts(unready.operationId, 0, 0);

    console.log("Committed score and reservation binding required: OK");

    const callbackFixture = await fixture(500);
    const failure = new Error("Simulated callback failure");
    let temporaryOrder;

    await assert.rejects(
        () => orderCreationTransaction.withLockedTask(
            callbackFixture.operationId,
            async (task, context) => {
                temporaryOrder = await context.commitReservedOrder(
                    callbackFixture,
                    schedule
                );

                throw failure;
            }
        ),
        (error) => error === failure
    );

    assert.ok(temporaryOrder);
    await assertCounts(callbackFixture.operationId, 0, 0);

    const afterCallback =
        await orderCreationRepository.findByOperationId(
            callbackFixture.operationId
        );

    assert.equal(afterCallback.status, "PROCESSING");
    assert.equal(afterCallback.score, 500);

    console.log("Callback failure rolls back aggregate and task: OK");
}

async function cleanup() {
    const client = await pool.connect();
    let releaseError;

    try {
        await client.query("BEGIN");

        const selected = await client.query(
            `
            SELECT DISTINCT o.oid
            FROM orders o
            LEFT JOIN outbox_events e ON e.oid = o.oid
            WHERE o.operation_id = ANY($1::uuid[])
               OR (
                   e.event_id = ANY($1::uuid[])
                   AND e.event_type = 'TEST_FIXTURE'
               )
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
             WHERE operation_id = ANY($1::uuid[])
                OR oid = ANY($2::int[])) AS orders_count,
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
        "Order creation commit check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        await cleanup();

        console.log("Scoped order commit fixtures removed: OK");
    } catch (error) {
        console.error(
            "Order commit fixture cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture operation IDs:", operationIds);
        console.error("Fixture order IDs:", cleanupOrderIds);
        process.exitCode = 1;
    } finally {
        await pool.end();
    }
}