import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    orderCreationRepository,
} from "../src/infrastructure/repositories/order-creation.repository.js";
import {
    orderCreationTransaction,
} from "../src/infrastructure/database/order-creation.transaction.js";
import {
    customerClient,
} from "../src/infrastructure/grpc/customer.client.js";
import {
    productClient,
} from "../src/infrastructure/grpc/product.client.js";
import {
    createOrderCreationProcessor,
} from "../src/application/order-creation.processor.js";

const operationIds = [];
const future = new Date(Date.now() + 86_400_000);

const requestedItems = [
    { pid: 1, qty: 2 },
    { pid: 2, qty: 1 },
];

async function fixture(score = 100) {
    const operationId = randomUUID();
    operationIds.push(operationId);

    await orderCreationRepository.createTask(
        {
            operationId,
            uid: 13,
            actorRole: "CUSTOMER",
            items: requestedItems,
        },
        { nextAttemptAt: future },
    );

    if (score !== null) {
        await orderCreationTransaction.withLockedTask(
            operationId,
            (_, context) => context.freezeScore(score),
        );
    }

    return operationId;
}

function taskOf(id) {
    return orderCreationRepository.findByOperationId(id);
}

function makeProducts() {
    const reservations = new Map();
    const calls = [];

    return {
        reservations,
        calls,

        client: {
            async reserveStock(operationId, items, actor) {
                calls.push(operationId);

                assert.deepEqual(items, requestedItems);
                assert.deepEqual(actor, {
                    uid: 13,
                    role: "CUSTOMER",
                });

                if (!reservations.has(operationId)) {
                    reservations.set(operationId, {
                        operationId,
                        uid: actor.uid,
                        status: "RESERVED",
                        items: [
                            {
                                pid: 1,
                                qty: 2,
                                unit_price: "25.50",
                            },
                            {
                                pid: 2,
                                qty: 1,
                                unit_price: "10.00",
                            },
                        ],
                    });
                }

                return structuredClone(
                    reservations.get(operationId),
                );
            },
        },
    };
}

function processorFor(
    products,
    transactions = orderCreationTransaction,
) {
    return createOrderCreationProcessor({
        transactions,
        products,
        customers: {
            async getCustomer() {
                assert.fail(
                    "Reserve phase must reuse the stored score",
                );
            },
        },
    });
}

function reserve(processor, id) {
    return processor.reserveAndStoreOrder(
        id,
        { nextAttemptAt: future },
    );
}

async function snapshot(id) {
    const orders = await pool.query(
        `
        SELECT *
        FROM orders
        WHERE operation_id = $1::uuid
        ORDER BY oid
        `,
        [id],
    );

    const details = await pool.query(
        `
        SELECT d.*
        FROM order_details d
        JOIN orders o ON o.oid = d.oid
        WHERE o.operation_id = $1::uuid
        ORDER BY d.pid
        `,
        [id],
    );

    const events = await pool.query(
        `
        SELECT e.*
        FROM outbox_events e
        JOIN orders o ON o.oid = e.oid
        WHERE o.operation_id = $1::uuid
        ORDER BY e.event_id
        `,
        [id],
    );

    return {
        orders: orders.rows,
        details: details.rows,
        events: events.rows,
    };
}

async function main() {
    const stock = makeProducts();
    const processor = processorFor(stock.client);

    const firstId = await fixture();
    const first = await reserve(processor, firstId);

    assert.equal(first.outcome, "ORDER_STORED");
    assert.equal(first.status, "CONFIRM_PENDING");
    assert.equal(first.order.discount, "3.05");
    assert.deepEqual(
        first.order.items,
        stock.reservations.get(firstId).items,
    );

    const saved = await snapshot(firstId);

    assert.equal(saved.orders.length, 1);
    assert.equal(saved.details.length, 2);
    assert.equal(saved.events.length, 1);
    assert.equal(saved.orders[0].discount, "3.05");
    assert.equal(saved.events[0].event_id, firstId);
    assert.equal(saved.events[0].event_type, "OrderCreated");
    assert.equal(saved.events[0].payload.discount, "3.05");
    assert.equal(
        (await taskOf(firstId)).status,
        "CONFIRM_PENDING",
    );

    console.log(
        "Reserved stock stores order, details, outbox and task: OK",
    );

    const replay = await reserve(processor, firstId);

    assert.equal(replay.outcome, "NOT_PROCESSING");
    assert.deepEqual(await snapshot(firstId), saved);
    assert.equal(
        stock.calls.filter((id) => id === firstId).length,
        1,
    );

    const concurrentId = await fixture();

    const concurrent = await Promise.all([
        reserve(processor, concurrentId),
        reserve(processor, concurrentId),
    ]);

    assert.deepEqual(
        concurrent.map((result) => result.outcome).sort(),
        ["NOT_PROCESSING", "ORDER_STORED"],
    );

    assert.equal(
        stock.calls.filter((id) => id === concurrentId).length,
        1,
    );

    assert.equal(
        (await snapshot(concurrentId)).orders.length,
        1,
    );

    console.log(
        "Reserve processor replay and concurrency: OK",
    );

    const missingScoreId = await fixture(null);

    assert.equal(
        (await reserve(processor, missingScoreId)).outcome,
        "SCORE_REQUIRED",
    );

    assert.equal(
        stock.calls.includes(missingScoreId),
        false,
    );

    const releasingId = await fixture();

    await orderCreationTransaction.withLockedTask(
        releasingId,
        (_, context) => context.requestRelease("NOT_FOUND"),
    );

    assert.equal(
        (await reserve(processor, releasingId)).outcome,
        "NOT_PROCESSING",
    );

    assert.equal(
        stock.calls.includes(releasingId),
        false,
    );

    console.log(
        "Missing score and release decisions block reservation: OK",
    );

    for (const code of ["NOT_FOUND", "CONFLICT"]) {
        const id = await fixture();

        const failing = processorFor({
            async reserveStock() {
                throw new DomainError(
                    code,
                    "Synthetic stock failure",
                );
            },
        });

        assert.equal(
            (await reserve(failing, id)).outcome,
            "RELEASE_PENDING",
        );

        const task = await taskOf(id);

        assert.equal(task.status, "RELEASE_PENDING");
        assert.equal(task.failure_code, code);
        assert.equal((await snapshot(id)).orders.length, 0);
    }

    const retryScenarios = [
        {
            code: "SERVICE_UNAVAILABLE",
            async call() {
                throw new DomainError(
                    "SERVICE_UNAVAILABLE",
                    "Synthetic timeout",
                );
            },
        },
        {
            code: "INTERNAL",
            async call(id) {
                return {
                    operationId: id,
                    uid: 14,
                    status: "RESERVED",
                    items: [],
                };
            },
        },
        {
            code: "INTERNAL",
            async call(id) {
                const value = await stock.client.reserveStock(
                    id,
                    requestedItems,
                    { uid: 13, role: "CUSTOMER" },
                );

                value.status = "CONFIRMED";
                return value;
            },
        },
    ];

    for (const scenario of retryScenarios) {
        const id = await fixture();

        const failing = processorFor({
            reserveStock: scenario.call,
        });

        assert.equal(
            (await reserve(failing, id)).outcome,
            "RETRY_SCHEDULED",
        );

        const task = await taskOf(id);

        assert.equal(task.status, "PROCESSING");
        assert.equal(task.failure_code, null);
        assert.equal(task.last_error_code, scenario.code);
        assert.equal(task.attempts, 1);
        assert.equal((await snapshot(id)).orders.length, 0);
    }

    console.log(
        "Permanent and transient stock failures routed safely: OK",
    );

    // Product đã giữ hàng nhưng caller nhận timeout.
    const timeoutId = await fixture();

    const timeoutProcessor = processorFor({
        async reserveStock(...args) {
            await stock.client.reserveStock(...args);

            throw new DomainError(
                "SERVICE_UNAVAILABLE",
                "Synthetic lost reserve response",
            );
        },
    });

    assert.equal(
        (await reserve(timeoutProcessor, timeoutId)).outcome,
        "RETRY_SCHEDULED",
    );

    assert.ok(stock.reservations.has(timeoutId));
    assert.equal((await snapshot(timeoutId)).orders.length, 0);

    assert.equal(
        (await reserve(processor, timeoutId)).outcome,
        "ORDER_STORED",
    );

    assert.equal((await snapshot(timeoutId)).orders.length, 1);

    // Callback lỗi sau các lệnh ghi SQL: tất cả phải rollback.
    const rollbackId = await fixture();
    const originalTask = await taskOf(rollbackId);
    const rollbackMarker = new Error("TEST_SQL_ROLLBACK");

    const rollbackProcessor = processorFor(
        stock.client,
        {
            withLockedTask(id, operation) {
                return orderCreationTransaction.withLockedTask(
                    id,
                    async (task, context) => {
                        await operation(task, context);
                        throw rollbackMarker;
                    },
                );
            },
        },
    );

    await assert.rejects(
        () => reserve(rollbackProcessor, rollbackId),
        (error) => error === rollbackMarker,
    );

    assert.deepEqual(
        await taskOf(rollbackId),
        originalTask,
    );

    assert.deepEqual(
        await snapshot(rollbackId),
        { orders: [], details: [], events: [] },
    );

    assert.ok(stock.reservations.has(rollbackId));

    assert.equal(
        (await reserve(processor, rollbackId)).outcome,
        "ORDER_STORED",
    );

    // SQL đã commit nhưng caller mất kết quả.
    const lostResultId = await fixture();
    const lostResultMarker = new Error("TEST_LOST_RESULT");

    const lostResultProcessor = processorFor(
        stock.client,
        {
            async withLockedTask(id, operation) {
                await orderCreationTransaction.withLockedTask(
                    id,
                    operation,
                );

                throw lostResultMarker;
            },
        },
    );

    await assert.rejects(
        () => reserve(lostResultProcessor, lostResultId),
        (error) => error === lostResultMarker,
    );

    const committed = await snapshot(lostResultId);

    assert.equal(committed.orders.length, 1);
    assert.equal(
        (await taskOf(lostResultId)).status,
        "CONFIRM_PENDING",
    );

    assert.equal(
        (await reserve(processor, lostResultId)).outcome,
        "NOT_PROCESSING",
    );

    assert.deepEqual(
        await snapshot(lostResultId),
        committed,
    );

    assert.equal(
        stock.calls.filter((id) => id === lostResultId).length,
        1,
    );

    console.log(
        "SQL rollback, reserve timeout and lost result recovery: OK",
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

        const orderIds = selected.rows.map((row) => row.oid);

        await client.query(
            `DELETE FROM outbox_events
             WHERE oid = ANY($1::integer[])`,
            [orderIds],
        );

        await client.query(
            `DELETE FROM order_details
             WHERE oid = ANY($1::integer[])`,
            [orderIds],
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

        const remaining = await client.query(
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
            [operationIds, orderIds],
        );

        assert.equal(Number(remaining.rows[0].count), 0);

        await client.query("COMMIT");

        console.log(
            "Scoped reserve processor fixtures removed: OK",
        );
    } catch (error) {
        await client.query("ROLLBACK");
        throw error;
    } finally {
        client.release();
    }
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Creation reserve processor check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    try {
        await cleanup();
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Creation reserve processor cleanup failed:",
            error.code ?? error.name,
            error.message,
        );
    } finally {
        customerClient.close();
        productClient.close();
        await pool.end();
    }
}