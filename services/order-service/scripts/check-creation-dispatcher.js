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
    createOrderCreationDispatcher,
} from "../src/application/order-creation.dispatcher.js";

const operationIds = [];
const past = new Date("2000-01-01T00:00:00.000Z");
const future = new Date(Date.now() + 86_400_000);

const calls = {
    customer: 0,
    reserve: [],
    confirm: [],
    release: [],
};

const reservations = new Map();

async function fixture(nextAttemptAt = past) {
    const id = randomUUID();
    operationIds.push(id);

    await orderCreationRepository.createTask(
        {
            operationId: id,
            uid: 13,
            actorRole: "CUSTOMER",
            items: [
                { pid: 1, qty: 2 },
                { pid: 2, qty: 1 },
            ],
        },
        { nextAttemptAt },
    );

    return id;
}

function taskOf(id) {
    return orderCreationRepository.findByOperationId(id);
}

async function ordersOf(id) {
    const result = await pool.query(
        `
        SELECT *
        FROM orders
        WHERE operation_id = $1::uuid
        ORDER BY oid
        `,
        [id],
    );

    return result.rows;
}

async function makeDue(id) {
    await pool.query(
        `
        UPDATE order_creation_tasks
        SET next_attempt_at = $2::timestamptz
        WHERE operation_id = $1::uuid
        `,
        [id, past],
    );
}

const customers = {
    async getCustomer(uid, actor) {
        calls.customer++;

        assert.equal(uid, 13);
        assert.deepEqual(actor, {
            uid: 13,
            role: "CUSTOMER",
        });

        return { uid: 13, score: 100 };
    },
};

const products = {
    async reserveStock(id, items, actor) {
        calls.reserve.push(id);

        assert.deepEqual(actor, {
            uid: 13,
            role: "CUSTOMER",
        });

        // Đọc bằng kết nối khác để xác nhận score đã commit.
        const committedTask = await taskOf(id);

        assert.equal(committedTask.status, "PROCESSING");
        assert.equal(committedTask.score, 100);

        assert.deepEqual(items, [
            { pid: 1, qty: 2 },
            { pid: 2, qty: 1 },
        ]);

        if (!reservations.has(id)) {
            reservations.set(id, {
                operationId: id,
                uid: 13,
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

        return structuredClone(reservations.get(id));
    },

    async confirmStock(id, actor) {
        calls.confirm.push(id);

        assert.equal(actor.uid, 13);

        // Order phải nhìn thấy từ kết nối khác trước Confirm.
        const orders = await ordersOf(id);
        const committedTask = await taskOf(id);

        assert.equal(orders.length, 1);
        assert.equal(orders[0].discount, "3.05");
        assert.equal(
            committedTask.status,
            "CONFIRM_PENDING",
        );

        const reservation = reservations.get(id);
        assert.ok(reservation);

        reservation.status = "CONFIRMED";

        return structuredClone(reservation);
    },

    async releaseStock(id, actor) {
        calls.release.push(id);

        assert.equal(actor.uid, 13);

        const orders = await ordersOf(id);
        const committedTask = await taskOf(id);

        assert.equal(orders.length, 0);
        assert.equal(
            committedTask.status,
            "RELEASE_PENDING",
        );
        assert.equal(
            committedTask.failure_code,
            "NOT_FOUND",
        );

        return {
            operationId: id,
            uid: 13,
            status: "RELEASED",
            items: [],
        };
    },
};

function dispatcherFor({
    customerDependency = customers,
    transactions = orderCreationTransaction,
} = {}) {
    return createOrderCreationDispatcher({
        transactions,
        customers: customerDependency,
        products,
    });
}

async function main() {
    const dispatcher = dispatcherFor();
    const successId = await fixture();

    assert.equal(
        (
            await dispatcher.processDueTask(successId)
        ).outcome,
        "SCORE_FROZEN",
    );

    assert.equal((await taskOf(successId)).score, 100);
    assert.equal((await ordersOf(successId)).length, 0);

    assert.equal(
        (
            await dispatcher.processDueTask(successId)
        ).outcome,
        "ORDER_STORED",
    );

    assert.equal(
        (await taskOf(successId)).status,
        "CONFIRM_PENDING",
    );

    assert.equal(
        (
            await dispatcher.processDueTask(successId)
        ).outcome,
        "ORDER_COMPLETED",
    );

    assert.equal(
        (await taskOf(successId)).status,
        "COMPLETED",
    );

    const event = await pool.query(
        `
        SELECT e.*
        FROM outbox_events e
        JOIN orders o ON o.oid = e.oid
        WHERE o.operation_id = $1::uuid
        `,
        [successId],
    );

    assert.equal(event.rowCount, 1);
    assert.equal(event.rows[0].payload.discount, "3.05");

    console.log(
        "Successful order phases use separate committed transactions: OK",
    );

    const failureId = await fixture();

    const failing = dispatcherFor({
        customerDependency: {
            async getCustomer() {
                throw new DomainError(
                    "NOT_FOUND",
                    "Synthetic missing customer",
                );
            },
        },
    });

    assert.equal(
        (
            await failing.processDueTask(failureId)
        ).outcome,
        "RELEASE_PENDING",
    );

    assert.equal(
        calls.release.includes(failureId),
        false,
    );

    assert.equal(
        (
            await failing.processDueTask(failureId)
        ).outcome,
        "ORDER_FAILED",
    );

    assert.equal(
        (await taskOf(failureId)).failure_code,
        "NOT_FOUND",
    );

    assert.equal(
        calls.reserve.includes(failureId),
        false,
    );

    console.log(
        "Permanent failure dispatches durable stock release: OK",
    );

    const futureId = await fixture(future);
    const beforeFuture = await taskOf(futureId);
    const beforeCalls = structuredClone(calls);

    assert.equal(
        (
            await dispatcher.processDueTask(futureId)
        ).outcome,
        "NOT_DUE",
    );

    assert.equal(
        (
            await dispatcher.processDueTask(successId)
        ).outcome,
        "ALREADY_COMPLETED",
    );

    assert.equal(
        (
            await dispatcher.processDueTask(failureId)
        ).outcome,
        "ALREADY_FAILED",
    );

    assert.deepEqual(calls, beforeCalls);
    assert.deepEqual(await taskOf(futureId), beforeFuture);

    let closedContext;

    await orderCreationTransaction.withLockedTask(
        futureId,
        async (_, context) => {
            closedContext = context;
            assert.equal(await context.isDue(), false);
        },
    );

    await assert.rejects(
        () => closedContext.isDue(),
        (error) =>
            error instanceof DomainError &&
            error.code === "INTERNAL",
    );

    console.log(
        "Future and closed tasks do not call services: OK",
    );

    const retryId = await fixture();
    let retryCalls = 0;

    const retrying = dispatcherFor({
        customerDependency: {
            async getCustomer(uid) {
                retryCalls++;

                if (retryCalls === 1) {
                    throw new DomainError(
                        "SERVICE_UNAVAILABLE",
                        "Synthetic customer outage",
                    );
                }

                return { uid, score: 100 };
            },
        },
    });

    assert.equal(
        (
            await retrying.processDueTask(retryId)
        ).outcome,
        "RETRY_SCHEDULED",
    );

    assert.equal(
        (await taskOf(retryId)).attempts,
        1,
    );

    // Quét được task, nhưng lịch bị đổi trước khi xử lý.
    await makeDue(retryId);

    const discovered =
        await orderCreationRepository.listDueOperationIds({
            limit: 100,
        });

    assert.ok(discovered.includes(retryId));

    await pool.query(
        `
        UPDATE order_creation_tasks
        SET next_attempt_at = $2::timestamptz
        WHERE operation_id = $1::uuid
        `,
        [retryId, future],
    );

    const beforeSkip = await taskOf(retryId);

    assert.equal(
        (
            await retrying.processDueTask(retryId)
        ).outcome,
        "NOT_DUE",
    );

    assert.equal(retryCalls, 1);
    assert.deepEqual(await taskOf(retryId), beforeSkip);

    await makeDue(retryId);

    assert.equal(
        (
            await retrying.processDueTask(retryId)
        ).outcome,
        "SCORE_FROZEN",
    );

    assert.equal(retryCalls, 2);

    console.log(
        "Retry schedule rechecked after candidate discovery: OK",
    );

    const concurrentId = await fixture();
    const customerCallsBefore = calls.customer;

    const concurrent = await Promise.all([
        dispatcher.processDueTask(concurrentId),
        dispatcher.processDueTask(concurrentId),
        dispatcher.processDueTask(concurrentId),
    ]);

    assert.deepEqual(
        concurrent.map((value) => value.outcome).sort(),
        [
            "ORDER_COMPLETED",
            "ORDER_STORED",
            "SCORE_FROZEN",
        ],
    );

    assert.equal(calls.customer - customerCallsBefore, 1);

    assert.equal(
        calls.reserve.filter(
            (id) => id === concurrentId,
        ).length,
        1,
    );

    assert.equal(
        calls.confirm.filter(
            (id) => id === concurrentId,
        ).length,
        1,
    );

    assert.equal(
        (await ordersOf(concurrentId)).length,
        1,
    );

    const rollbackId = await fixture();
    const beforeRollback = await taskOf(rollbackId);
    const marker = new Error("TEST_DISPATCH_ROLLBACK");

    const rollbackDispatcher = dispatcherFor({
        transactions: {
            withLockedTask(id, operation) {
                return orderCreationTransaction.withLockedTask(
                    id,
                    async (task, context) => {
                        await operation(task, context);
                        throw marker;
                    },
                );
            },
        },
    });

    await assert.rejects(
        () => rollbackDispatcher.processDueTask(rollbackId),
        (error) => error === marker,
    );

    assert.deepEqual(
        await taskOf(rollbackId),
        beforeRollback,
    );

    assert.equal(
        (
            await dispatcher.processDueTask(rollbackId)
        ).outcome,
        "SCORE_FROZEN",
    );

    console.log(
        "Concurrent dispatch and transaction rollback remain recoverable: OK",
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
            SELECT oid FROM orders
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
            "Scoped dispatcher fixtures removed: OK",
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
        "Creation dispatcher check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    try {
        await cleanup();
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Creation dispatcher cleanup failed:",
            error.code ?? error.name,
            error.message,
        );
    } finally {
        customerClient.close();
        productClient.close();
        await pool.end();
    }
}