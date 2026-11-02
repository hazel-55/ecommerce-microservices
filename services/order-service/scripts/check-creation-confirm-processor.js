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

const items = [
    { pid: 1, qty: 2, unit_price: "25.50" },
    { pid: 2, qty: 1, unit_price: "10.00" },
];

function acknowledgment(id, status = "CONFIRMED") {
    return {
        operationId: id,
        uid: 13,
        status,
        items: structuredClone(items),
    };
}

function locked(id, operation) {
    return orderCreationTransaction.withLockedTask(
        id,
        operation,
    );
}

function taskOf(id) {
    return orderCreationRepository.findByOperationId(id);
}

async function fixture({ withOrder = true } = {}) {
    const id = randomUUID();
    operationIds.push(id);

    await orderCreationRepository.createTask(
        {
            operationId: id,
            uid: 13,
            actorRole: "CUSTOMER",
            items,
        },
        { nextAttemptAt: future },
    );

    await locked(
        id,
        (_, context) => context.freezeScore(100),
    );

    if (withOrder) {
        await locked(
            id,
            (_, context) => context.commitReservedOrder(
                acknowledgment(id, "RESERVED"),
                { nextAttemptAt: future },
            ),
        );
    }

    return id;
}

function processorFor(
    confirmStock,
    transactions = orderCreationTransaction,
) {
    return createOrderCreationProcessor({
        transactions,
        products: { confirmStock },
        customers: {
            async getCustomer() {
                assert.fail("Confirm phase must not call Customer");
            },
        },
    });
}

async function aggregate(id) {
    const orders = await pool.query(
        `SELECT * FROM orders
         WHERE operation_id = $1::uuid ORDER BY oid`,
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
    const calls = [];
    const confirmed = new Set();

    async function confirmStock(id, actor) {
        assert.deepEqual(actor, {
            uid: 13,
            role: "CUSTOMER",
        });

        calls.push(id);
        confirmed.add(id);

        return acknowledgment(id);
    }

    const processor = processorFor(confirmStock);

    const firstId = await fixture();
    const originalAggregate = await aggregate(firstId);

    const first = await processor.confirmStoredOrder(firstId);

    assert.equal(first.outcome, "ORDER_COMPLETED");
    assert.equal(first.status, "COMPLETED");
    assert.equal(first.order.discount, "3.05");
    assert.equal((await taskOf(firstId)).status, "COMPLETED");

    assert.deepEqual(
        await aggregate(firstId),
        originalAggregate,
    );

    const replay = await processor.confirmStoredOrder(firstId);

    assert.equal(replay.outcome, "ALREADY_COMPLETED");
    assert.equal(
        calls.filter((id) => id === firstId).length,
        1,
    );

    const concurrentId = await fixture();

    const concurrent = await Promise.all([
        processor.confirmStoredOrder(concurrentId),
        processor.confirmStoredOrder(concurrentId),
    ]);

    assert.deepEqual(
        concurrent.map((value) => value.outcome).sort(),
        ["ALREADY_COMPLETED", "ORDER_COMPLETED"],
    );

    assert.equal(
        calls.filter((id) => id === concurrentId).length,
        1,
    );

    console.log(
        "Stored order confirmation, replay and concurrency: OK",
    );

    const processingId = await fixture({ withOrder: false });

    assert.equal(
        (
            await processor.confirmStoredOrder(processingId)
        ).outcome,
        "NOT_CONFIRM_PENDING",
    );

    const releasingId = await fixture({ withOrder: false });

    await locked(
        releasingId,
        (_, context) => context.requestRelease("NOT_FOUND"),
    );

    assert.equal(
        (
            await processor.confirmStoredOrder(releasingId)
        ).outcome,
        "NOT_CONFIRM_PENDING",
    );

    assert.equal(calls.includes(processingId), false);
    assert.equal(calls.includes(releasingId), false);

    console.log(
        "Uncommitted order and release states block confirmation: OK",
    );

    for (const code of [
        "SERVICE_UNAVAILABLE",
        "NOT_FOUND",
        "CONFLICT",
    ]) {
        const id = await fixture();
        const before = await aggregate(id);

        const failing = processorFor(async () => {
            throw new DomainError(
                code,
                "Synthetic confirmation failure",
            );
        });

        const result = await failing.confirmStoredOrder(id);
        const task = await taskOf(id);

        assert.equal(result.outcome, "RETRY_SCHEDULED");
        assert.equal(task.status, "CONFIRM_PENDING");
        assert.equal(task.failure_code, null);
        assert.equal(task.attempts, 1);

        assert.equal(
            task.last_error_code,
            code === "SERVICE_UNAVAILABLE"
                ? "SERVICE_UNAVAILABLE"
                : "INTERNAL",
        );

        assert.deepEqual(await aggregate(id), before);
    }

    console.log(
        "Confirmation failures retry without releasing stored orders: OK",
    );

    for (const kind of ["wrong-uid", "not-confirmed"]) {
        const id = await fixture();

        const invalid = processorFor(async () => {
            const response = acknowledgment(id);

            if (kind === "wrong-uid") {
                response.uid = 14;
            } else {
                response.status = "RESERVED";
            }

            return response;
        });

        assert.equal(
            (await invalid.confirmStoredOrder(id)).outcome,
            "RETRY_SCHEDULED",
        );

        const task = await taskOf(id);

        assert.equal(task.status, "CONFIRM_PENDING");
        assert.equal(task.last_error_code, "INTERNAL");
        assert.equal(task.failure_code, null);
    }

    // Giá có định dạng hợp lệ nhưng khác snapshot đã lưu.
    const wrongPriceId = await fixture();
    const beforeWrongPrice = await taskOf(wrongPriceId);
    const aggregateBeforeWrongPrice = await aggregate(
        wrongPriceId,
    );

    const wrongPrice = processorFor(async () => {
        const response = acknowledgment(wrongPriceId);
        response.items[0].unit_price = "99.00";
        return response;
    });

    await assert.rejects(
        () => wrongPrice.confirmStoredOrder(wrongPriceId),
        (error) =>
            error instanceof DomainError &&
            error.code === "CONFLICT",
    );

    assert.deepEqual(
        await taskOf(wrongPriceId),
        beforeWrongPrice,
    );

    assert.deepEqual(
        await aggregate(wrongPriceId),
        aggregateBeforeWrongPrice,
    );

    console.log(
        "Confirmation identity, status and price snapshots protected: OK",
    );

    const rollbackId = await fixture();
    const beforeRollback = await taskOf(rollbackId);
    const rollbackMarker = new Error("TEST_SQL_ROLLBACK");

    const rollbackProcessor = processorFor(
        confirmStock,
        {
            withLockedTask(id, operation) {
                return locked(
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
        () => rollbackProcessor.confirmStoredOrder(rollbackId),
        (error) => error === rollbackMarker,
    );

    assert.ok(confirmed.has(rollbackId));
    assert.deepEqual(
        await taskOf(rollbackId),
        beforeRollback,
    );

    assert.equal(
        (
            await processor.confirmStoredOrder(rollbackId)
        ).outcome,
        "ORDER_COMPLETED",
    );

    const lostResultId = await fixture();
    const lostResultMarker = new Error("TEST_LOST_RESULT");

    const lostResultProcessor = processorFor(
        confirmStock,
        {
            async withLockedTask(id, operation) {
                await locked(id, operation);
                throw lostResultMarker;
            },
        },
    );

    await assert.rejects(
        () => lostResultProcessor.confirmStoredOrder(lostResultId),
        (error) => error === lostResultMarker,
    );

    assert.equal(
        (await taskOf(lostResultId)).status,
        "COMPLETED",
    );

    assert.equal(
        (
            await processor.confirmStoredOrder(lostResultId)
        ).outcome,
        "ALREADY_COMPLETED",
    );

    assert.equal(
        calls.filter((id) => id === lostResultId).length,
        1,
    );

    console.log(
        "Confirmed stock survives SQL rollback and lost result: OK",
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
            "Scoped confirm processor fixtures removed: OK",
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
        "Creation confirm processor check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    try {
        await cleanup();
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Creation confirm processor cleanup failed:",
            error.code ?? error.name,
            error.message,
        );
    } finally {
        customerClient.close();
        productClient.close();
        await pool.end();
    }
}