import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
    DomainError,
} from "../../order-service/src/domain/errors.js";
import {
    customerClient,
} from "../../order-service/src/infrastructure/grpc/customer.client.js";
import {
    productClient,
} from "../../order-service/src/infrastructure/grpc/product.client.js";
import {
    pool,
} from "../../order-service/src/infrastructure/database/pool.js";
import {
    orderCreationRepository,
} from "../../order-service/src/infrastructure/repositories/order-creation.repository.js";
import {
    orderRepository,
} from "../../order-service/src/infrastructure/repositories/order.repository.js";
import {
    orderCreationTransaction,
} from "../../order-service/src/infrastructure/database/order-creation.transaction.js";
import {
    createOrderCreationDispatcher,
} from "../../order-service/src/application/order-creation.dispatcher.js";
import {
    calculateOrderPricing,
} from "../../order-service/src/domain/order.pricing.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import {
    ProductModel,
} from "../src/infrastructure/database/models/product.model.js";
import {
    StockReservationModel,
} from "../src/infrastructure/database/models/stock-reservation.model.js";
import {
    productRepository,
} from "../src/infrastructure/repositories/product.repository.js";

const actor = { uid: 13, role: "CUSTOMER" };
const operationId = randomUUID();
const fixtureId = randomUUID();

const productNames = [
    `order_creation_${fixtureId}_A`,
    `order_creation_${fixtureId}_B`,
];

const future = new Date(Date.now() + 86_400_000);

// Giữ Outbox fixture ở tương lai.
// Mọi xử lý SQL và lời gọi gRPC vẫn dùng implementation thật.
const dispatcher = createOrderCreationDispatcher({
    transactions: {
        withLockedTask(id, operation) {
            return orderCreationTransaction.withLockedTask(
                id,
                (task, context) => operation(
                    task,
                    {
                        ...context,

                        commitReservedOrder(
                            reservation,
                            options = {},
                        ) {
                            return context.commitReservedOrder(
                                reservation,
                                {
                                    ...options,
                                    nextAttemptAt: future,
                                },
                            );
                        },
                    },
                ),
            );
        },
    },
});

function taskOf() {
    return orderCreationRepository.findByOperationId(
        operationId,
    );
}

async function makeDue() {
    await pool.query(
        `
        UPDATE order_creation_tasks
        SET next_attempt_at =
            CURRENT_TIMESTAMP - INTERVAL '1 second'
        WHERE operation_id = $1::uuid
        `,
        [operationId],
    );
}

async function assertStock(pid, quantity, reservedQuantity) {
    const product = await ProductModel.findOne({ pid }).lean();

    assert.ok(product);
    assert.equal(product.quantity, quantity);
    assert.equal(product.reservedQuantity, reservedQuantity);
}

async function storedOrder() {
    const result = await pool.query(
        `
        SELECT oid
        FROM orders
        WHERE operation_id = $1::uuid
        `,
        [operationId],
    );

    assert.equal(result.rowCount, 1);

    return orderRepository.findById(
        result.rows[0].oid,
    );
}

async function eventsOf() {
    const result = await pool.query(
        `
        SELECT e.*
        FROM outbox_events e
        JOIN orders o ON o.oid = e.oid
        WHERE o.operation_id = $1::uuid
        ORDER BY e.event_id
        `,
        [operationId],
    );

    return result.rows;
}

async function main() {
    const customer = await customerClient.getCustomer(
        actor.uid,
        actor,
    );

    assert.equal(customer.uid, actor.uid);
    assert.ok(Number.isInteger(customer.score));
    assert.ok(customer.score >= 0);
    assert.ok(customer.score <= 2147483647);

    await connectDatabase();

    const first = await productRepository.create({
        pname: productNames[0],
        price: "25.50",
        quantity: 10,
    });

    const second = await productRepository.create({
        pname: productNames[1],
        price: "10.00",
        quantity: 6,
    });

    const input = {
        operationId,
        uid: actor.uid,
        actorRole: actor.role,
        items: [
            { pid: first.pid, qty: 2 },
            { pid: second.pid, qty: 1 },
        ],
    };

    const expectedItems = [
        {
            pid: first.pid,
            qty: 2,
            unit_price: "25.50",
        },
        {
            pid: second.pid,
            qty: 1,
            unit_price: "10.00",
        },
    ].sort((a, b) => a.pid - b.pid);

    await orderCreationRepository.createTask(
        input,
        { nextAttemptAt: future },
    );

    await makeDue();

    const frozen = await dispatcher.processDueTask(
        operationId,
    );

    assert.equal(frozen.outcome, "SCORE_FROZEN");

    const frozenTask = await taskOf();

    assert.equal(frozenTask.status, "PROCESSING");
    assert.equal(frozenTask.score, customer.score);

    await assert.rejects(
        () => productClient.getStockReservation(
            operationId,
            actor,
        ),
        (error) =>
            error instanceof DomainError &&
            error.code === "NOT_FOUND",
    );

    await assertStock(first.pid, 10, 0);
    await assertStock(second.pid, 6, 0);

    console.log(
        "Live Customer score frozen before stock reservation: OK",
    );

    const persisted = await dispatcher.processDueTask(
        operationId,
    );

    assert.equal(persisted.outcome, "ORDER_STORED");

    const pendingTask = await taskOf();

    assert.equal(
        pendingTask.status,
        "CONFIRM_PENDING",
    );

    assert.equal(pendingTask.score, frozenTask.score);

    const reserved =
        await productClient.getStockReservation(
            operationId,
            actor,
        );

    assert.equal(reserved.status, "RESERVED");
    assert.deepEqual(reserved.items, expectedItems);

    await assertStock(first.pid, 8, 2);
    await assertStock(second.pid, 5, 1);

    const orderBeforeConfirm = await storedOrder();
    const eventsBeforeConfirm = await eventsOf();

    assert.equal(orderBeforeConfirm.uid, actor.uid);
    assert.deepEqual(
        orderBeforeConfirm.items,
        expectedItems,
    );

    assert.equal(eventsBeforeConfirm.length, 1);
    assert.equal(
        eventsBeforeConfirm[0].event_id,
        operationId,
    );

    assert.equal(
        eventsBeforeConfirm[0].event_type,
        "OrderCreated",
    );

    assert.equal(
        eventsBeforeConfirm[0].published_at,
        null,
    );

    assert.ok(
        eventsBeforeConfirm[0].next_attempt_at.getTime() >
        Date.now(),
    );

    console.log(
        "Live Product reservation and atomic Order persistence: OK",
    );

    // Task fixture được hẹn ở tương lai khi lưu Order.
    // Cho phép riêng fixture này chạy bước Confirm.
    await makeDue();

    const completed = await dispatcher.processDueTask(
        operationId,
    );

    assert.equal(completed.outcome, "ORDER_COMPLETED");
    assert.equal(
        (await taskOf()).status,
        "COMPLETED",
    );

    const confirmed =
        await productClient.getStockReservation(
            operationId,
            actor,
        );

    assert.equal(confirmed.status, "CONFIRMED");
    assert.deepEqual(confirmed.items, expectedItems);

    await assertStock(first.pid, 8, 0);
    await assertStock(second.pid, 5, 0);

    console.log(
        "Live stock confirmation completes creation task: OK",
    );

    const expectedPricing = calculateOrderPricing(
        reserved.items,
        frozenTask.score,
    );

    const orderAfterConfirm = await storedOrder();

    assert.equal(
        orderAfterConfirm.discount,
        expectedPricing.discount,
    );

    assert.deepEqual(
        orderAfterConfirm.items,
        expectedPricing.items,
    );

    assert.deepEqual(
        orderAfterConfirm,
        orderBeforeConfirm,
    );

    const eventsAfterConfirm = await eventsOf();

    assert.deepEqual(
        eventsAfterConfirm,
        eventsBeforeConfirm,
    );

    const payload = eventsAfterConfirm[0].payload;

    assert.equal(payload.version, 1);
    assert.equal(payload.oid, orderAfterConfirm.oid);
    assert.equal(payload.uid, actor.uid);
    assert.equal(
        payload.discount,
        expectedPricing.discount,
    );

    assert.deepEqual(payload.items, expectedItems);

    console.log(
        "Stored prices and discount match live snapshots: OK",
    );

    // Cùng operationId và cùng request phải dùng task cũ.
    const replayTask =
        await orderCreationRepository.createTask(input);

    assert.equal(replayTask.status, "COMPLETED");

    for (let index = 0; index < 2; index++) {
        const replay = await dispatcher.processDueTask(
            operationId,
        );

        assert.equal(
            replay.outcome,
            "ALREADY_COMPLETED",
        );
    }

    assert.deepEqual(
        await storedOrder(),
        orderAfterConfirm,
    );

    assert.deepEqual(
        await eventsOf(),
        eventsAfterConfirm,
    );

    assert.deepEqual(
        await productClient.getStockReservation(
            operationId,
            actor,
        ),
        confirmed,
    );

    await assertStock(first.pid, 8, 0);
    await assertStock(second.pid, 5, 0);

    console.log(
        "Completed creation replay does not duplicate order or stock: OK",
    );
}

async function cleanupSql() {
    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        const selected = await client.query(
            `
            SELECT oid
            FROM orders
            WHERE operation_id = $1::uuid
            `,
            [operationId],
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
             WHERE operation_id = $1::uuid`,
            [operationId],
        );

        await client.query(
            `DELETE FROM order_creation_tasks
             WHERE operation_id = $1::uuid`,
            [operationId],
        );

        const remaining = await client.query(
            `
            SELECT
                (SELECT COUNT(*) FROM order_creation_tasks
                 WHERE operation_id = $1::uuid)
                +
                (SELECT COUNT(*) FROM orders
                 WHERE operation_id = $1::uuid)
                +
                (SELECT COUNT(*) FROM order_details
                 WHERE oid = ANY($2::integer[]))
                +
                (SELECT COUNT(*) FROM outbox_events
                 WHERE oid = ANY($2::integer[]))
                AS count
            `,
            [operationId, orderIds],
        );

        assert.equal(Number(remaining.rows[0].count), 0);

        await client.query("COMMIT");
    } catch (error) {
        await client.query("ROLLBACK");
        throw error;
    } finally {
        client.release();
    }
}

async function cleanupMongo() {
    await StockReservationModel.deleteMany({
        _id: operationId,
    });

    await ProductModel.deleteMany({
        pname: { $in: productNames },
    });

    assert.equal(
        await StockReservationModel.countDocuments({
            _id: operationId,
        }),
        0,
    );

    assert.equal(
        await ProductModel.countDocuments({
            pname: { $in: productNames },
        }),
        0,
    );
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Live order creation check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    customerClient.close();
    productClient.close();

    let cleanupSucceeded = true;

    try {
        await cleanupSql();
    } catch (error) {
        cleanupSucceeded = false;
        process.exitCode = 1;

        console.error(
            "Live order SQL cleanup failed:",
            error.code ?? error.name,
        );
    }

    try {
        if (ProductModel.db.readyState === 1) {
            await cleanupMongo();
        }
    } catch (error) {
        cleanupSucceeded = false;
        process.exitCode = 1;

        console.error(
            "Live order Mongo cleanup failed:",
            error.code ?? error.name,
        );
    }

    if (cleanupSucceeded) {
        console.log(
            "Scoped live order creation fixtures removed: OK",
        );
    } else {
        console.error("Fixture operation ID:", operationId);
        console.error("Fixture product names:", productNames);
    }

    try {
        await pool.end();
    } finally {
        await disconnectDatabase();
    }
}