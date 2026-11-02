import assert from "node:assert/strict";
import {
    readFile,
    writeFile,
    unlink,
} from "node:fs/promises";
import {
    validateCreationOperationId,
} from "../../order-service/src/domain/order-creation.rules.js";
import {
    calculateOrderPricing,
} from "../../order-service/src/domain/order.pricing.js";
import {
    productClient,
} from "../../order-service/src/infrastructure/grpc/product.client.js";
import {
    pool,
} from "../../order-service/src/infrastructure/database/pool.js";
import {
    orderRepository,
} from "../../order-service/src/infrastructure/repositories/order.repository.js";
import {
    orderCreationRepository,
} from "../../order-service/src/infrastructure/repositories/order-creation.repository.js";
import {
    orderCreationDispatcher,
} from "../../order-service/src/application/order-creation.dispatcher.js";
import {
    customerClient,
} from "../../order-service/src/infrastructure/grpc/customer.client.js";
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

const manifestPath = new URL(
    "./order-worker-recovery.fixture.json",
    import.meta.url,
);

async function loadFixture() {
    const fixture = JSON.parse(
        await readFile(manifestPath, "utf8"),
    );

    assert.equal(fixture.version, 1);
    assert.equal(fixture.prepared, true);
    assert.equal(fixture.outageObserved, true);

    validateCreationOperationId(fixture.operationId);

    assert.equal(fixture.uid, 13);
    assert.equal(fixture.role, "CUSTOMER");
    assert.ok(Number.isInteger(fixture.pid));
    assert.ok(fixture.pid > 0);

    assert.equal(
        fixture.productName,
        `order_worker_recovery_${fixture.operationId}`,
    );

    assert.equal(fixture.price, "25.50");
    assert.equal(fixture.initialQuantity, 10);
    assert.equal(fixture.qty, 2);

    assert.ok(Number.isInteger(fixture.expectedScore));
    assert.ok(fixture.expectedScore >= 0);
    assert.ok(fixture.expectedScore <= 2147483647);

    assert.equal(
        fixture.outageScore,
        fixture.expectedScore,
    );

    assert.ok(Number.isInteger(fixture.outageAttempts));
    assert.ok(fixture.outageAttempts >= 1);

    return fixture;
}

async function waitForCompletion(fixture) {
    const deadline = Date.now() + 120_000;
    let lastProgressAt = 0;

    while (Date.now() < deadline) {
        const task =
            await orderCreationRepository.findByOperationId(
                fixture.operationId,
            );

        assert.ok(task);

        if (task.status === "COMPLETED") {
            return task;
        }

        if (
            ["FAILED", "RELEASE_PENDING"]
                .includes(task.status)
        ) {
            throw new Error(
                `Unexpected recovery state: ${task.status}`,
            );
        }

        if (Date.now() - lastProgressAt >= 10_000) {
            lastProgressAt = Date.now();

            console.log(
                "Waiting for worker recovery:",
                {
                    status: task.status,
                    attempts: task.attempts,
                    lastErrorCode: task.last_error_code,
                },
            );
        }

        await new Promise((resolve) => {
            setTimeout(resolve, 1000);
        });
    }

    throw new Error(
        "Worker did not complete the recovery task in time",
    );
}

async function readOrder(fixture) {
    const selected = await pool.query(
        `
        SELECT oid
        FROM orders
        WHERE operation_id = $1::uuid
        `,
        [fixture.operationId],
    );

    assert.equal(selected.rowCount, 1);

    return orderRepository.findById(
        selected.rows[0].oid,
    );
}

async function readEvents(fixture) {
    const result = await pool.query(
        `
        SELECT e.*
        FROM outbox_events e
        JOIN orders o ON o.oid = e.oid
        WHERE o.operation_id = $1::uuid
        ORDER BY e.event_id
        `,
        [fixture.operationId],
    );

    return result.rows;
}

async function assertStock(fixture) {
    const product = await ProductModel.findOne({
        pid: fixture.pid,
        pname: fixture.productName,
    }).lean();

    assert.ok(product);

    assert.equal(
        product.quantity,
        fixture.initialQuantity - fixture.qty,
    );

    assert.equal(product.reservedQuantity, 0);
}

async function checkCompleted(fixture) {
    // Chỉ chờ worker thật xử lý, không gọi processor để hoàn tất.
    const task = await waitForCompletion(fixture);

    assert.equal(task.operation_id, fixture.operationId);
    assert.equal(task.uid, fixture.uid);
    assert.equal(task.actor_role, fixture.role);
    assert.ok(task.attempts >= fixture.outageAttempts);
    assert.equal(task.last_error_code, null);
    assert.equal(task.failure_code, null);

    console.log(
        "Worker resumes the original task after Product restart: OK",
    );

    const expectedItems = [{
        pid: fixture.pid,
        qty: fixture.qty,
        unit_price: fixture.price,
    }];

    assert.equal(task.score, fixture.outageScore);

    const order = await readOrder(fixture);
    const pricing = calculateOrderPricing(
        expectedItems,
        fixture.outageScore,
    );

    assert.equal(order.uid, fixture.uid);
    assert.equal(order.discount, pricing.discount);
    assert.deepEqual(order.items, expectedItems);

    console.log(
        "Frozen score and order price snapshots preserved: OK",
    );

    const events = await readEvents(fixture);

    assert.equal(events.length, 1);
    assert.equal(events[0].event_id, fixture.operationId);
    assert.equal(events[0].event_type, "OrderCreated");

    assert.equal(events[0].payload.version, 1);
    assert.equal(events[0].payload.oid, order.oid);
    assert.equal(events[0].payload.uid, fixture.uid);
    assert.equal(
        events[0].payload.discount,
        pricing.discount,
    );

    assert.deepEqual(
        events[0].payload.items,
        expectedItems,
    );

    console.log(
        "Recovered order and outbox created once: OK",
    );

    const actor = {
        uid: fixture.uid,
        role: fixture.role,
    };

    const reservation =
        await productClient.getStockReservation(
            fixture.operationId,
            actor,
        );

    assert.equal(reservation.status, "CONFIRMED");
    assert.deepEqual(reservation.items, expectedItems);

    await connectDatabase();
    await assertStock(fixture);

    assert.equal(
        await StockReservationModel.countDocuments({
            _id: fixture.operationId,
            uid: fixture.uid,
            status: "CONFIRMED",
        }),
        1,
    );

    console.log(
        "Recovered stock confirmed without duplicate deduction: OK",
    );

    await orderCreationRepository.createTask({
        operationId: fixture.operationId,
        uid: fixture.uid,
        actorRole: fixture.role,
        items: [{
            pid: fixture.pid,
            qty: fixture.qty,
        }],
    });

    const replay =
        await orderCreationDispatcher.processDueTask(
            fixture.operationId,
        );

    assert.equal(replay.outcome, "ALREADY_COMPLETED");
    assert.deepEqual(await readOrder(fixture), order);
    assert.deepEqual(await readEvents(fixture), events);

    assert.deepEqual(
        await productClient.getStockReservation(
            fixture.operationId,
            actor,
        ),
        reservation,
    );

    await assertStock(fixture);

    await writeFile(
        manifestPath,
        `${JSON.stringify(
            {
                ...fixture,
                recoveryVerified: true,
                orderId: order.oid,
                recoveredAt: new Date().toISOString(),
            },
            null,
            2,
        )}\n`,
        "utf8",
    );

    console.log(
        "Completed recovery replay remains idempotent: OK",
    );
}

async function cleanupSql(fixture) {
    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        const tasks = await client.query(
            `
            SELECT *
            FROM order_creation_tasks
            WHERE operation_id = $1::uuid
            FOR UPDATE
            `,
            [fixture.operationId],
        );

        if (tasks.rowCount === 1) {
            assert.equal(tasks.rows[0].status, "COMPLETED");
            assert.equal(tasks.rows[0].uid, fixture.uid);
        }

        const orders = await client.query(
            `
            SELECT oid, operation_id, uid
            FROM orders
            WHERE oid = $1::integer
            FOR UPDATE
            `,
            [fixture.orderId],
        );

        if (orders.rowCount === 1) {
            assert.equal(
                orders.rows[0].operation_id,
                fixture.operationId,
            );

            assert.equal(orders.rows[0].uid, fixture.uid);
        }

        const boundOrders = await client.query(
            `
            SELECT oid
            FROM orders
            WHERE operation_id = $1::uuid
            `,
            [fixture.operationId],
        );

        assert.ok(boundOrders.rowCount <= 1);

        if (boundOrders.rowCount === 1) {
            assert.equal(
                boundOrders.rows[0].oid,
                fixture.orderId,
            );
        }

        await client.query(
            `DELETE FROM outbox_events
             WHERE oid = $1::integer`,
            [fixture.orderId],
        );

        await client.query(
            `DELETE FROM order_details
             WHERE oid = $1::integer`,
            [fixture.orderId],
        );

        await client.query(
            `
            DELETE FROM orders
            WHERE oid = $1::integer
              AND operation_id = $2::uuid
            `,
            [fixture.orderId, fixture.operationId],
        );

        await client.query(
            `DELETE FROM order_creation_tasks
             WHERE operation_id = $1::uuid`,
            [fixture.operationId],
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
                 WHERE oid = $2::integer)
                +
                (SELECT COUNT(*) FROM outbox_events
                 WHERE oid = $2::integer)
                AS count
            `,
            [fixture.operationId, fixture.orderId],
        );

        assert.equal(Number(remaining.rows[0].count), 0);

        await client.query("COMMIT");

        console.log(
            "Recovery SQL fixtures removed: OK",
        );
    } catch (error) {
        await client.query("ROLLBACK");
        throw error;
    } finally {
        client.release();
    }
}

async function cleanupMongo(fixture) {
    await connectDatabase();

    const product = await ProductModel.findOne({
        pid: fixture.pid,
        pname: fixture.productName,
    }).lean();

    if (product) {
        assert.equal(
            product.quantity,
            fixture.initialQuantity - fixture.qty,
        );
        assert.equal(product.reservedQuantity, 0);
    }

    const reservation =
        await StockReservationModel.findById(
            fixture.operationId,
        ).lean();

    if (reservation) {
        assert.equal(reservation.uid, fixture.uid);
        assert.equal(reservation.status, "CONFIRMED");
    }

    await StockReservationModel.deleteMany({
        _id: fixture.operationId,
        uid: fixture.uid,
        status: "CONFIRMED",
    });

    await ProductModel.deleteMany({
        pid: fixture.pid,
        pname: fixture.productName,
    });

    assert.equal(
        await StockReservationModel.countDocuments({
            _id: fixture.operationId,
        }),
        0,
    );

    assert.equal(
        await ProductModel.countDocuments({
            pname: fixture.productName,
        }),
        0,
    );

    console.log(
        "Recovery Mongo fixtures removed: OK",
    );
}

async function cleanup(fixture) {
    assert.equal(fixture.recoveryVerified, true);
    assert.ok(Number.isInteger(fixture.orderId));
    assert.ok(fixture.orderId > 0);

    await cleanupSql(fixture);
    await cleanupMongo(fixture);

    await unlink(manifestPath);

    console.log(
        "Recovery fixture manifest removed: OK",
    );
}

try {
    const mode = process.argv[2];

    if (!["completed", "cleanup"].includes(mode)) {
        throw new Error(
            "Use: node check-order-worker-recovery.js completed|cleanup",
        );
    }

    const fixture = await loadFixture();

    if (mode === "completed") {
        await checkCompleted(fixture);
    } else {
        await cleanup(fixture);
    }
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Order worker recovery check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    customerClient.close();
    productClient.close();

    try {
        await pool.end();
    } finally {
        await disconnectDatabase();
    }
}