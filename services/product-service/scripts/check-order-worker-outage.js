import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import {
    DomainError,
} from "../../order-service/src/domain/errors.js";
import {
    validateCreationOperationId,
} from "../../order-service/src/domain/order-creation.rules.js";
import {
    productClient,
} from "../../order-service/src/infrastructure/grpc/product.client.js";
import {
    pool,
} from "../../order-service/src/infrastructure/database/pool.js";
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

    validateCreationOperationId(fixture.operationId);

    assert.equal(fixture.uid, 13);
    assert.equal(fixture.role, "CUSTOMER");

    assert.ok(Number.isInteger(fixture.pid));
    assert.ok(fixture.pid > 0);
    assert.ok(fixture.pid <= 2147483647);

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

    return fixture;
}

async function assertProductUnavailable(fixture) {
    await assert.rejects(
        () => productClient.getStockReservation(
            fixture.operationId,
            {
                uid: fixture.uid,
                role: fixture.role,
            },
        ),
        (error) =>
            error instanceof DomainError &&
            error.code === "SERVICE_UNAVAILABLE",
    );
}

async function arm(fixture) {
    // Chỉ kích hoạt task sau khi Product thực sự không truy cập được.
    await assertProductUnavailable(fixture);

    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        const selected = await client.query(
            `
            SELECT *
            FROM order_creation_tasks
            WHERE operation_id = $1::uuid
            FOR UPDATE
            `,
            [fixture.operationId],
        );

        assert.equal(selected.rowCount, 1);

        const task = selected.rows[0];

        assert.equal(task.uid, fixture.uid);
        assert.equal(task.actor_role, fixture.role);
        assert.equal(task.status, "PROCESSING");
        assert.equal(task.score, null);
        assert.equal(task.attempts, 0);

        assert.deepEqual(task.items, [{
            pid: fixture.pid,
            qty: fixture.qty,
        }]);

        assert.equal(
            task.next_attempt_at.getTime(),
            new Date("2100-01-01T00:00:00.000Z").getTime(),
        );

        const updated = await client.query(
            `
            UPDATE order_creation_tasks
            SET
                next_attempt_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE operation_id = $1::uuid
            RETURNING operation_id
            `,
            [fixture.operationId],
        );

        assert.equal(updated.rowCount, 1);

        await client.query("COMMIT");

        console.log(
            "Order fixture armed after Product outage: OK",
        );
    } catch (error) {
        await client.query("ROLLBACK");
        throw error;
    } finally {
        client.release();
    }
}

async function waitForRetry(fixture) {
    const deadline = Date.now() + 45_000;
    let lastProgressAt = 0;

    while (Date.now() < deadline) {
        const selected = await pool.query(
            `
            SELECT *
            FROM order_creation_tasks
            WHERE operation_id = $1::uuid
            `,
            [fixture.operationId],
        );

        assert.equal(selected.rowCount, 1);

        const task = selected.rows[0];

        if (
            task.status === "PROCESSING" &&
            task.score !== null &&
            task.attempts >= 1 &&
            task.last_error_code === "SERVICE_UNAVAILABLE"
        ) {
            return task;
        }

        if (task.status !== "PROCESSING") {
            throw new Error(
                `Unexpected outage task state: ${task.status}`,
            );
        }

        if (Date.now() - lastProgressAt >= 10_000) {
            lastProgressAt = Date.now();

            console.log(
                "Waiting for outage retry:",
                {
                    status: task.status,
                    scoreFrozen: task.score !== null,
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
        "Task did not reach the expected Product outage retry state",
    );
}

async function checkPending(fixture) {
    const task = await waitForRetry(fixture);

    assert.equal(task.uid, fixture.uid);
    assert.equal(task.actor_role, fixture.role);
    assert.equal(task.score, fixture.expectedScore);

    assert.deepEqual(task.items, [{
        pid: fixture.pid,
        qty: fixture.qty,
    }]);

    console.log(
        "Frozen customer score persisted during outage: OK",
    );

    assert.equal(task.status, "PROCESSING");
    assert.equal(task.failure_code, null);
    assert.ok(task.attempts >= 1);

    assert.equal(
        task.last_error_code,
        "SERVICE_UNAVAILABLE",
    );

    assert.ok(
        task.next_attempt_at.getTime() >
        task.updated_at.getTime(),
    );

    await assertProductUnavailable(fixture);

    console.log(
        "Product outage schedules durable retry: OK",
    );

    const orders = await pool.query(
        `
        SELECT COUNT(*) AS count
        FROM orders
        WHERE operation_id = $1::uuid
        `,
        [fixture.operationId],
    );

    assert.equal(Number(orders.rows[0].count), 0);

    const events = await pool.query(
        `
        SELECT COUNT(*) AS count
        FROM outbox_events
        WHERE event_id = $1::uuid
        `,
        [fixture.operationId],
    );

    assert.equal(Number(events.rows[0].count), 0);

    await connectDatabase();

    const product = await ProductModel.findOne({
        pid: fixture.pid,
        pname: fixture.productName,
    }).lean();

    assert.ok(product);
    assert.equal(
        product.quantity,
        fixture.initialQuantity,
    );
    assert.equal(product.reservedQuantity, 0);

    assert.equal(
        await StockReservationModel.countDocuments({
            _id: fixture.operationId,
        }),
        0,
    );

    console.log(
        "No order or stock mutation during outage: OK",
    );

    await writeFile(
        manifestPath,
        `${JSON.stringify(
            {
                ...fixture,
                outageObserved: true,
                outageAttempts: task.attempts,
                outageScore: task.score,
                outageObservedAt: new Date().toISOString(),
            },
            null,
            2,
        )}\n`,
        "utf8",
    );

    console.log(
        "Recovery fixture preserved for service restart: OK",
    );

    console.log(
        "Observed retry attempts:",
        task.attempts,
    );
}

try {
    const mode = process.argv[2];

    if (!["arm", "pending"].includes(mode)) {
        throw new Error(
            "Use: node check-order-worker-outage.js arm|pending",
        );
    }

    const fixture = await loadFixture();

    if (mode === "arm") {
        await arm(fixture);
    } else {
        await checkPending(fixture);
    }
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Order worker outage check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    productClient.close();

    try {
        await pool.end();
    } finally {
        await disconnectDatabase();
    }
}