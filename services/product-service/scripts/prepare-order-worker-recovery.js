import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
    open,
    writeFile,
    unlink,
} from "node:fs/promises";
import {
    customerClient,
} from "../../order-service/src/infrastructure/grpc/customer.client.js";
import {
    pool,
} from "../../order-service/src/infrastructure/database/pool.js";
import {
    orderCreationRepository,
} from "../../order-service/src/infrastructure/repositories/order-creation.repository.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import {
    ProductModel,
} from "../src/infrastructure/database/models/product.model.js";
import {
    productRepository,
} from "../src/infrastructure/repositories/product.repository.js";

const manifestPath = new URL(
    "./order-worker-recovery.fixture.json",
    import.meta.url,
);

const fixture = {
    version: 1,
    operationId: randomUUID(),
    uid: 13,
    role: "CUSTOMER",
    productName: "",
    pid: null,
    price: "25.50",
    initialQuantity: 10,
    qty: 2,
    expectedScore: null,
    prepared: false,
};

fixture.productName =
    `order_worker_recovery_${fixture.operationId}`;

const deferredUntil =
    new Date("2100-01-01T00:00:00.000Z");

let manifestCreated = false;
let prepared = false;

async function saveManifest() {
    await writeFile(
        manifestPath,
        `${JSON.stringify(fixture, null, 2)}\n`,
        "utf8",
    );
}

async function main() {
    // Không ghi đè fixture của lần kiểm tra chưa hoàn tất.
    const handle = await open(manifestPath, "wx");
    manifestCreated = true;

    try {
        await handle.writeFile(
            `${JSON.stringify(fixture, null, 2)}\n`,
            "utf8",
        );
    } finally {
        await handle.close();
    }

    const actor = {
        uid: fixture.uid,
        role: fixture.role,
    };

    const customer = await customerClient.getCustomer(
        fixture.uid,
        actor,
    );

    assert.equal(customer.uid, fixture.uid);
    assert.ok(Number.isInteger(customer.score));
    assert.ok(customer.score >= 0);
    assert.ok(customer.score <= 2147483647);

    fixture.expectedScore = customer.score;
    await saveManifest();

    console.log(
        "Recovery customer profile and score: OK",
    );

    await connectDatabase();

    const product = await productRepository.create({
        pname: fixture.productName,
        price: fixture.price,
        quantity: fixture.initialQuantity,
    });

    fixture.pid = product.pid;
    await saveManifest();

    const storedProduct = await ProductModel.findOne({
        pid: fixture.pid,
        pname: fixture.productName,
    }).lean();

    assert.ok(storedProduct);
    assert.equal(
        storedProduct.quantity,
        fixture.initialQuantity,
    );
    assert.equal(storedProduct.reservedQuantity, 0);

    console.log(
        "Isolated recovery product created: OK",
    );

    await orderCreationRepository.createTask(
        {
            operationId: fixture.operationId,
            uid: fixture.uid,
            actorRole: fixture.role,
            items: [{
                pid: fixture.pid,
                qty: fixture.qty,
            }],
        },
        { nextAttemptAt: deferredUntil },
    );

    const task =
        await orderCreationRepository.findByOperationId(
            fixture.operationId,
        );

    assert.ok(task);
    assert.equal(task.status, "PROCESSING");
    assert.equal(task.score, null);
    assert.equal(task.attempts, 0);

    assert.equal(
        task.next_attempt_at.getTime(),
        deferredUntil.getTime(),
    );

    console.log(
        "Deferred recovery task persisted: OK",
    );

    fixture.prepared = true;
    await saveManifest();
    prepared = true;

    console.log(
        "Recovery fixture manifest saved: OK",
    );

    console.log(
        "Fixture operation ID:",
        fixture.operationId,
    );

    console.log("Fixture product PID:", fixture.pid);
}

async function cleanupFailedPreparation() {
    let succeeded = true;

    try {
        await pool.query(
            `
            DELETE FROM order_creation_tasks
            WHERE operation_id = $1::uuid
            `,
            [fixture.operationId],
        );

        const remaining = await pool.query(
            `
            SELECT COUNT(*) AS count
            FROM order_creation_tasks
            WHERE operation_id = $1::uuid
            `,
            [fixture.operationId],
        );

        assert.equal(
            Number(remaining.rows[0].count),
            0,
        );
    } catch (error) {
        succeeded = false;

        console.error(
            "Recovery preparation SQL cleanup failed:",
            error.code ?? error.name,
        );
    }

    try {
        if (ProductModel.db.readyState !== 1) {
            await connectDatabase();
        }

        await ProductModel.deleteMany({
            pname: fixture.productName,
        });

        assert.equal(
            await ProductModel.countDocuments({
                pname: fixture.productName,
            }),
            0,
        );
    } catch (error) {
        succeeded = false;

        console.error(
            "Recovery preparation product cleanup failed:",
            error.code ?? error.name,
        );
    }

    if (succeeded) {
        await unlink(manifestPath);

        console.log(
            "Failed preparation fixtures removed: OK",
        );
    } else {
        console.error(
            "Keep fixture manifest for recovery:",
            fixture.operationId,
            fixture.productName,
        );
    }
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Recovery preparation failed:",
        error.code ?? error.name,
        error.message,
    );

    if (error.code === "EEXIST") {
        console.error(
            "Existing fixture manifest kept unchanged",
        );
    }
} finally {
    customerClient.close();

    try {
        if (manifestCreated && !prepared) {
            await cleanupFailedPreparation();
        }
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Recovery preparation cleanup failed:",
            error.code ?? error.name,
            error.message,
        );
    } finally {
        try {
            await pool.end();
        } finally {
            await disconnectDatabase();
        }
    }
}