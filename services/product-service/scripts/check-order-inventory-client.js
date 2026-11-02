import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../../order-service/src/domain/errors.js";
import {
    productClient as orderProductClient,
    createProductClient,
} from "../../order-service/src/infrastructure/grpc/product.client.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import { ProductModel } from "../src/infrastructure/database/models/product.model.js";
import { StockReservationModel } from "../src/infrastructure/database/models/stock-reservation.model.js";
import { productRepository } from "../src/infrastructure/repositories/product.repository.js";

const actor = { uid: 13, role: "CUSTOMER" };

const invalidClient = createProductClient({
    serviceToken: "invalid-order-product-token",
});

const fixtureId = randomUUID();
const productNames = [
    `order_client_${fixtureId}_A`,
    `order_client_${fixtureId}_B`,
];

const operationIds = [];

function newOperationId() {
    const id = randomUUID();
    operationIds.push(id);
    return id;
}

async function expectDomainError(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function assertStock(pid, quantity, reservedQuantity) {
    const product = await ProductModel.findOne({ pid }).lean();

    assert.ok(product);
    assert.equal(product.quantity, quantity);
    assert.equal(product.reservedQuantity, reservedQuantity);
}

async function main() {
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

    await expectDomainError(
        () => orderProductClient.getStockReservation(
            "invalid",
            actor
        ),
        "INVALID_ARGUMENT"
    );

    await expectDomainError(
        () => orderProductClient.reserveStock(
            newOperationId(),
            [],
            actor
        ),
        "INVALID_ARGUMENT"
    );

    console.log("Order inventory client input validation: OK");

    const confirmId = newOperationId();

    const reserved = await orderProductClient.reserveStock(
        confirmId,
        [
            { pid: second.pid, qty: 1 },
            { pid: first.pid, qty: 2 },
        ],
        actor
    );

    assert.deepEqual(reserved, {
        operationId: confirmId,
        uid: 13,
        status: "RESERVED",
        items: [
            { pid: first.pid, qty: 2, unit_price: "25.50" },
            { pid: second.pid, qty: 1, unit_price: "10.00" },
        ],
    });

    await assertStock(first.pid, 8, 2);
    await assertStock(second.pid, 5, 1);

    console.log("Order inventory client reserve and price snapshots: OK");

    const confirmed = await orderProductClient.confirmStock(
        confirmId,
        actor
    );

    assert.equal(confirmed.status, "CONFIRMED");
    assert.deepEqual(confirmed.items, reserved.items);

    assert.deepEqual(
        await orderProductClient.getStockReservation(
            confirmId,
            actor
        ),
        confirmed
    );

    await assertStock(first.pid, 8, 0);
    await assertStock(second.pid, 5, 0);

    console.log("Order inventory client confirm and lookup: OK");

    const releaseId = newOperationId();

    await orderProductClient.reserveStock(
        releaseId,
        [
            { pid: first.pid, qty: 1 },
            { pid: second.pid, qty: 2 },
        ],
        actor
    );

    const released = await orderProductClient.releaseStock(
        releaseId,
        actor
    );

    assert.equal(released.status, "RELEASED");

    assert.deepEqual(
        await orderProductClient.getStockReservation(
            releaseId,
            actor
        ),
        released
    );

    const earlyReleaseId = newOperationId();

    const earlyRelease = await orderProductClient.releaseStock(
        earlyReleaseId,
        actor
    );

    assert.equal(earlyRelease.status, "RELEASED");
    assert.deepEqual(earlyRelease.items, []);

    await expectDomainError(
        () => orderProductClient.reserveStock(
            earlyReleaseId,
            [{ pid: first.pid, qty: 1 }],
            actor
        ),
        "CONFLICT"
    );

    await assertStock(first.pid, 8, 0);
    await assertStock(second.pid, 5, 0);

    console.log("Order inventory client release and late-reserve protection: OK");

    await expectDomainError(
        () => orderProductClient.releaseStock(confirmId, actor),
        "CONFLICT"
    );

    const failedId = newOperationId();

    await expectDomainError(
        () => orderProductClient.reserveStock(
            failedId,
            [
                { pid: first.pid, qty: 1 },
                { pid: second.pid, qty: 99 },
            ],
            actor
        ),
        "CONFLICT"
    );

    await expectDomainError(
        () => orderProductClient.getStockReservation(
            failedId,
            actor
        ),
        "NOT_FOUND"
    );

    await expectDomainError(
        () => orderProductClient.getStockReservation(
            confirmId,
            { uid: 14, role: "CUSTOMER" }
        ),
        "FORBIDDEN"
    );

    await expectDomainError(
        () => invalidClient.getStockReservation(
            confirmId,
            actor
        ),
        "INTERNAL"
    );

    await assertStock(first.pid, 8, 0);
    await assertStock(second.pid, 5, 0);

    console.log("Order inventory client conflict and error mapping: OK");
}

async function cleanup() {
    await StockReservationModel.deleteMany({
        _id: { $in: operationIds },
    });

    await ProductModel.deleteMany({
        pname: { $in: productNames },
    });

    assert.equal(
        await StockReservationModel.countDocuments({
            _id: { $in: operationIds },
        }),
        0
    );

    assert.equal(
        await ProductModel.countDocuments({
            pname: { $in: productNames },
        }),
        0
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Order inventory client check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    orderProductClient.close();
    invalidClient.close();

    try {
        if (ProductModel.db.readyState === 1) {
            await cleanup();

            console.log(
                "Scoped Order inventory client fixtures removed: OK"
            );
        }
    } catch (error) {
        console.error(
            "Order inventory client fixture cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture product names:", productNames);
        console.error("Fixture operation IDs:", operationIds);
        process.exitCode = 1;
    } finally {
        await disconnectDatabase();
    }
}