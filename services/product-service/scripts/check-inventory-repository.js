import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import { ProductModel } from "../src/infrastructure/database/models/product.model.js";
import { StockReservationModel } from "../src/infrastructure/database/models/stock-reservation.model.js";
import { productRepository } from "../src/infrastructure/repositories/product.repository.js";
import { inventoryRepository } from "../src/infrastructure/repositories/inventory.repository.js";

const fixtureId = randomUUID();
const productNames = [
    `inventory_${fixtureId}_A`,
    `inventory_${fixtureId}_B`,
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
        quantity: 5,
    });

    const operationId = newOperationId();

    const request = {
        operationId,
        uid: 13,
        items: [
            { pid: second.pid, qty: 2 },
            { pid: first.pid, qty: 3 },
        ],
    };

    const reservation = await inventoryRepository.reserve(request);

    assert.deepEqual(reservation, {
        operationId,
        uid: 13,
        status: "RESERVED",
        items: [
            {
                pid: first.pid,
                qty: 3,
                unit_price: "25.50",
            },
            {
                pid: second.pid,
                qty: 2,
                unit_price: "10.00",
            },
        ],
    });

    await assertStock(first.pid, 7, 3);
    await assertStock(second.pid, 3, 2);

    console.log("Atomic multi-product reservation and price snapshots: OK");

    const replay = await inventoryRepository.reserve({
        ...request,
        items: [...request.items].reverse(),
    });

    assert.deepEqual(replay, reservation);

    await expectDomainError(
        () => inventoryRepository.reserve({
            ...request,
            uid: 14,
        }),
        "FORBIDDEN"
    );

    await expectDomainError(
        () => inventoryRepository.reserve({
            ...request,
            items: [{ pid: first.pid, qty: 1 }],
        }),
        "CONFLICT"
    );

    assert.deepEqual(
        await inventoryRepository.findByOperationId({
            operationId,
            uid: 13,
        }),
        reservation
    );

    await assertStock(first.pid, 7, 3);
    await assertStock(second.pid, 3, 2);

    console.log("Reservation replay and request binding: OK");

    const failedOperationId = newOperationId();

    await expectDomainError(
        () => inventoryRepository.reserve({
            operationId: failedOperationId,
            uid: 13,
            items: [
                { pid: first.pid, qty: 1 },
                { pid: second.pid, qty: 99 },
            ],
        }),
        "CONFLICT"
    );

    await assertStock(first.pid, 7, 3);
    await assertStock(second.pid, 3, 2);

    assert.equal(
        await inventoryRepository.findByOperationId({
            operationId: failedOperationId,
            uid: 13,
        }),
        null
    );

    console.log("Failed reservation rolls back all stock changes: OK");

    const competingIds = [
        newOperationId(),
        newOperationId(),
    ];

    const competing = await Promise.allSettled(
        competingIds.map((id) =>
            inventoryRepository.reserve({
                operationId: id,
                uid: 13,
                items: [{ pid: second.pid, qty: 2 }],
            })
        )
    );

    const succeeded = competing.filter(
        (result) => result.status === "fulfilled"
    );

    const rejected = competing.filter(
        (result) => result.status === "rejected"
    );

    assert.equal(succeeded.length, 1);
    assert.equal(rejected.length, 1);
    assert.ok(rejected[0].reason instanceof DomainError);
    assert.equal(rejected[0].reason.code, "CONFLICT");

    await assertStock(second.pid, 1, 4);

    console.log("Concurrent reservations cannot oversell: OK");

    const sharedOperationId = newOperationId();

    const sharedRequest = {
        operationId: sharedOperationId,
        uid: 13,
        items: [{ pid: first.pid, qty: 2 }],
    };

    const sameOperation = await Promise.allSettled([
        inventoryRepository.reserve(sharedRequest),
        inventoryRepository.reserve(sharedRequest),
    ]);

    for (const result of sameOperation) {
        if (result.status === "rejected") {
            throw result.reason;
        }
    }

    assert.deepEqual(
        sameOperation[0].value,
        sameOperation[1].value
    );

    await assertStock(first.pid, 5, 5);

    assert.equal(
        await StockReservationModel.countDocuments({
            _id: sharedOperationId,
        }),
        1
    );

    console.log("Concurrent same-operation retry deducts stock once: OK");
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
        "Inventory repository check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        if (ProductModel.db.readyState === 1) {
            await cleanup();
            console.log("Scoped inventory fixtures removed: OK");
        }
    } catch (error) {
        console.error(
            "Inventory fixture cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture product names:", productNames);
        console.error("Fixture operation IDs:", operationIds);
        process.exitCode = 1;
    } finally {
        await disconnectDatabase();
    }
}