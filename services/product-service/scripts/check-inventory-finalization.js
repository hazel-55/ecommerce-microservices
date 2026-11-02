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
    `finalization_${fixtureId}_A`,
    `finalization_${fixtureId}_B`,
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
        quantity: 20,
    });

    const second = await productRepository.create({
        pname: productNames[1],
        price: "10.00",
        quantity: 10,
    });

    const confirmCommand = {
        operationId: newOperationId(),
        uid: 13,
    };

    await inventoryRepository.reserve({
        ...confirmCommand,
        items: [
            { pid: first.pid, qty: 3 },
            { pid: second.pid, qty: 2 },
        ],
    });

    const confirmed =
        await inventoryRepository.confirm(confirmCommand);

    assert.equal(confirmed.status, "CONFIRMED");
    await assertStock(first.pid, 17, 0);
    await assertStock(second.pid, 8, 0);

    assert.deepEqual(
        await inventoryRepository.confirm(confirmCommand),
        confirmed
    );

    await assertStock(first.pid, 17, 0);
    await assertStock(second.pid, 8, 0);

    console.log("Confirmed reservation keeps deducted stock: OK");

    const releaseCommand = {
        operationId: newOperationId(),
        uid: 13,
    };

    await inventoryRepository.reserve({
        ...releaseCommand,
        items: [
            { pid: first.pid, qty: 4 },
            { pid: second.pid, qty: 1 },
        ],
    });

    await assertStock(first.pid, 13, 4);
    await assertStock(second.pid, 7, 1);

    const released =
        await inventoryRepository.release(releaseCommand);

    assert.equal(released.status, "RELEASED");

    assert.deepEqual(
        await inventoryRepository.release(releaseCommand),
        released
    );

    await assertStock(first.pid, 17, 0);
    await assertStock(second.pid, 8, 0);

    console.log("Released reservation restores stock once: OK");

    await expectDomainError(
        () => inventoryRepository.release(confirmCommand),
        "CONFLICT"
    );

    await expectDomainError(
        () => inventoryRepository.confirm(releaseCommand),
        "CONFLICT"
    );

    await expectDomainError(
        () => inventoryRepository.reserve({
            ...releaseCommand,
            items: [{ pid: first.pid, qty: 4 }],
        }),
        "CONFLICT"
    );

    await expectDomainError(
        () => inventoryRepository.confirm({
            ...confirmCommand,
            uid: 14,
        }),
        "FORBIDDEN"
    );

    await expectDomainError(
        () => inventoryRepository.release({
            ...releaseCommand,
            uid: 14,
        }),
        "FORBIDDEN"
    );

    await assertStock(first.pid, 17, 0);

    console.log("Terminal reservation transitions and owner binding: OK");

    const earlyReleaseCommand = {
        operationId: newOperationId(),
        uid: 13,
    };

    const earlyRelease =
        await inventoryRepository.release(earlyReleaseCommand);

    assert.equal(earlyRelease.status, "RELEASED");
    assert.deepEqual(earlyRelease.items, []);

    await expectDomainError(
        () => inventoryRepository.reserve({
            ...earlyReleaseCommand,
            items: [{ pid: first.pid, qty: 1 }],
        }),
        "CONFLICT"
    );

    await expectDomainError(
        () => inventoryRepository.confirm(earlyReleaseCommand),
        "CONFLICT"
    );

    await expectDomainError(
        () => inventoryRepository.confirm({
            operationId: newOperationId(),
            uid: 13,
        }),
        "NOT_FOUND"
    );

    await assertStock(first.pid, 17, 0);

    console.log("Release-before-reserve prevents late deduction: OK");

    const raceCommand = {
        operationId: newOperationId(),
        uid: 13,
    };

    await inventoryRepository.reserve({
        ...raceCommand,
        items: [{ pid: first.pid, qty: 2 }],
    });

    const race = await Promise.allSettled([
        inventoryRepository.confirm(raceCommand),
        inventoryRepository.release(raceCommand),
    ]);

    const succeeded = race.filter(
        (result) => result.status === "fulfilled"
    );

    const rejected = race.filter(
        (result) => result.status === "rejected"
    );

    assert.equal(succeeded.length, 1);
    assert.equal(rejected.length, 1);
    assert.ok(rejected[0].reason instanceof DomainError);
    assert.equal(rejected[0].reason.code, "CONFLICT");

    const settled =
        await inventoryRepository.findByOperationId(raceCommand);

    assert.ok(
        ["CONFIRMED", "RELEASED"].includes(settled.status)
    );

    assert.equal(settled.status, succeeded[0].value.status);

    const availableAfterRace =
        settled.status === "CONFIRMED" ? 15 : 17;

    await assertStock(first.pid, availableAfterRace, 0);

    console.log("Concurrent confirm and release settle once: OK");

    const reserveReleaseCommand = {
        operationId: newOperationId(),
        uid: 13,
    };

    const reserveRelease = await Promise.allSettled([
        inventoryRepository.reserve({
            ...reserveReleaseCommand,
            items: [{ pid: first.pid, qty: 1 }],
        }),
        inventoryRepository.release(reserveReleaseCommand),
    ]);

    assert.equal(reserveRelease[1].status, "fulfilled");

    if (reserveRelease[0].status === "rejected") {
        assert.ok(
            reserveRelease[0].reason instanceof DomainError
        );
        assert.equal(
            reserveRelease[0].reason.code,
            "CONFLICT"
        );
    }

    const finalReservation =
        await inventoryRepository.findByOperationId(
            reserveReleaseCommand
        );

    assert.equal(finalReservation.status, "RELEASED");

    await assertStock(first.pid, availableAfterRace, 0);
    await assertStock(second.pid, 8, 0);

    console.log("Concurrent reserve and release leave no held stock: OK");
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
        "Inventory finalization check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        if (ProductModel.db.readyState === 1) {
            await cleanup();
            console.log("Scoped finalization fixtures removed: OK");
        }
    } catch (error) {
        console.error(
            "Finalization fixture cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture product names:", productNames);
        console.error("Fixture operation IDs:", operationIds);
        process.exitCode = 1;
    } finally {
        await disconnectDatabase();
    }
}