import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { DomainError } from "../src/domain/errors.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import {
    productConfig,
    productTransportConfig,
} from "../src/infrastructure/config/env.js";
import { ProductModel } from "../src/infrastructure/database/models/product.model.js";
import { StockReservationModel } from "../src/infrastructure/database/models/stock-reservation.model.js";
import { productRepository } from "../src/infrastructure/repositories/product.repository.js";
import { inventoryRepository } from "../src/infrastructure/repositories/inventory.repository.js";

const fixtureId = randomUUID();

const productNames = ["A", "B", "C", "D"].map(
    (suffix) => `guard_${fixtureId}_${suffix}`
);

const operationIds = [];

const definition = loadSync(
    fileURLToPath(
        new URL("../../../proto/product.proto", import.meta.url)
    ),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    }
);

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.product.v1;

const client = new proto.ProductService(
    productConfig.grpcAddress,
    grpc.credentials.createInsecure()
);

function command() {
    const operationId = randomUUID();
    operationIds.push(operationId);

    return { operationId, uid: 13 };
}

function adminMetadata() {
    const metadata = new grpc.Metadata();

    metadata.set("x-service-name", "api-gateway");
    metadata.set(
        "x-service-token",
        productTransportConfig.gatewayToken
    );
    metadata.set("x-user-id", "1");
    metadata.set("x-user-role", "ADMIN");

    return metadata;
}

function invoke(method, input) {
    return new Promise((resolve, reject) => {
        client[method](
            input,
            adminMetadata(),
            { deadline: new Date(Date.now() + 10000) },
            (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }

                resolve(response);
            }
        );
    });
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

async function createProduct(name) {
    return productRepository.create({
        pname: name,
        price: "25.50",
        quantity: 10,
    });
}

async function main() {
    await connectDatabase();

    const first = await createProduct(productNames[0]);
    const firstCommand = command();

    await inventoryRepository.reserve({
        ...firstCommand,
        items: [{ pid: first.pid, qty: 2 }],
    });

    const before = await productRepository.findById(first.pid);

    const changedInput = {
        pname: productNames[0],
        price: "30.00",
        quantity: 99,
    };

    await expectDomainError(
        () => productRepository.update(first.pid, changedInput),
        "CONFLICT"
    );

    await expectDomainError(
        () => productRepository.deleteById(first.pid),
        "CONFLICT"
    );

    await assert.rejects(
        () => invoke("updateProduct", {
            pid: first.pid,
            ...changedInput,
        }),
        (error) => {
            assert.equal(error.code, grpc.status.ABORTED);
            return true;
        }
    );

    await assert.rejects(
        () => invoke("deleteProduct", { pid: first.pid }),
        (error) => {
            assert.equal(error.code, grpc.status.ABORTED);
            return true;
        }
    );

    assert.deepEqual(
        await productRepository.findById(first.pid),
        before
    );

    await assertStock(first.pid, 8, 2);

    console.log("Active reservation blocks product update and delete: OK");

    await inventoryRepository.release(firstCommand);

    const afterRelease = await productRepository.update(
        first.pid,
        {
            pname: productNames[0],
            price: "30.00",
            quantity: 12,
        }
    );

    assert.equal(afterRelease.quantity, 12);
    await assertStock(first.pid, 12, 0);

    const confirmCommand = command();

    await inventoryRepository.reserve({
        ...confirmCommand,
        items: [{ pid: first.pid, qty: 2 }],
    });

    await inventoryRepository.confirm(confirmCommand);
    await assertStock(first.pid, 10, 0);

    const afterConfirm = await productRepository.update(
        first.pid,
        {
            pname: productNames[0],
            price: "35.00",
            quantity: 15,
        }
    );

    assert.equal(afterConfirm.quantity, 15);

    assert.ok(
        await productRepository.deleteById(first.pid)
    );

    assert.equal(
        await productRepository.findById(first.pid),
        null
    );

    console.log("Release and confirm allow subsequent product mutations: OK");

    const legacy = await createProduct(productNames[1]);

    await ProductModel.updateOne(
        { pid: legacy.pid },
        { $unset: { reservedQuantity: "" } }
    );

    const legacyUpdated = await productRepository.update(
        legacy.pid,
        {
            pname: productNames[1],
            price: "10.00",
            quantity: 11,
        }
    );

    assert.equal(legacyUpdated.quantity, 11);
    assert.ok(await productRepository.deleteById(legacy.pid));

    console.log("Legacy product remains editable: OK");

    const updateRaceProduct = await createProduct(productNames[2]);
    const updateRaceCommand = command();

    const updateRace = await Promise.allSettled([
        inventoryRepository.reserve({
            ...updateRaceCommand,
            items: [{ pid: updateRaceProduct.pid, qty: 3 }],
        }),
        productRepository.update(updateRaceProduct.pid, {
            pname: productNames[2],
            price: "30.00",
            quantity: 20,
        }),
    ]);

    if (updateRace[0].status === "rejected") {
        throw updateRace[0].reason;
    }

    if (updateRace[1].status === "fulfilled") {
        await assertStock(updateRaceProduct.pid, 17, 3);
    } else {
        assert.ok(updateRace[1].reason instanceof DomainError);
        assert.equal(updateRace[1].reason.code, "CONFLICT");

        await assertStock(updateRaceProduct.pid, 7, 3);
    }

    console.log("Concurrent update and reserve preserve stock accounting: OK");

    const deleteRaceProduct = await createProduct(productNames[3]);
    const deleteRaceCommand = command();

    const deleteRace = await Promise.allSettled([
        inventoryRepository.reserve({
            ...deleteRaceCommand,
            items: [{ pid: deleteRaceProduct.pid, qty: 3 }],
        }),
        productRepository.deleteById(deleteRaceProduct.pid),
    ]);

    assert.equal(
        deleteRace.filter(
            (result) => result.status === "fulfilled"
        ).length,
        1
    );

    if (deleteRace[1].status === "fulfilled") {
        assert.ok(deleteRace[1].value);
        assert.equal(deleteRace[0].status, "rejected");
        assert.ok(deleteRace[0].reason instanceof DomainError);
        assert.equal(deleteRace[0].reason.code, "NOT_FOUND");

        assert.equal(
            await productRepository.findById(deleteRaceProduct.pid),
            null
        );

        assert.equal(
            await inventoryRepository.findByOperationId(
                deleteRaceCommand
            ),
            null
        );
    } else {
        assert.equal(deleteRace[0].status, "fulfilled");
        assert.ok(deleteRace[1].reason instanceof DomainError);
        assert.equal(deleteRace[1].reason.code, "CONFLICT");

        await assertStock(deleteRaceProduct.pid, 7, 3);
    }

    console.log("Concurrent delete and reserve settle safely: OK");
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
        "Inventory CRUD guard check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    client.close();

    try {
        if (ProductModel.db.readyState === 1) {
            await cleanup();
            console.log("Scoped guard fixtures removed: OK");
        }
    } catch (error) {
        console.error(
            "Guard fixture cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture product names:", productNames);
        console.error("Fixture operation IDs:", operationIds);
        process.exitCode = 1;
    } finally {
        await disconnectDatabase();
    }
}