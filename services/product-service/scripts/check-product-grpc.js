import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import {
    productConfig,
    productTransportConfig,
} from "../src/infrastructure/config/env.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import { ProductModel } from "../src/infrastructure/database/models/product.model.js";

const definition = protoLoader.loadSync(
    fileURLToPath(new URL("../../../proto/product.proto", import.meta.url)),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    },
);

const productPackage = grpc.loadPackageDefinition(definition)
    .ecommerce.product.v1;

const client = new productPackage.ProductService(
    productConfig.grpcAddress,
    grpc.credentials.createInsecure(),
);

const suffix = randomUUID();
const initialName = `grpc_${suffix}_Initial`;
const updatedName = `grpc_${suffix}_Updated`;
const fixtureNames = [initialName, updatedName];

function metadata(role) {
    const result = new grpc.Metadata();

    result.set("x-service-name", "api-gateway");
    result.set("x-service-token", productTransportConfig.gatewayToken);
    result.set("x-user-id", role === "ADMIN" ? "1" : "13");
    result.set("x-user-role", role);

    return result;
}

const adminMetadata = metadata("ADMIN");
const customerMetadata = metadata("CUSTOMER");

function invoke(method, request, callerMetadata) {
    return new Promise((resolve, reject) => {
        client[method](
            request,
            callerMetadata,
            { deadline: new Date(Date.now() + 10_000) },
            (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }

                resolve(response);
            },
        );
    });
}

async function expectRpcError(operation, expectedCode) {
    await assert.rejects(operation, (error) => {
        assert.equal(error.code, expectedCode);
        return true;
    });
}

try {
    const created = await invoke(
        "CreateProduct",
        {
            pname: initialName,
            price: "25",
            quantity: 10,
        },
        adminMetadata,
    );

    assert.ok(Number.isInteger(created.pid) && created.pid > 0);
    assert.deepEqual(created, {
        pid: created.pid,
        pname: initialName,
        price: "25.00",
        quantity: 10,
    });

    const read = await invoke(
        "GetProduct",
        { pid: created.pid },
        customerMetadata,
    );

    assert.deepEqual(read, created);

    const listed = await invoke(
        "ListProducts",
        {},
        customerMetadata,
    );

    assert.deepEqual(
        listed.items.find((item) => item.pid === created.pid),
        created,
    );

    console.log("Product gRPC create, read and list: OK");

    const update = await invoke(
        "UpdateProduct",
        {
            pid: created.pid,
            pname: updatedName,
            price: "30.5",
            quantity: 0,
        },
        adminMetadata,
    );

    assert.deepEqual(update, {
        message: "Product updated successfully",
        pid: created.pid,
    });

    const updated = await invoke(
        "GetProduct",
        { pid: created.pid },
        customerMetadata,
    );

    assert.deepEqual(updated, {
        pid: created.pid,
        pname: updatedName,
        price: "30.50",
        quantity: 0,
    });

    console.log("Product gRPC update and decimal normalization: OK");

    await expectRpcError(
        () => invoke(
            "DeleteProduct",
            { pid: created.pid },
            customerMetadata,
        ),
        grpc.status.PERMISSION_DENIED,
    );

    assert.deepEqual(
        await invoke(
            "GetProduct",
            { pid: created.pid },
            customerMetadata,
        ),
        updated,
    );

    console.log("Non-admin gRPC delete forbidden: OK");

    const deleted = await invoke(
        "DeleteProduct",
        { pid: created.pid },
        adminMetadata,
    );

    assert.deepEqual(deleted, {
        message: "Product deleted successfully",
        pid: created.pid,
    });

    await expectRpcError(
        () => invoke(
            "GetProduct",
            { pid: created.pid },
            customerMetadata,
        ),
        grpc.status.NOT_FOUND,
    );

    console.log("Product gRPC delete and NOT_FOUND: OK");
} catch (error) {
    console.error("Product live gRPC check failed:", error.message);
    process.exitCode = 1;
} finally {
    client.close();

    try {
        await connectDatabase();

        await ProductModel.deleteMany({
            pname: { $in: fixtureNames },
        });

        assert.equal(
            await ProductModel.countDocuments({
                pname: { $in: fixtureNames },
            }),
            0,
        );

        console.log("Scoped gRPC fixtures removed: OK");
    } catch (error) {
        console.error("gRPC fixture cleanup failed:", {
            errorName: error.name,
            code: error.code ?? "UNKNOWN",
        });

        process.exitCode = 1;
    } finally {
        await disconnectDatabase();
    }
}