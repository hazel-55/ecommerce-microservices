import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import {
    productConfig,
    productTransportConfig,
} from "../src/infrastructure/config/env.js";
import {
    readOrderActor,
} from "../src/interfaces/grpc/caller.js";

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

function metadata({
    service = "order-service",
    token = productTransportConfig.orderToken,
    uid = "13",
    role = "CUSTOMER",
} = {}) {
    const result = new grpc.Metadata();

    result.set("x-service-name", service);
    result.set("x-service-token", token);
    result.set("x-user-id", uid);
    result.set("x-user-role", role);

    return result;
}

function invoke(method, input, caller = metadata()) {
    return new Promise((resolve, reject) => {
        client[method](
            input,
            caller,
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

async function expectGrpcError(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    const command = {
        operationId: randomUUID(),
        uid: 13,
    };

    assert.deepEqual(readOrderActor(metadata()), {
        uid: 13,
        role: "CUSTOMER",
    });

    await expectGrpcError(
        () => invoke(
            "getStockReservation",
            command,
            metadata({ token: "invalid-order-token" })
        ),
        grpc.status.UNAUTHENTICATED
    );

    console.log("Product Order caller credentials over gRPC: OK");

    await expectGrpcError(
        () => invoke("reserveStock", {
            operationId: "invalid",
            uid: 13,
            items: [{ pid: 1, qty: 1 }],
        }),
        grpc.status.INVALID_ARGUMENT
    );

    await expectGrpcError(
        () => invoke("reserveStock", {
            operationId: randomUUID(),
            uid: 14,
            items: [{ pid: 1, qty: 1 }],
        }),
        grpc.status.PERMISSION_DENIED
    );

    console.log("Inventory RPC input and actor binding: OK");

    await expectGrpcError(
        () => invoke("getStockReservation", command),
        grpc.status.NOT_FOUND
    );

    console.log("Inventory RPC missing reservation mapping: OK");

    await expectGrpcError(
        () => invoke("listProducts", {}),
        grpc.status.UNAUTHENTICATED
    );

    await expectGrpcError(
        () => invoke("createProduct", {
            pname: "Must not be created",
            price: "25.00",
            quantity: 1,
        }),
        grpc.status.UNAUTHENTICATED
    );

    await expectGrpcError(
        () => invoke(
            "getStockReservation",
            command,
            metadata({
                service: "api-gateway",
                token: productTransportConfig.gatewayToken,
                uid: "1",
                role: "ADMIN",
            })
        ),
        grpc.status.UNAUTHENTICATED
    );

    console.log("Caller separation for CRUD and inventory: OK");

    const duplicateUid = metadata();
    duplicateUid.add("x-user-id", "13");

    for (const invalidMetadata of [
        metadata({ uid: "0" }),
        metadata({ role: "UNKNOWN" }),
        duplicateUid,
    ]) {
        await expectGrpcError(
            () => invoke(
                "getStockReservation",
                command,
                invalidMetadata
            ),
            grpc.status.UNAUTHENTICATED
        );
    }

    console.log("Invalid Order actor metadata rejected: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Inventory gRPC check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    client.close();
}