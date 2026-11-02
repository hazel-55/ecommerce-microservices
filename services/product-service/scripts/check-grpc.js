import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import {
    productConfig,
    productTransportConfig,
} from "../src/infrastructure/config/env.js";

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

function metadata(role = "CUSTOMER", token = productTransportConfig.gatewayToken) {
    const result = new grpc.Metadata();

    result.set("x-service-name", "api-gateway");
    result.set("x-service-token", token);
    result.set("x-user-id", role === "ADMIN" ? "1" : role === "SHIPMENT_STAFF" ? "2" : "13");
    result.set("x-user-role", role);

    return result;
}

function invoke(method, request, callerMetadata) {
    return new Promise((resolve, reject) => {
        client[method](
            request,
            callerMetadata,
            { deadline: new Date(Date.now() + 5000) },
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

async function expectRpcError(method, request, callerMetadata, expectedCode) {
    await assert.rejects(
        () => invoke(method, request, callerMetadata),
        (error) => {
            assert.equal(error.code, expectedCode);
            return true;
        },
    );
}

try {
    await expectRpcError(
        "ListProducts", {}, new grpc.Metadata(),
        grpc.status.UNAUTHENTICATED,
    );

    await expectRpcError(
        "ListProducts", {}, metadata("CUSTOMER", "incorrect-token"),
        grpc.status.UNAUTHENTICATED,
    );

    console.log("Product caller credentials over gRPC: OK");

    await expectRpcError(
        "GetProduct", { pid: 0 }, metadata(),
        grpc.status.INVALID_ARGUMENT,
    );

    console.log("Product read validation mapping: OK");

    await expectRpcError(
        "CreateProduct",
        { pname: "Must not be created", price: "25.00", quantity: 1 },
        metadata("CUSTOMER"),
        grpc.status.PERMISSION_DENIED,
    );

    await expectRpcError(
        "UpdateProduct",
        { pid: 1, pname: "Must not be updated", price: "25.00", quantity: 1 },
        metadata("SHIPMENT_STAFF"),
        grpc.status.PERMISSION_DENIED,
    );

    console.log("Product mutation role enforcement over gRPC: OK");

    await expectRpcError(
        "CreateProduct",
        { pname: "Invalid price", price: "1.001", quantity: 1 },
        metadata("ADMIN"),
        grpc.status.INVALID_ARGUMENT,
    );

    await expectRpcError(
        "UpdateProduct",
        { pid: 0, pname: "Invalid ID", price: "25.00", quantity: 1 },
        metadata("ADMIN"),
        grpc.status.INVALID_ARGUMENT,
    );

    await expectRpcError(
        "DeleteProduct", { pid: 0 }, metadata("ADMIN"),
        grpc.status.INVALID_ARGUMENT,
    );

    console.log("Product admin input validation over gRPC: OK");
} catch (error) {
    console.error("Product gRPC check failed:", error.message);
    process.exitCode = 1;
} finally {
    client.close();
}