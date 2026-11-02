import assert from "node:assert/strict";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import {
    customerTransportConfig,
} from "../src/infrastructure/config/env.js";

const definition = loadSync(
    fileURLToPath(new URL("../../../proto/customer.proto", import.meta.url)),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    }
);

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.customer.v1;

const client = new proto.CustomerService(
    "127.0.0.1:50052",
    grpc.credentials.createInsecure()
);

function metadata(service, token) {
    const value = new grpc.Metadata();
    value.set("x-service-name", service);
    value.set("x-service-token", token);
    return value;
}

const gateway = metadata(
    "api-gateway",
    customerTransportConfig.gatewayToken
);

gateway.set("x-user-id", "1");
gateway.set("x-user-role", "CUSTOMER");

const auth = metadata(
    "auth-service",
    customerTransportConfig.authToken
);

function invoke(method, request, headers) {
    return new Promise((resolve, reject) => {
        client[method](
            request,
            headers,
            { deadline: Date.now() + 5000 },
            (error, response) => {
                if (error) reject(error);
                else resolve(response);
            }
        );
    });
}

async function expectCode(method, request, headers, code) {
    await assert.rejects(
        () => invoke(method, request, headers),
        (error) => error.code === code
    );
}

try {
    await expectCode(
        "GetCustomer",
        { uid: 1 },
        new grpc.Metadata(),
        grpc.status.UNAUTHENTICATED
    );

    await expectCode(
        "GetCustomer",
        { uid: 2 },
        gateway,
        grpc.status.PERMISSION_DENIED
    );

    await expectCode(
        "GetCustomer",
        { uid: 1 },
        gateway,
        grpc.status.NOT_FOUND
    );

    await expectCode(
        "CreateCustomer",
        { uid: 0, fullname: "John Doe" },
        gateway,
        grpc.status.UNAUTHENTICATED
    );

    await expectCode(
        "CreateCustomer",
        { uid: 0, fullname: "John Doe" },
        auth,
        grpc.status.INVALID_ARGUMENT
    );

    await expectCode(
        "UpdateCustomer",
        { uid: 1, fullname: "John Doe", mid: 0 },
        gateway,
        grpc.status.INVALID_ARGUMENT
    );

    console.log("Customer gRPC server check: OK");
    console.log("Caller validation: OK");
    console.log("Authorization and error mapping: OK");
    console.log("Optional mid validation: OK");
} catch (error) {
    console.error("gRPC check failed:", error.message);
    process.exitCode = 1;
} finally {
    client.close();
}