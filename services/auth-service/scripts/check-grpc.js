import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";

const definition = loadSync(
    fileURLToPath(new URL("../../../proto/auth.proto", import.meta.url)),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    }
);

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.auth.v1;

const client = new proto.AuthService(
    "127.0.0.1:50051",
    grpc.credentials.createInsecure()
);

function invoke(method, request) {
    return new Promise((resolve, reject) => {
        client[method](
            request,
            { deadline: Date.now() + 10000 },
            (error, response) => {
                if (error) reject(error);
                else resolve(response);
            }
        );
    });
}

async function expectCode(method, request, code) {
    await assert.rejects(
        () => invoke(method, request),
        (error) => error.code === code
    );
}

try {
    const username = `missing_${randomUUID().replaceAll("-", "")}`;

    await expectCode(
        "Register",
        {
            username,
            password: "P@ss123",
            phone: "0900000000",
        },
        grpc.status.INVALID_ARGUMENT
    );

    await expectCode(
        "Register",
        {
            username,
            password: "x".repeat(73),
            email: `${username}@example.com`,
            phone: "0900000000",
        },
        grpc.status.INVALID_ARGUMENT
    );

    await expectCode(
        "Login",
        { username: "", password: "P@ss123" },
        grpc.status.INVALID_ARGUMENT
    );

    await expectCode(
        "Login",
        { username, password: "P@ss123" },
        grpc.status.UNAUTHENTICATED
    );

    console.log("Auth gRPC server check: OK");
    console.log("Validation error mapping: OK");
    console.log("Invalid credentials rejected: OK");
} catch (error) {
    console.error("Auth gRPC check failed:", error.message);
    process.exitCode = 1;
} finally {
    client.close();
}