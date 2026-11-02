import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import jwt from "jsonwebtoken";
import { gatewayConfig } from "../src/config/env.js";
import {
    authClient,
    mapAuthRpcError,
} from "../src/infrastructure/grpc/auth.client.js";

async function expectHttpError(operation, expectedStatus) {
    await assert.rejects(operation, (error) => {
        assert.equal(error.status, expectedStatus);
        return true;
    });
}

try {
    await expectHttpError(
        () => authClient.login({ username: 123, password: "P@ss123" }),
        400,
    );

    await expectHttpError(
        () => authClient.register(null),
        400,
    );

    console.log("Gateway Auth input shape checks: OK");

    await expectHttpError(
        () => authClient.register({
            username: "",
            password: "",
            email: "",
            phone: "",
        }),
        400,
    );

    console.log("Auth validation error mapping: OK");

    const username = "recovery_019cd44745364961bad7a1b684ce8a96";

    await expectHttpError(
        () => authClient.login({
            username,
            password: "incorrect-password",
        }),
        401,
    );

    console.log("Invalid credentials map to 401: OK");

    const result = await authClient.login({
        username,
        password: "P@ss123",
    });

    assert.equal(result.tokenType, "Bearer");
    assert.equal(result.expiresIn, 3600);
    assert.equal(typeof result.accessToken, "string");

    const payload = jwt.verify(
        result.accessToken,
        gatewayConfig.jwtSecret,
        {
            algorithms: ["HS256"],
            issuer: gatewayConfig.jwtIssuer,
            audience: gatewayConfig.jwtAudience,
        },
    );

    assert.equal(payload.sub, "12");
    assert.equal(payload.role, "CUSTOMER");

    console.log("Gateway to Auth live login: OK");
    console.log("Returned JWT verification: OK");

    const mappings = [
        [grpc.status.INVALID_ARGUMENT, 400],
        [grpc.status.UNAUTHENTICATED, 401],
        [grpc.status.PERMISSION_DENIED, 403],
        [grpc.status.ALREADY_EXISTS, 409],
        [grpc.status.UNAVAILABLE, 503],
        [grpc.status.DEADLINE_EXCEEDED, 503],
        [grpc.status.INTERNAL, 500],
    ];

    for (const [code, expectedStatus] of mappings) {
        const error = mapAuthRpcError({
            code,
            details: "Internal information must not be exposed",
        });

        assert.equal(error.status, expectedStatus);
        assert.ok(!error.message.includes("Internal information"));
    }

    console.log("Auth RPC error mapping table: OK");
} catch (error) {
    console.error("Gateway Auth client check failed:", error.message);
    process.exitCode = 1;
} finally {
    authClient.close();
}