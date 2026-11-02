import assert from "node:assert/strict";
import jwt from "jsonwebtoken";
import { gatewayConfig } from "../src/config/env.js";

const baseUrl = `http://127.0.0.1:${gatewayConfig.port}`;

async function post(path, body) {
    const response = await fetch(`${baseUrl}${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(15_000),
    });

    return { response, body: await response.json() };
}

try {
    const invalidRegister = await post("/api/auth/register", {
        username: "",
        password: "",
        email: "",
        phone: "",
    });

    assert.equal(invalidRegister.response.status, 400);
    assert.equal(typeof invalidRegister.body.message, "string");
    console.log("HTTP registration validation: OK");

    const username = "recovery_019cd44745364961bad7a1b684ce8a96";

    const invalidLogin = await post("/api/auth/login", {
        username,
        password: "incorrect-password",
    });

    assert.equal(invalidLogin.response.status, 401);
    console.log("HTTP invalid credentials: OK");

    const login = await post("/api/auth/login", {
        username,
        password: "P@ss123",
    });

    assert.equal(login.response.status, 200);
    assert.equal(login.body.tokenType, "Bearer");
    assert.equal(login.body.expiresIn, 3600);

    const payload = jwt.verify(login.body.accessToken, gatewayConfig.jwtSecret, {
        algorithms: ["HS256"],
        issuer: gatewayConfig.jwtIssuer,
        audience: gatewayConfig.jwtAudience,
    });

    assert.equal(payload.sub, "12");
    assert.equal(payload.role, "CUSTOMER");
    console.log("HTTP login and JWT: OK");

    assert.ok(login.response.headers.get("X-Request-Id"));
    assert.equal(
        login.response.headers.get("X-RateLimit-Limit"),
        String(gatewayConfigAuthLimit()),
    );

    const remaining = login.response.headers.get("X-RateLimit-Remaining");
    assert.notEqual(remaining, null);
    assert.ok(Number(remaining) >= 0);
    console.log("Request ID and auth rate headers: OK");

    const missingRoute = await fetch(`${baseUrl}/api/unknown`, {
        signal: AbortSignal.timeout(15_000),
    });

    assert.equal(missingRoute.status, 404);
    console.log("Unknown route returns 404: OK");
} catch (error) {
    console.error("Auth HTTP check failed:", error.message);
    process.exitCode = 1;
}

function gatewayConfigAuthLimit() {
    return Number(process.env.AUTH_RATE_LIMIT_MAX_REQUESTS);
}