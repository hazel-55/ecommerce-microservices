import assert from "node:assert/strict";
import {
    gatewayConfig,
    rateLimitConfig,
} from "../src/config/env.js";

const baseUrl = `http://127.0.0.1:${gatewayConfig.port}`;

try {
    let blockedResponse;
    let validationCount = 0;

    for (let index = 0; index <= rateLimitConfig.authMaxRequests; index++) {
        const response = await fetch(`${baseUrl}/api/auth/login`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({}),
            signal: AbortSignal.timeout(15_000),
        });

        const body = await response.json();

        if (response.status === 429) {
            blockedResponse = response;

            assert.equal(body.message, "Too many requests");
            break;
        }

        assert.equal(response.status, 400);
        validationCount++;
    }

    assert.ok(blockedResponse, "Expected HTTP 429 within the request limit");

    assert.equal(
        blockedResponse.headers.get("X-RateLimit-Limit"),
        String(rateLimitConfig.authMaxRequests),
    );
    assert.equal(
        blockedResponse.headers.get("X-RateLimit-Remaining"),
        "0",
    );

    const retryAfter = Number(
        blockedResponse.headers.get("Retry-After"),
    );

    assert.ok(
        Number.isInteger(retryAfter) &&
        retryAfter >= 1 &&
        retryAfter <= rateLimitConfig.windowSeconds,
    );

    assert.ok(blockedResponse.headers.get("X-Request-Id"));

    console.log("HTTP auth rate limiting: OK");
    console.log("429 response and rate headers: OK");
    console.log("Retry-After and request ID: OK");
    console.log(`Validation responses before blocking: ${validationCount}`);
    console.log(`Retry after: ${retryAfter} seconds`);
} catch (error) {
    console.error("HTTP rate limit check failed:", error.message);
    process.exitCode = 1;
}