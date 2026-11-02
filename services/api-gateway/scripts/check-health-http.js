import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { gatewayConfig } from "../src/config/env.js";

const mode = process.argv[2];

const baseUrl =
    `http://127.0.0.1:${gatewayConfig.port}`;

function fail(code) {
    const error = new Error(code);
    error.code = code;
    throw error;
}

async function readHealth(authorization) {
    const startedAt = performance.now();

    const response = await fetch(
        `${baseUrl}/api/health`,
        {
            headers: authorization
                ? { Authorization: authorization }
                : {},
            signal: AbortSignal.timeout(3000),
        },
    );

    if (![200, 503].includes(response.status)) {
        fail(`UNEXPECTED_HTTP_${response.status}`);
    }

    const data = await response.json();

    assert.equal(
        response.headers.get("cache-control"),
        "no-store",
    );

    assert.match(
        response.headers.get("x-request-id") ?? "",
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );

    assert.equal(
        response.headers.get("retry-after"),
        null,
    );

    return {
        status: response.status,
        data,
        elapsedMs: Math.round(
            performance.now() - startedAt,
        ),
    };
}

async function main() {
    if (!["up", "down"].includes(mode)) {
        fail("USE_up_OR_down");
    }

    const expectedStatus = mode === "up"
        ? 200
        : 503;

    const expectedBody = mode === "up"
        ? {
            status: "UP",
            message: "E-commerce API is running",
        }
        : {
            status: "DOWN",
            message: "E-commerce API is not ready",
        };

    const until = Date.now() + 20_000;
    let result;

    do {
        result = await readHealth();

        if (result.status === expectedStatus) {
            break;
        }

        await sleep(250);
    } while (Date.now() < until);

    assert.equal(result.status, expectedStatus);
    assert.deepEqual(result.data, expectedBody);

    console.log(
        "Live health HTTP status and exact response: OK",
    );

    const publicResult = await readHealth(
        "Bearer deliberately-invalid",
    );

    assert.equal(
        publicResult.status,
        expectedStatus,
    );

    assert.deepEqual(
        publicResult.data,
        expectedBody,
    );

    console.log(
        "Live health remains public without a valid JWT: OK",
    );

    console.log(
        "Live health no-store, request ID and limiter bypass: OK",
    );

    console.log({
        mode,
        status: result.status,
        elapsedMs: result.elapsedMs,
    });
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Live health check failed:",
        error.code ?? error.name,
    );
}