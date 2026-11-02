import assert from "node:assert/strict";
import express from "express";

import { app } from "../src/app.js";
import {
    createHealthRouter,
} from "../src/routes/health.routes.js";
import {
    createRedisHealthCheck,
} from "../src/infrastructure/health/redis.health.js";
import {
    redisClient,
} from "../src/infrastructure/redis.js";

import { authClient } from "../src/infrastructure/grpc/auth.client.js";
import { customerClient } from "../src/infrastructure/grpc/customer.client.js";
import { productClient } from "../src/infrastructure/grpc/product.client.js";
import { orderClient } from "../src/infrastructure/grpc/order.client.js";
import { shipmentClient } from "../src/infrastructure/grpc/shipment.client.js";

const UP = {
    status: "UP",
    message: "E-commerce API is running",
};

const DOWN = {
    status: "DOWN",
    message: "E-commerce API is not ready",
};

const servers = [];

let mode = "UP";
let pingCount = 0;
let lastSignal;
let pending;
let downstreamCalls = 0;

function deferred() {
    let resolve;
    let reject;

    const promise = new Promise((done, fail) => {
        resolve = done;
        reject = fail;
    });

    return { promise, resolve, reject };
}

const redis = {
    isReady: true,

    withCommandOptions(options) {
        assert.ok(
            options.abortSignal instanceof AbortSignal,
        );

        assert.equal(options.timeout, 50);

        lastSignal = options.abortSignal;

        return {
            ping() {
                pingCount += 1;

                if (mode === "WAIT") {
                    return pending.promise;
                }

                if (mode === "ERROR") {
                    return Promise.reject(
                        new Error("PRIVATE_REDIS_DETAIL"),
                    );
                }

                return Promise.resolve(
                    mode === "BAD_REPLY"
                        ? "unexpected"
                        : "PONG",
                );
            },
        };
    },
};

async function listen(application) {
    const server = application.listen(
        0,
        "127.0.0.1",
    );

    servers.push(server);

    await new Promise((resolve, reject) => {
        server.once("listening", resolve);
        server.once("error", reject);
    });

    return `http://127.0.0.1:${server.address().port}`;
}

async function getHealth(baseUrl, authorization) {
    const response = await fetch(
        `${baseUrl}/api/health`,
        {
            headers: authorization
                ? { Authorization: authorization }
                : {},
            signal: AbortSignal.timeout(3000),
        },
    );

    const data = await response.json();

    assert.equal(
        response.headers.get("cache-control"),
        "no-store",
    );

    return { response, data };
}

function expectHealth(result, status, data) {
    assert.equal(result.response.status, status);
    assert.deepEqual(result.data, data);
}

async function main() {
    assert.throws(
        () => createRedisHealthCheck(),
        TypeError,
    );

    for (const timeoutMs of [
        0,
        -1,
        1.5,
        5001,
        NaN,
    ]) {
        assert.throws(
            () => createRedisHealthCheck({
                redis,
                timeoutMs,
            }),
            TypeError,
        );
    }

    assert.equal(pingCount, 0);

    console.log(
        "Health probe options validated before Redis access: OK",
    );

    const isolatedApp = express();

    isolatedApp.use(
        "/api/health",
        createHealthRouter({
            redis,
            timeoutMs: 50,
        }),
    );

    isolatedApp.use((req, res) => {
        downstreamCalls += 1;

        res.status(429).json({
            message: "Downstream limiter reached",
        });
    });

    const baseUrl = await listen(isolatedApp);

    expectHealth(
        await getHealth(baseUrl),
        200,
        UP,
    );

    expectHealth(
        await getHealth(
            baseUrl,
            "Bearer invalid",
        ),
        200,
        UP,
    );

    assert.equal(downstreamCalls, 0);

    console.log(
        "Public health response and no-store headers: OK",
    );

    redis.isReady = false;

    const beforeDisconnected = pingCount;

    expectHealth(
        await getHealth(baseUrl),
        503,
        DOWN,
    );

    assert.equal(
        pingCount,
        beforeDisconnected,
    );

    redis.isReady = true;

    console.log(
        "Disconnected Redis returns 503 without sending PING: OK",
    );

    for (const failure of [
        "ERROR",
        "BAD_REPLY",
    ]) {
        mode = failure;

        expectHealth(
            await getHealth(baseUrl),
            503,
            DOWN,
        );
    }

    console.log(
        "Redis failures and invalid replies return safe 503: OK",
    );

    mode = "WAIT";
    pending = deferred();

    const beforeTimeout = pingCount;

    expectHealth(
        await getHealth(baseUrl),
        503,
        DOWN,
    );

    assert.equal(lastSignal.aborted, true);

    expectHealth(
        await getHealth(baseUrl),
        503,
        DOWN,
    );

    assert.equal(
        pingCount,
        beforeTimeout + 1,
    );

    console.log(
        "Health timeout prevents additional pending Redis commands: OK",
    );

    pending.reject(
        new Error("PRIVATE_LATE_REDIS_ERROR"),
    );

    await new Promise((resolve) => {
        setImmediate(resolve);
    });

    mode = "UP";

    expectHealth(
        await getHealth(baseUrl),
        200,
        UP,
    );

    assert.equal(
        pingCount,
        beforeTimeout + 2,
    );

    assert.equal(downstreamCalls, 0);

    console.log(
        "Late Redis failure is handled and health recovers: OK",
    );

    // Trong tiến trình thử này, không chạy server.js
    // và không kết nối Redis client thật.
    assert.equal(redisClient.isOpen, false);

    const actualAppUrl = await listen(app);

    const actualHealth = await getHealth(
        actualAppUrl,
        "Bearer invalid",
    );

    expectHealth(actualHealth, 503, DOWN);

    assert.match(
        actualHealth.response.headers.get(
            "x-request-id",
        ) ?? "",
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );

    assert.equal(
        actualHealth.response.headers.get(
            "retry-after",
        ),
        null,
    );

    assert.equal(
        actualHealth.response.headers.get(
            "x-powered-by",
        ),
        null,
    );

    assert.equal(redisClient.isOpen, false);

    console.log(
        "Actual app health bypasses JWT and Redis rate limiter: OK",
    );
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Health check failed:",
        error.code ?? error.name,
    );

    if (error instanceof ReferenceError) {
        console.error(error.stack);
    }
} finally {
    pending?.resolve("PONG");

    const closed = await Promise.allSettled(
        servers.map((server) =>
            new Promise((resolve, reject) => {
                server.close((error) => {
                    if (error) {
                        reject(error);
                        return;
                    }

                    resolve();
                });

                server.closeAllConnections();
            }),
        ),
    );

    for (const client of [
        authClient,
        customerClient,
        productClient,
        orderClient,
        shipmentClient,
    ]) {
        client.close();
    }

    if (redisClient.isOpen) {
        await redisClient.close();
    }

    if (
        closed.some(
            (result) => result.status === "rejected",
        )
    ) {
        process.exitCode = 1;

        console.error(
            "Health test server cleanup failed",
        );
    } else {
        console.log(
            "Health test servers and clients closed: OK",
        );
    }
}