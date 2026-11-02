import assert from "node:assert/strict";
import http from "node:http";
import { randomUUID } from "node:crypto";
import { getEventListeners } from "node:events";
import express from "express";
import jwt from "jsonwebtoken";

import { gatewayConfig } from "../src/config/env.js";
import { HttpError } from "../src/errors/http-error.js";
import { orderRouter } from "../src/routes/order.routes.js";
import {
    orderClient,
} from "../src/infrastructure/grpc/order.client.js";

const originals = {
    createOrder: orderClient.createOrder,
    listOrders: orderClient.listOrders,
    getOrder: orderClient.getOrder,
};

const calls = [];
const signals = [];

let server;
let baseUrl;
let mode = "SUCCESS";
let failureStatus = 503;
let entered;
let aborted;
let settled;
let releasePending;
let activeRequest;

function deferred() {
    let resolve;

    const promise = new Promise((done) => {
        resolve = done;
    });

    return { promise, resolve };
}

async function within(promise, milliseconds = 3000) {
    let timer;

    try {
        return await Promise.race([
            promise,
            new Promise((resolve, reject) => {
                timer = setTimeout(() => {
                    reject(new Error("Test wait timed out"));
                }, milliseconds);
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}

function token(expiresIn = 60) {
    return jwt.sign(
        { role: "CUSTOMER" },
        gatewayConfig.jwtSecret,
        {
            algorithm: "HS256",
            subject: "13",
            issuer: gatewayConfig.jwtIssuer,
            audience: gatewayConfig.jwtAudience,
            expiresIn,
        },
    );
}

const validToken = token();

const body = {
    items: [{ pid: 7, qty: 2 }],
};

function storedOrder() {
    return {
        oid: 41,
        uid: 13,
        discount: "2.55",
        items: [
            {
                pid: 7,
                qty: 2,
                unit_price: "25.50",
            },
        ],
    };
}

const expectedDetail = {
    oid: 41,
    uid: 13,
    discount: 2.55,
    items: [
        {
            pid: 7,
            qty: 2,
            unit_price: 25.5,
        },
    ],
};

orderClient.createOrder = async (
    operationId,
    input,
    actor,
    { signal } = {},
) => {
    calls.push({
        operationId,
        input: structuredClone(input),
        actor: structuredClone(actor),
    });

    assert.ok(signal instanceof AbortSignal);
    signals.push(signal);

    if (mode === "FAIL") {
        throw new HttpError(
            failureStatus,
            "Order fixture failure",
        );
    }

    if (mode === "WAIT") {
        let onAbort;

        try {
            await new Promise((resolve) => {
                releasePending = resolve;

                onAbort = () => {
                    aborted.resolve();
                    resolve();
                };

                signal.addEventListener(
                    "abort",
                    onAbort,
                    { once: true },
                );

                entered.resolve();

                if (signal.aborted) {
                    onAbort();
                }
            });

            throw new HttpError(
                503,
                "Order service unavailable",
            );
        } finally {
            signal.removeEventListener(
                "abort",
                onAbort,
            );

            releasePending = undefined;
            settled.resolve();
        }
    }

    return {
        ...storedOrder(),
        shipment: {
            shipid: 61,
            oid: 41,
            status: "PENDING",
        },
    };
};

orderClient.listOrders = async (actor) => {
    assert.deepEqual(actor, {
        uid: 13,
        role: "CUSTOMER",
    });

    return {
        items: [
            {
                oid: 41,
                uid: 13,
                discount: "2.55",
            },
        ],
    };
};

orderClient.getOrder = async (oid, actor) => {
    assert.equal(oid, 41);

    assert.deepEqual(actor, {
        uid: 13,
        role: "CUSTOMER",
    });

    return storedOrder();
};

async function post({
    authorization = `Bearer ${validToken}`,
    key,
    input = body,
} = {}) {
    const headers = {
        "Content-Type": "application/json",
    };

    if (authorization !== null) {
        headers.Authorization = authorization;
    }

    if (key !== undefined) {
        headers["Idempotency-Key"] = key;
    }

    const response = await fetch(
        `${baseUrl}/api/orders`,
        {
            method: "POST",
            headers,
            body: JSON.stringify(input),
            signal: AbortSignal.timeout(5000),
        },
    );

    return {
        status: response.status,
        key: response.headers.get("idempotency-key"),
        body: await response.json(),
    };
}

async function duplicateHeaderRequest() {
    return new Promise((resolve, reject) => {
        const request = http.request(
            `${baseUrl}/api/orders`,
            {
                method: "POST",
                headers: [
                    "Authorization",
                    `Bearer ${validToken}`,
                    "Content-Type",
                    "application/json",
                    "Idempotency-Key",
                    randomUUID(),
                    "Idempotency-Key",
                    randomUUID(),
                ],
            },
            (response) => {
                response.resume();

                response.once("end", () => {
                    resolve(response.statusCode);
                });

                response.once("error", reject);
            },
        );

        request.setTimeout(3000, () => {
            request.destroy(new Error("HTTP test timed out"));
        });

        request.once("error", reject);
        request.end(JSON.stringify(body));
    });
}

async function main() {
    const app = express();

    app.use(express.json({ limit: "16kb" }));
    app.use("/api/orders", orderRouter);

    app.use((error, req, res, next) => {
        if (res.headersSent) {
            return next(error);
        }

        if (res.destroyed) {
            return;
        }

        res.status(
            error instanceof HttpError
                ? error.status
                : 500,
        ).json({
            message:
                error instanceof HttpError
                    ? error.message
                    : "Internal server error",
        });
    });

    server = app.listen(0, "127.0.0.1");

    await new Promise((resolve, reject) => {
        server.once("listening", resolve);
        server.once("error", reject);
    });

    baseUrl = `http://127.0.0.1:${server.address().port}`;

    for (const authorization of [
        null,
        "Bearer invalid",
        `Bearer ${token(-10)}`,
    ]) {
        const result = await post({ authorization });
        assert.equal(result.status, 401);
    }

    assert.equal(calls.length, 0);

    console.log("HTTP CreateOrder JWT protection: OK");

    const input = {
        ...body,
        uid: 999,
        role: "ADMIN",
        discount: 0,
    };

    const created = await post({ input });

    assert.equal(created.status, 201);

    assert.deepEqual(created.body, {
        ...expectedDetail,
        shipment: {
            shipid: 61,
            oid: 41,
            status: "PENDING",
        },
    });

    assert.deepEqual(calls.at(-1).actor, {
        uid: 13,
        role: "CUSTOMER",
    });

    assert.deepEqual(calls.at(-1).input, input);
    assert.equal(signals.at(-1).aborted, false);

    console.log(
        "HTTP CreateOrder actor and numeric response: OK",
    );

    assert.match(
        created.key,
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );

    assert.equal(
        created.key,
        calls.at(-1).operationId,
    );

    console.log(
        "Generated operation identity returned to HTTP client: OK",
    );

    const key = randomUUID();
    const beforeReplay = calls.length;

    for (let attempt = 0; attempt < 2; attempt += 1) {
        const result = await post({ key });

        assert.equal(result.status, 201);
        assert.equal(result.key, key);
    }

    assert.deepEqual(
        calls.slice(beforeReplay).map(
            (call) => call.operationId,
        ),
        [key, key],
    );

    console.log(
        "Supplied operation identity preserved across HTTP requests: OK",
    );

    const beforeInvalid = calls.length;

    for (const invalidKey of [
        "",
        "invalid",
        key.toUpperCase(),
        `${key},${randomUUID()}`,
    ]) {
        const result = await post({
            key: invalidKey,
        });

        assert.equal(result.status, 400);
    }

    assert.equal(
        await duplicateHeaderRequest(),
        400,
    );

    assert.equal(calls.length, beforeInvalid);

    console.log(
        "Invalid and duplicate operation headers rejected: OK",
    );

    mode = "FAIL";

    for (const status of [
        400,
        403,
        404,
        409,
        502,
        503,
        500,
    ]) {
        failureStatus = status;

        const result = await post({ key });

        assert.equal(result.status, status);
        assert.equal(result.key, key);

        assert.deepEqual(result.body, {
            message: "Order fixture failure",
        });
    }

    console.log(
        "Client HTTP errors preserve operation identity: OK",
    );

    mode = "WAIT";
    entered = deferred();
    aborted = deferred();
    settled = deferred();

    const requestClosed = deferred();

    activeRequest = http.request(
        `${baseUrl}/api/orders`,
        {
            method: "POST",
            headers: {
                Authorization: `Bearer ${validToken}`,
                "Content-Type": "application/json",
                "Idempotency-Key": randomUUID(),
            },
        },
    );

    activeRequest.on("error", () => { });
    activeRequest.once("close", requestClosed.resolve);

    activeRequest.setTimeout(3000, () => {
        activeRequest.destroy();
    });

    activeRequest.end(JSON.stringify(body));

    await within(entered.promise);

    const cancellationSignal = signals.at(-1);

    activeRequest.destroy();

    await within(aborted.promise);
    await within(settled.promise);
    await within(requestClosed.promise);

    assert.equal(cancellationSignal.aborted, true);

    assert.equal(
        getEventListeners(
            cancellationSignal,
            "abort",
        ).length,
        0,
    );

    console.log(
        "HTTP disconnect aborts active Order request: OK",
    );

    mode = "SUCCESS";

    const listResponse = await fetch(
        `${baseUrl}/api/orders`,
        {
            headers: {
                Authorization: `Bearer ${validToken}`,
            },
            signal: AbortSignal.timeout(5000),
        },
    );

    assert.equal(listResponse.status, 200);

    assert.deepEqual(await listResponse.json(), {
        items: [
            {
                oid: 41,
                uid: 13,
                discount: 2.55,
            },
        ],
    });

    const detailResponse = await fetch(
        `${baseUrl}/api/orders/41`,
        {
            headers: {
                Authorization: `Bearer ${validToken}`,
            },
            signal: AbortSignal.timeout(5000),
        },
    );

    assert.equal(detailResponse.status, 200);

    assert.deepEqual(
        await detailResponse.json(),
        expectedDetail,
    );

    console.log("Existing HTTP Order reads preserved: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Order create HTTP check failed:",
        error.status ?? error.code ?? error.name,
    );

    if (error instanceof ReferenceError) {
        console.error(error.stack);
    }

    process.exitCode = 1;
} finally {
    activeRequest?.destroy();
    releasePending?.();

    Object.assign(orderClient, originals);
    orderClient.close();

    try {
        if (server) {
            await new Promise((resolve, reject) => {
                server.close((error) => {
                    if (error) {
                        reject(error);
                        return;
                    }

                    resolve();
                });

                server.closeAllConnections();
            });
        }

        console.log(
            "HTTP test server and client resources closed: OK",
        );
    } catch (error) {
        console.error(
            "HTTP test cleanup failed:",
            error.code ?? error.name,
        );

        process.exitCode = 1;
    }
}