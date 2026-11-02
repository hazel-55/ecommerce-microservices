import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { getEventListeners } from "node:events";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import jwt from "jsonwebtoken";
import { gatewayConfig } from "../src/config/env.js";
import { HttpError } from "../src/errors/http-error.js";
import {
    authenticate,
} from "../src/middleware/auth.middleware.js";
import {
    createOrderClient,
    orderClient,
} from "../src/infrastructure/grpc/order.client.js";

const protoPath = fileURLToPath(
    new URL("../../../proto/order.proto", import.meta.url),
);

const definition = loadSync(protoPath, {
    includeDirs: [dirname(protoPath)],
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.order.v1;

const FIXTURE_TOKEN = "gateway-order-fixture-".padEnd(40, "x");
const PRIVATE_ERROR = "PRIVATE_ORDER_RPC_FIXTURE";

const validInput = {
    items: [{ pid: 7, qty: 2 }],
};

const records = [];
const clients = [];

const server = new grpc.Server();

let bound = false;
let mode = "success";
let remoteCode;
let mutateResponse;
let pendingGate;

function response() {
    return {
        oid: 41,
        uid: 13,
        discount: "2.55",
        items: [{
            pid: 7,
            qty: 2,
            unit_price: "25.50",
        }],
        shipment: {
            shipid: 61,
            oid: 41,
            status: "PENDING",
        },
    };
}

function deferred() {
    let resolve;

    const promise = new Promise((done) => {
        resolve = done;
    });

    return { promise, resolve };
}

function newGate() {
    return {
        entered: deferred(),
        cancelled: deferred(),
        deadline: null,
    };
}

function within(promise, milliseconds = 3000) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            reject(new Error("Client test timed out"));
        }, milliseconds);

        promise.then(
            (value) => {
                clearTimeout(timer);
                resolve(value);
            },
            (error) => {
                clearTimeout(timer);
                reject(error);
            },
        );
    });
}

server.addService(proto.OrderService.service, {
    CreateOrder(call, callback) {
        try {
            assert.deepEqual(
                call.metadata.get("x-service-name"),
                ["api-gateway"],
            );

            assert.deepEqual(
                call.metadata.get("x-service-token"),
                [FIXTURE_TOKEN],
            );

            assert.deepEqual(
                call.metadata.get("x-user-id"),
                ["13"],
            );

            assert.deepEqual(
                call.metadata.get("x-user-role"),
                ["CUSTOMER"],
            );

            const ids = call.metadata.get(
                "x-order-operation-id",
            );

            assert.equal(ids.length, 1);
            assert.equal(typeof ids[0], "string");

            records.push({
                operationId: ids[0],
                request: structuredClone(call.request),
            });
        } catch {
            callback({
                code: grpc.status.UNAUTHENTICATED,
                details: PRIVATE_ERROR,
            });

            return;
        }

        if (mode === "error") {
            callback({
                code: remoteCode,
                details: PRIVATE_ERROR,
            });

            return;
        }

        if (mode === "pending") {
            const gate = pendingGate;

            gate.deadline = Number(call.getDeadline());

            call.once("cancelled", () => {
                gate.cancelled.resolve();
            });

            gate.entered.resolve();
            return;
        }

        const value = response();

        callback(
            null,
            mode === "malformed"
                ? mutateResponse(value)
                : value,
        );
    },

    ListOrders(call, callback) {
        callback(null, {
            items: [{
                oid: 41,
                uid: 13,
                discount: "2.55",
            }],
        });
    },

    GetOrder(call, callback) {
        callback(null, {
            oid: call.request.oid,
            uid: 13,
            discount: "2.55",
            items: response().items,
        });
    },
});

async function expectHttp(operation, status) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof HttpError);
        assert.equal(error.status, status);

        assert.equal(
            error.message.includes(PRIVATE_ERROR),
            false,
        );

        return true;
    });
}

async function main() {
    const port = await new Promise((resolve, reject) => {
        server.bindAsync(
            "127.0.0.1:0",
            grpc.ServerCredentials.createInsecure(),
            (error, assignedPort) => {
                if (error) {
                    reject(error);
                    return;
                }

                bound = true;
                resolve(assignedPort);
            },
        );
    });

    const address = `127.0.0.1:${port}`;

    const client = createOrderClient({
        address,
        serviceToken: FIXTURE_TOKEN,
    });

    clients.push(client);

    const token = jwt.sign(
        { role: "CUSTOMER" },
        gatewayConfig.jwtSecret,
        {
            algorithm: "HS256",
            subject: "13",
            issuer: gatewayConfig.jwtIssuer,
            audience: gatewayConfig.jwtAudience,
            expiresIn: 60,
        },
    );

    const req = {
        headers: {
            authorization: `Bearer ${token}`,
        },
    };

    let authError;

    authenticate(req, {}, (error) => {
        authError = error;
    });

    assert.equal(authError, undefined);

    const actor = req.auth;

    assert.deepEqual(actor, {
        uid: 13,
        role: "CUSTOMER",
    });

    for (const id of [null, "invalid"]) {
        await expectHttp(
            () => client.createOrder(
                id,
                validInput,
                actor,
            ),
            400,
        );
    }

    for (const body of [
        null,
        [],
        {},
        { items: [] },
        { items: [{ pid: 0, qty: 1 }] },
        { items: [{ pid: 7, qty: 0 }] },
        { items: [{ pid: "7", qty: 2 }] },
        { items: [{ pid: 7, qty: 2147483648 }] },
        {
            items: [
                { pid: 7, qty: 1 },
                { pid: 7, qty: 2 },
            ],
        },
    ]) {
        await expectHttp(
            () => client.createOrder(
                randomUUID(),
                body,
                actor,
            ),
            400,
        );
    }

    for (const invalidActor of [
        null,
        { uid: 0, role: "CUSTOMER" },
        { uid: 13, role: "ROOT" },
    ]) {
        await expectHttp(
            () => client.createOrder(
                randomUUID(),
                validInput,
                invalidActor,
            ),
            401,
        );
    }

    assert.equal(records.length, 0);

    console.log(
        "Verified JWT actor and CreateOrder input validation: OK",
    );

    const operationId = randomUUID();

    const body = {
        items: [{
            pid: 7,
            qty: 2,
            unit_price: "0.01",
        }],
        uid: 999,
        role: "ADMIN",
        score: 999,
        discount: "0.00",
    };

    const unchanged = structuredClone(body);

    assert.deepEqual(
        await client.createOrder(
            operationId,
            body,
            actor,
        ),
        response(),
    );

    assert.deepEqual(records[0], {
        operationId,
        request: validInput,
    });

    assert.deepEqual(body, unchanged);

    console.log(
        "Gateway CreateOrder metadata and response fields: OK",
    );

    mode = "error";

    const mappings = [
        [grpc.status.INVALID_ARGUMENT, 400],
        [grpc.status.UNAUTHENTICATED, 502],
        [grpc.status.PERMISSION_DENIED, 403],
        [grpc.status.NOT_FOUND, 404],
        [grpc.status.ABORTED, 409],
        [grpc.status.UNAVAILABLE, 503],
        [grpc.status.DEADLINE_EXCEEDED, 503],
        [grpc.status.CANCELLED, 503],
        [grpc.status.INTERNAL, 500],
        [grpc.status.UNKNOWN, 500],
    ];

    for (const [code, status] of mappings) {
        remoteCode = code;

        await expectHttp(
            () => client.createOrder(
                randomUUID(),
                validInput,
                actor,
            ),
            status,
        );
    }

    console.log("CreateOrder RPC errors mapped safely: OK");

    mode = "malformed";

    const mutations = [
        (value) => {
            value.uid = 14;
            return value;
        },
        (value) => {
            value.discount = "invalid";
            return value;
        },
        (value) => {
            value.items = [];
            return value;
        },
        (value) => {
            value.items[0].pid = 8;
            return value;
        },
        (value) => {
            value.items[0].qty = 3;
            return value;
        },
        (value) => {
            value.items[0].unit_price = "invalid";
            return value;
        },
        (value) => {
            value.shipment = null;
            return value;
        },
        (value) => {
            value.shipment.oid = 99;
            return value;
        },
        (value) => {
            value.shipment.shipid = 0;
            return value;
        },
        (value) => {
            value.shipment.status = "UNKNOWN";
            return value;
        },
    ];

    for (const mutation of mutations) {
        mutateResponse = mutation;

        await expectHttp(
            () => client.createOrder(
                randomUUID(),
                validInput,
                actor,
            ),
            502,
        );
    }

    console.log("Invalid CreateOrder responses rejected: OK");

    const retryId = randomUUID();
    const beforeRetry = records.length;

    mode = "error";
    remoteCode = grpc.status.UNAVAILABLE;

    await expectHttp(
        () => client.createOrder(
            retryId,
            validInput,
            actor,
        ),
        503,
    );

    mode = "success";

    await client.createOrder(
        retryId,
        validInput,
        actor,
    );

    assert.deepEqual(
        records.slice(beforeRetry).map(
            (record) => record.operationId,
        ),
        [retryId, retryId],
    );

    console.log(
        "Create operation identity preserved across retries: OK",
    );

    const shortClient = createOrderClient({
        address,
        serviceToken: FIXTURE_TOKEN,
        creationTimeoutMs: 300,
    });

    clients.push(shortClient);

    await shortClient.listOrders(actor);

    mode = "pending";
    pendingGate = newGate();

    const startedAt = Date.now();

    const timed = expectHttp(
        () => shortClient.createOrder(
            randomUUID(),
            validInput,
            actor,
        ),
        503,
    );

    await within(pendingGate.entered.promise);

    assert.ok(
        pendingGate.deadline <= startedAt + 350,
    );

    await within(timed);
    await within(pendingGate.cancelled.promise);

    pendingGate = newGate();

    const controller = new AbortController();

    const cancelled = expectHttp(
        () => client.createOrder(
            randomUUID(),
            validInput,
            actor,
            { signal: controller.signal },
        ),
        503,
    );

    await within(pendingGate.entered.promise);

    controller.abort();

    await within(cancelled, 1000);
    await within(pendingGate.cancelled.promise, 1000);

    assert.equal(
        getEventListeners(controller.signal, "abort").length,
        0,
    );

    console.log(
        "CreateOrder deadline and cancellation forwarded: OK",
    );

    mode = "success";

    assert.deepEqual(
        await client.listOrders(actor),
        {
            items: [{
                oid: 41,
                uid: 13,
                discount: "2.55",
            }],
        },
    );

    assert.deepEqual(
        await client.getOrder(41, actor),
        {
            oid: 41,
            uid: 13,
            discount: "2.55",
            items: response().items,
        },
    );

    console.log("Existing Order reads remain available: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Order create client check failed:",
        error.status ?? error.code ?? error.name,
    );

    if (error instanceof ReferenceError) {
        console.error(error.stack);
    }

    process.exitCode = 1;
} finally {
    for (const client of clients) {
        client.close();
    }

    orderClient.close();

    try {
        if (bound) {
            await new Promise((resolve, reject) => {
                const timer = setTimeout(() => {
                    server.forceShutdown();

                    reject(new Error(
                        "Test shutdown timed out",
                    ));
                }, 5000);

                server.tryShutdown((error) => {
                    clearTimeout(timer);

                    if (error) {
                        reject(error);
                        return;
                    }

                    resolve();
                });
            });
        } else {
            server.forceShutdown();
        }

        console.log(
            "Order create client test connections closed: OK",
        );
    } catch (error) {
        server.forceShutdown();

        console.error(
            "Order create client cleanup failed:",
            error.code ?? error.name,
        );

        process.exitCode = 1;
    }
}