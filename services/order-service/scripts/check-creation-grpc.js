import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { DomainError } from "../src/domain/errors.js";
import {
    normalizeCreationTaskInput,
    assertSameCreationTask,
} from "../src/domain/order-creation.rules.js";
import {
    orderTransportConfig,
} from "../src/infrastructure/config/env.js";
import {
    createGrpcServer,
} from "../src/interfaces/grpc/server.js";
import {
    customerClient,
} from "../src/infrastructure/grpc/customer.client.js";
import {
    productClient,
} from "../src/infrastructure/grpc/product.client.js";
import {
    shipmentClient,
} from "../src/infrastructure/grpc/shipment.client.js";

const protoPath = fileURLToPath(
    new URL("../../../proto/order.proto", import.meta.url),
);

const definition = loadSync(protoPath, {
    includeDirs: [dirname(protoPath)],
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.order.v1;

const PRIVATE_ERROR = "PRIVATE_CREATION_FIXTURE";
const validInput = { items: [{ pid: 7, qty: 2 }] };

const records = [];
const stored = new Map();
const logs = [];

let mode = "success";
let domainCode;
let sqlCode;
let badResult;
let pendingGate;
let server;
let rpcClient;

function resultFor(actor) {
    return {
        oid: 41,
        uid: actor.uid,
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

function metadata({
    uid = 13,
    role = "CUSTOMER",
    operationId = randomUUID(),
    token = orderTransportConfig.gatewayToken,
} = {}) {
    const value = new grpc.Metadata();

    value.set("x-service-name", "api-gateway");
    value.set("x-service-token", token);
    value.set("x-user-id", String(uid));
    value.set("x-user-role", role);

    if (operationId !== null) {
        value.set("x-order-operation-id", operationId);
    }

    return value;
}

function deferred() {
    let resolve;

    const promise = new Promise((done) => {
        resolve = done;
    });

    return { promise, resolve };
}

function within(promise, milliseconds = 3000) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            reject(new Error("Creation gRPC test timed out"));
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

function start(
    method,
    request,
    callerMetadata = metadata(),
    deadline = new Date(Date.now() + 10_000),
) {
    let call;

    const result = new Promise((resolve, reject) => {
        call = rpcClient[method](
            request,
            callerMetadata,
            { deadline },
            (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }

                resolve(response);
            },
        );
    });

    return { call, result };
}

function invoke(...args) {
    return start(...args).result;
}

async function expectStatus(operation, code, details) {
    await assert.rejects(operation, (error) => {
        assert.equal(error.code, code);

        if (details !== undefined) {
            assert.equal(error.details, details);
        }

        return true;
    });
}

async function main() {
    server = createGrpcServer(
        {
            error(data, message) {
                logs.push({ data, message });
            },
        },
        {
            application: {
                async listOrders() {
                    return { items: [] };
                },

                async getOrder() {
                    throw new DomainError(
                        "NOT_FOUND",
                        "Order not found",
                    );
                },
            },

            creation: {
                async waitForOrder(id, input, actor, options) {
                    const request = normalizeCreationTaskInput({
                        operationId: id,
                        uid: actor.uid,
                        actorRole: actor.role,
                        items: input.items,
                    });

                    records.push({
                        id,
                        input: structuredClone(input),
                        actor: structuredClone(actor),
                        options,
                    });

                    if (mode === "domain") {
                        throw new DomainError(
                            domainCode,
                            domainCode === "INTERNAL"
                                ? PRIVATE_ERROR
                                : "Fixture business error",
                        );
                    }

                    if (mode === "sql") {
                        throw Object.assign(
                            new Error(PRIVATE_ERROR),
                            { code: sqlCode },
                        );
                    }

                    if (mode === "unexpected") {
                        throw new Error(PRIVATE_ERROR);
                    }

                    if (mode === "bad") {
                        return badResult(resultFor(actor));
                    }

                    if (mode === "pending") {
                        const gate = pendingGate;

                        gate.options = options;
                        gate.entered.resolve();

                        await new Promise((resolve) => {
                            if (options.signal.aborted) {
                                resolve();
                                return;
                            }

                            options.signal.addEventListener(
                                "abort",
                                resolve,
                                { once: true },
                            );
                        });

                        gate.finished.resolve();

                        throw new DomainError(
                            "SERVICE_UNAVAILABLE",
                            "Creation interrupted",
                        );
                    }

                    const existing = stored.get(id);

                    if (existing) {
                        assertSameCreationTask(existing, request);
                        return structuredClone(existing.response);
                    }

                    const response = resultFor(actor);

                    stored.set(id, {
                        uid: request.uid,
                        actor_role: request.actorRole,
                        items: request.items,
                        response: structuredClone(response),
                    });

                    return response;
                },
            },
        },
    );

    const port = await new Promise((resolve, reject) => {
        server.bindAsync(
            "127.0.0.1:0",
            grpc.ServerCredentials.createInsecure(),
            (error, assignedPort) => {
                if (error) {
                    reject(error);
                    return;
                }

                resolve(assignedPort);
            },
        );
    });

    rpcClient = new proto.OrderService(
        `127.0.0.1:${port}`,
        grpc.credentials.createInsecure(),
    );

    await new Promise((resolve, reject) => {
        rpcClient.waitForReady(
            new Date(Date.now() + 3000),
            (error) => {
                if (error) {
                    reject(error);
                    return;
                }

                resolve();
            },
        );
    });

    await expectStatus(
        () => invoke(
            "createOrder",
            validInput,
            new grpc.Metadata(),
        ),
        grpc.status.UNAUTHENTICATED,
    );

    await expectStatus(
        () => invoke(
            "createOrder",
            validInput,
            metadata({
                token: "invalid-token",
                operationId: null,
            }),
        ),
        grpc.status.UNAUTHENTICATED,
    );

    for (const operationId of [null, "invalid"]) {
        await expectStatus(
            () => invoke(
                "createOrder",
                validInput,
                metadata({ operationId }),
            ),
            grpc.status.INVALID_ARGUMENT,
        );
    }

    const duplicate = metadata();
    duplicate.add("x-order-operation-id", randomUUID());

    await expectStatus(
        () => invoke("createOrder", validInput, duplicate),
        grpc.status.INVALID_ARGUMENT,
    );

    assert.equal(records.length, 0);

    console.log("CreateOrder caller and operation metadata validation: OK");

    for (const input of [
        { items: [] },
        { items: [{ pid: 0, qty: 1 }] },
        { items: [{ pid: 7, qty: 0 }] },
        {
            items: [
                { pid: 7, qty: 1 },
                { pid: 7, qty: 2 },
            ],
        },
    ]) {
        await expectStatus(
            () => invoke("createOrder", input),
            grpc.status.INVALID_ARGUMENT,
        );
    }

    assert.equal(records.length, 0);

    const operationId = randomUUID();

    const response = await invoke(
        "createOrder",
        {
            ...validInput,
            uid: 999,
            role: "ADMIN",
        },
        metadata({ operationId }),
    );

    assert.deepEqual(response, {
        oid: 41,
        uid: 13,
        discount: "2.55",
        items: [{
            pid: 7,
            qty: 2,
            unitPrice: "25.50",
        }],
        shipment: {
            shipid: 61,
            oid: 41,
            status: "PENDING",
        },
    });

    assert.deepEqual(records[0].actor, {
        uid: 13,
        role: "CUSTOMER",
    });

    assert.deepEqual(records[0].input, validInput);
    assert.ok(records[0].options.deadline instanceof Date);
    assert.ok(records[0].options.signal instanceof AbortSignal);
    assert.equal(records[0].options.timeoutMs, 5000);

    console.log("CreateOrder input, actor and decimal Shipment response: OK");

    assert.deepEqual(
        await invoke(
            "createOrder",
            validInput,
            metadata({ operationId }),
        ),
        response,
    );

    assert.equal(stored.size, 1);

    await expectStatus(
        () => invoke(
            "createOrder",
            { items: [{ pid: 7, qty: 3 }] },
            metadata({ operationId }),
        ),
        grpc.status.ABORTED,
    );

    await expectStatus(
        () => invoke(
            "createOrder",
            validInput,
            metadata({ operationId, uid: 14 }),
        ),
        grpc.status.PERMISSION_DENIED,
    );

    await expectStatus(
        () => invoke(
            "createOrder",
            validInput,
            metadata({ operationId, role: "ADMIN" }),
        ),
        grpc.status.ABORTED,
    );

    assert.equal(stored.size, 1);

    console.log("CreateOrder replay and operation binding: OK");

    mode = "domain";

    const mappings = {
        INVALID_ARGUMENT: grpc.status.INVALID_ARGUMENT,
        UNAUTHENTICATED: grpc.status.UNAUTHENTICATED,
        FORBIDDEN: grpc.status.PERMISSION_DENIED,
        NOT_FOUND: grpc.status.NOT_FOUND,
        CONFLICT: grpc.status.ABORTED,
        SERVICE_UNAVAILABLE: grpc.status.UNAVAILABLE,
        INTERNAL: grpc.status.INTERNAL,
    };

    for (const [code, status] of Object.entries(mappings)) {
        domainCode = code;

        await expectStatus(
            () => invoke("createOrder", validInput),
            status,
            code === "INTERNAL"
                ? "Order operation failed"
                : undefined,
        );
    }

    mode = "sql";

    for (const code of ["55P03", "57014"]) {
        sqlCode = code;

        await expectStatus(
            () => invoke("createOrder", validInput),
            grpc.status.UNAVAILABLE,
            "Order database temporarily unavailable",
        );
    }

    mode = "unexpected";

    await expectStatus(
        () => invoke("createOrder", validInput),
        grpc.status.INTERNAL,
        "Order operation failed",
    );

    assert.ok(logs.length > 0);
    assert.equal(JSON.stringify(logs).includes(PRIVATE_ERROR), false);

    assert.equal(
        JSON.stringify(logs).includes(
            orderTransportConfig.gatewayToken,
        ),
        false,
    );

    console.log("CreateOrder domain and SQL errors mapped safely: OK");

    mode = "bad";

    const invalidResults = [
        () => null,
        (order) => { order.uid = 14; return order; },
        (order) => { order.discount = "-1.00"; return order; },
        (order) => { order.items[0].pid = 8; return order; },
        (order) => { order.items[0].qty = 3; return order; },
        (order) => { order.items[0].unit_price = "invalid"; return order; },
        (order) => { order.shipment.oid = 99; return order; },
        (order) => { order.shipment.shipid = 0; return order; },
        (order) => { order.shipment.status = "UNKNOWN"; return order; },
    ];

    for (const mutate of invalidResults) {
        badResult = mutate;

        await expectStatus(
            () => invoke("createOrder", validInput),
            grpc.status.INTERNAL,
            "Order operation failed",
        );
    }

    console.log("Malformed creation responses rejected: OK");

    mode = "pending";

    pendingGate = {
        entered: deferred(),
        finished: deferred(),
        options: null,
    };

    const deadline = new Date(Date.now() + 300);

    const timed = start(
        "createOrder",
        validInput,
        metadata(),
        deadline,
    );

    const timedResult = expectStatus(
        () => timed.result,
        grpc.status.DEADLINE_EXCEEDED,
    );

    await within(pendingGate.entered.promise);

    assert.ok(
        pendingGate.options.deadline.getTime()
        <= deadline.getTime() + 50,
    );

    await within(timedResult);
    await within(pendingGate.finished.promise);

    assert.equal(pendingGate.options.signal.aborted, true);

    pendingGate = {
        entered: deferred(),
        finished: deferred(),
        options: null,
    };

    const cancelled = start(
        "createOrder",
        validInput,
    );

    const cancelledResult = expectStatus(
        () => cancelled.result,
        grpc.status.CANCELLED,
    );

    await within(pendingGate.entered.promise);

    cancelled.call.cancel();

    await within(cancelledResult);
    await within(pendingGate.finished.promise);

    assert.equal(pendingGate.options.signal.aborted, true);

    assert.deepEqual(
        await invoke("listOrders", {}),
        { items: [] },
    );

    console.log("Live CreateOrder deadline and cancellation propagation: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Creation gRPC check failed:",
        error.code ?? error.name,
    );

    process.exitCode = 1;
} finally {
    rpcClient?.close();

    customerClient.close();
    productClient.close();
    shipmentClient.close();

    try {
        if (server) {
            await new Promise((resolve, reject) => {
                const timer = setTimeout(() => {
                    server.forceShutdown();
                    reject(new Error("Test shutdown timed out"));
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
        }

        stored.clear();

        console.log("CreateOrder gRPC test resources closed: OK");
    } catch (error) {
        server?.forceShutdown();

        console.error(
            "Creation gRPC cleanup failed:",
            error.code ?? error.name,
        );

        process.exitCode = 1;
    }
}