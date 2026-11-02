import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { DomainError } from "../src/domain/errors.js";
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

const input = {
    items: [{ pid: 7, qty: 2 }],
};

const gates = new Map();

let server;
let rpcClient;

function deferred() {
    let resolve;

    const promise = new Promise((done) => {
        resolve = done;
    });

    return { promise, resolve };
}

function newGate(error = null) {
    const gate = {
        operationId: randomUUID(),
        entered: deferred(),
        release: deferred(),
        abortSeen: deferred(),
        applicationFinished: false,
        error,
    };

    gates.set(gate.operationId, gate);

    return gate;
}

function within(promise, milliseconds = 3000) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            reject(new Error("Drain test timed out"));
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

function metadata(
    operationId,
    token = orderTransportConfig.gatewayToken,
) {
    const value = new grpc.Metadata();

    value.set("x-service-name", "api-gateway");
    value.set("x-service-token", token);
    value.set("x-user-id", "13");
    value.set("x-user-role", "CUSTOMER");
    value.set("x-order-operation-id", operationId);

    return value;
}

function start(gate, token) {
    let call;

    const result = new Promise((resolve, reject) => {
        call = rpcClient.createOrder(
            input,
            metadata(gate.operationId, token),
            { deadline: new Date(Date.now() + 10_000) },
            (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }

                resolve(response);
            },
        );
    });

    // Quan sát lỗi sớm; các assertion vẫn dùng Promise gốc.
    result.catch(() => { });

    return { call, result };
}

async function expectStatus(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    server = createGrpcServer(
        { error() { } },
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
                async waitForOrder(id, request, actor, options) {
                    const gate = gates.get(id);

                    assert.ok(gate);
                    assert.deepEqual(request, input);
                    assert.deepEqual(actor, {
                        uid: 13,
                        role: "CUSTOMER",
                    });

                    function onAbort() {
                        gate.abortSeen.resolve();
                    }

                    options.signal.addEventListener(
                        "abort",
                        onAbort,
                        { once: true },
                    );

                    if (options.signal.aborted) {
                        onAbort();
                    }

                    gate.entered.resolve();

                    try {
                        // Cố ý giữ công việc đang chạy sau khi RPC bị hủy.
                        await gate.release.promise;

                        if (gate.error) {
                            throw gate.error;
                        }

                        if (options.signal.aborted) {
                            throw new DomainError(
                                "SERVICE_UNAVAILABLE",
                                "Fixture creation interrupted",
                            );
                        }

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
                    } finally {
                        options.signal.removeEventListener(
                            "abort",
                            onAbort,
                        );

                        gate.applicationFinished = true;
                    }
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

    await within(server.waitForIdle());

    console.log("Idle server drain completes immediately: OK");

    const normalGate = newGate();
    const normal = start(normalGate);

    await within(normalGate.entered.promise);

    let normalDrained = false;

    const normalDrain = server.waitForIdle().then(() => {
        normalDrained = true;
    });

    await Promise.resolve();

    assert.equal(normalDrained, false);

    normalGate.release.resolve();

    const response = await within(normal.result);
    await within(normalDrain);

    assert.equal(response.oid, 41);
    assert.equal(normalGate.applicationFinished, true);
    assert.equal(normalDrained, true);

    console.log("Drain waits for normal application completion: OK");

    const firstGate = newGate();
    const secondGate = newGate();

    const first = start(firstGate);
    const second = start(secondGate);

    await Promise.all([
        within(firstGate.entered.promise),
        within(secondGate.entered.promise),
    ]);

    const finished = [false, false];

    const drains = finished.map((value, index) =>
        server.waitForIdle().then(() => {
            finished[index] = true;
        }),
    );

    firstGate.release.resolve();

    await within(first.result);

    assert.deepEqual(finished, [false, false]);

    secondGate.release.resolve();

    await within(second.result);
    await within(Promise.all(drains));

    assert.deepEqual(finished, [true, true]);

    console.log("Multiple drain waiters wait for the last active handler: OK");

    for (const [error, status] of [
        [
            new DomainError("CONFLICT", "Fixture conflict"),
            grpc.status.ABORTED,
        ],
        [
            new Error("Private fixture failure"),
            grpc.status.INTERNAL,
        ],
    ]) {
        const gate = newGate(error);
        const flight = start(gate);

        await within(gate.entered.promise);

        const drain = server.waitForIdle();

        gate.release.resolve();

        await expectStatus(() => flight.result, status);
        await within(drain);

        assert.equal(gate.applicationFinished, true);
    }

    const rejected = start(
        newGate(),
        "invalid-gateway-token",
    );

    await expectStatus(
        () => rejected.result,
        grpc.status.UNAUTHENTICATED,
    );

    await within(server.waitForIdle());

    console.log("Business, unexpected and authentication errors release drain tracking: OK");

    const cancelledGate = newGate();
    const cancelled = start(cancelledGate);

    await within(cancelledGate.entered.promise);

    const cancellation = expectStatus(
        () => cancelled.result,
        grpc.status.CANCELLED,
    );

    cancelled.call.cancel();

    await within(cancellation);
    await within(cancelledGate.abortSeen.promise);

    let applicationDrained = false;

    const applicationDrain = server.waitForIdle().then(() => {
        applicationDrained = true;
    });

    await within(new Promise((resolve, reject) => {
        server.tryShutdown((error) => {
            if (error) {
                reject(error);
                return;
            }

            resolve();
        });
    }));

    // Transport đã dừng, nhưng công việc JavaScript vẫn đang giữ.
    assert.equal(applicationDrained, false);
    assert.equal(cancelledGate.applicationFinished, false);

    cancelledGate.release.resolve();

    await within(applicationDrain);

    assert.equal(applicationDrained, true);
    assert.equal(cancelledGate.applicationFinished, true);

    console.log("Cancelled RPC still drains application work after transport shutdown: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Order gRPC drain check failed:",
        error.code ?? error.name,
    );

    process.exitCode = 1;
} finally {
    for (const gate of gates.values()) {
        gate.release.resolve();
    }

    rpcClient?.close();
    server?.forceShutdown();

    customerClient.close();
    productClient.close();
    shipmentClient.close();

    try {
        if (server) {
            await within(server.waitForIdle());
        }

        gates.clear();

        console.log("Order gRPC drain test resources closed: OK");
    } catch (error) {
        console.error(
            "Order gRPC drain cleanup failed:",
            error.code ?? error.name,
        );

        process.exitCode = 1;
    }
}