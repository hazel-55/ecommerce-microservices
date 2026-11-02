import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import { fileURLToPath } from "node:url";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import {
    invokeUnary,
} from "../src/infrastructure/grpc/unary.js";

const definition = loadSync(
    fileURLToPath(
        new URL("../../../proto/customer.proto", import.meta.url),
    ),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    },
);

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.customer.v1;

const server = new grpc.Server();

let client;
let bound = false;
let requests = 0;
let receivedMetadata;

function deferred() {
    let resolve;

    const promise = new Promise((done) => {
        resolve = done;
    });

    return { promise, resolve };
}

const started = new Map([
    [3, deferred()],
    [4, deferred()],
]);

const cancelled = new Map([
    [3, deferred()],
    [4, deferred()],
]);

function within(promise, milliseconds = 3000) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            reject(new Error("Test event wait timed out"));
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

server.addService(proto.CustomerService.service, {
    GetCustomer(call, callback) {
        requests += 1;

        const uid = call.request.uid;

        if (uid === 2) {
            callback({
                code: grpc.status.NOT_FOUND,
                details: "Fixture customer missing",
            });

            return;
        }

        if (uid === 3 || uid === 4) {
            call.once("cancelled", () => {
                cancelled.get(uid).resolve();
            });

            started.get(uid).resolve();

            // Giữ RPC mở để kiểm tra deadline hoặc explicit cancel.
            return;
        }

        receivedMetadata = call.metadata.get("x-check");

        callback(null, {
            uid,
            fullname: "Unary fixture",
            score: 100,
        });
    },
});

function invoke(uid, options = {}) {
    return invokeUnary({
        client,
        method: "getCustomer",
        request: { uid },
        metadata: new grpc.Metadata(),
        ...options,
    });
}

async function expectCode(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.equal(error.code, code);
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

    client = new proto.CustomerService(
        `127.0.0.1:${port}`,
        grpc.credentials.createInsecure(),
    );

    await new Promise((resolve, reject) => {
        client.waitForReady(
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

    for (const options of [
        { timeoutMs: 0 },
        { timeoutMs: 60_001 },
        { timeoutMs: 1.5 },
        { deadline: new Date("invalid") },
        { deadline: "invalid" },
        { signal: {} },
    ]) {
        await assert.rejects(
            () => invoke(1, options),
            TypeError,
        );
    }

    await assert.rejects(
        () => invoke(1, { method: "missingMethod" }),
        TypeError,
    );

    await expectCode(
        () => invoke(1, {
            deadline: new Date(Date.now() - 1000),
        }),
        grpc.status.DEADLINE_EXCEEDED,
    );

    const preAborted = new AbortController();
    preAborted.abort();

    await expectCode(
        () => invoke(1, {
            signal: preAborted.signal,
        }),
        grpc.status.CANCELLED,
    );

    assert.equal(requests, 0);

    assert.equal(
        getEventListeners(preAborted.signal, "abort").length,
        0,
    );

    console.log("Unary input, expired deadline and pre-abort validation: OK");

    const metadata = new grpc.Metadata();
    metadata.set("x-check", "unary-fixture");

    const successController = new AbortController();

    const customer = await invoke(1, {
        metadata,
        signal: successController.signal,
    });

    assert.equal(customer.uid, 1);
    assert.equal(customer.fullname, "Unary fixture");
    assert.equal(customer.score, 100);

    assert.deepEqual(
        receivedMetadata,
        ["unary-fixture"],
    );

    assert.equal(
        getEventListeners(successController.signal, "abort").length,
        0,
    );

    console.log("Live unary metadata and response: OK");

    const errorController = new AbortController();

    await assert.rejects(
        () => invoke(2, {
            signal: errorController.signal,
        }),
        (error) => {
            assert.equal(
                error.code,
                grpc.status.NOT_FOUND,
            );

            assert.equal(
                error.details,
                "Fixture customer missing",
            );

            return true;
        },
    );

    assert.equal(
        getEventListeners(errorController.signal, "abort").length,
        0,
    );

    console.log("Live unary service errors preserved: OK");

    const deadlineController = new AbortController();

    const deadlineResult = expectCode(
        () => invoke(3, {
            timeoutMs: 300,
            signal: deadlineController.signal,
        }),
        grpc.status.DEADLINE_EXCEEDED,
    );

    await within(started.get(3).promise);
    await deadlineResult;
    await within(cancelled.get(3).promise);

    assert.equal(
        getEventListeners(deadlineController.signal, "abort").length,
        0,
    );

    console.log("Live gRPC deadline cancels the pending call: OK");

    const controller = new AbortController();

    const cancelledResult = expectCode(
        () => invoke(4, {
            signal: controller.signal,
        }),
        grpc.status.CANCELLED,
    );

    await within(started.get(4).promise);

    controller.abort();

    await cancelledResult;
    await within(cancelled.get(4).promise);

    assert.equal(
        getEventListeners(controller.signal, "abort").length,
        0,
    );

    // Hủy một RPC không đóng cả client channel.
    assert.equal((await invoke(1)).uid, 1);

    console.log("AbortSignal cancels RPC and channel remains reusable: OK");

    let capturedDeadline;

    const recordingClient = {
        getCustomer(request, metadata, options, callback) {
            capturedDeadline = options.deadline;

            queueMicrotask(() => {
                callback(null, { uid: request.uid });
            });

            return {
                cancel() { },
            };
        },
    };

    const parentDeadline = new Date(
        Date.now() + 1000,
    );

    const originalDeadline = parentDeadline.getTime();

    await invokeUnary({
        client: recordingClient,
        method: "getCustomer",
        request: { uid: 1 },
        metadata: new grpc.Metadata(),
        timeoutMs: 10_000,
        deadline: parentDeadline,
    });

    assert.equal(
        capturedDeadline.getTime(),
        originalDeadline,
    );

    assert.equal(
        parentDeadline.getTime(),
        originalDeadline,
    );

    const before = Date.now();

    await invokeUnary({
        client: recordingClient,
        method: "getCustomer",
        request: { uid: 1 },
        metadata: new grpc.Metadata(),
        timeoutMs: 200,
        deadline: new Date(Date.now() + 60_000),
    });

    const after = Date.now();

    assert.ok(
        capturedDeadline.getTime() >= before + 200,
    );

    assert.ok(
        capturedDeadline.getTime() <= after + 200,
    );

    console.log("Earlier deadline and abort listener cleanup: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Unary RPC check failed:",
        error.code ?? error.name,
    );

    process.exitCode = 1;
} finally {
    client?.close();

    try {
        if (bound) {
            await new Promise((resolve, reject) => {
                const timer = setTimeout(() => {
                    server.forceShutdown();
                    reject(new Error("Test server shutdown timed out"));
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

        console.log("Unary test connections closed: OK");
    } catch (error) {
        server.forceShutdown();

        console.error(
            "Unary test shutdown failed:",
            error.code ?? error.name,
        );

        process.exitCode = 1;
    }
}