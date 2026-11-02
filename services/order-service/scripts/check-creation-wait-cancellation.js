import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { getEventListeners } from "node:events";
import { DomainError } from "../src/domain/errors.js";
import {
    createOrderCreationWaiter,
} from "../src/application/order-creation.wait.js";
import {
    customerClient,
} from "../src/infrastructure/grpc/customer.client.js";
import {
    productClient,
} from "../src/infrastructure/grpc/product.client.js";
import {
    shipmentClient,
} from "../src/infrastructure/grpc/shipment.client.js";

const operationId = randomUUID();
const actor = { uid: 13, role: "CUSTOMER" };
const input = { items: [{ pid: 7, qty: 2 }] };

const completed = {
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

function makeWaiter(advanceOrder) {
    return createOrderCreationWaiter({
        progress: { advanceOrder },
    });
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
            reject(new Error("Waiter test timed out"));
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

async function expectCode(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    const calls = [];

    const sequential = makeWaiter(
        async (id, body, caller, options) => {
            assert.equal(id, operationId);
            assert.deepEqual(body, input);
            assert.deepEqual(caller, actor);

            calls.push(options);

            return calls.length === 2
                ? completed
                : null;
        },
    );

    assert.deepEqual(
        await sequential.waitForOrder(
            operationId,
            input,
            actor,
            { intervalMs: 1 },
        ),
        completed,
    );

    assert.equal(calls.length, 2);
    assert.equal(calls[0].deadline, calls[1].deadline);
    assert.equal(calls[0].signal, calls[1].signal);
    assert.ok(calls[0].deadline instanceof Date);
    assert.ok(calls[0].signal instanceof AbortSignal);

    console.log("Waiter shares one deadline and signal across attempts: OK");

    let captured;
    let unexpectedCalls = 0;

    const parentDeadline = new Date(Date.now() + 1000);
    const originalDeadline = parentDeadline.getTime();

    const parentBound = makeWaiter(
        async (id, body, caller, options) => {
            captured = options;
            return completed;
        },
    );

    await parentBound.waitForOrder(
        operationId,
        input,
        actor,
        {
            timeoutMs: 5000,
            deadline: parentDeadline,
        },
    );

    assert.equal(
        captured.deadline.getTime(),
        originalDeadline,
    );

    assert.equal(parentDeadline.getTime(), originalDeadline);

    const unused = makeWaiter(async () => {
        unexpectedCalls += 1;
        return completed;
    });

    const preAborted = new AbortController();
    preAborted.abort();

    for (const options of [
        { signal: preAborted.signal },
        { deadline: new Date(Date.now() - 1000) },
    ]) {
        await expectCode(
            () => unused.waitForOrder(
                operationId,
                input,
                actor,
                options,
            ),
            "SERVICE_UNAVAILABLE",
        );
    }

    for (const options of [
        { signal: {} },
        { deadline: new Date("invalid") },
    ]) {
        await expectCode(
            () => unused.waitForOrder(
                operationId,
                input,
                actor,
                options,
            ),
            "INVALID_ARGUMENT",
        );
    }

    assert.equal(unexpectedCalls, 0);

    console.log("Parent deadline and ended request validation: OK");

    let phaseFinished = false;
    let expiryCalls = 0;

    const expiry = makeWaiter(
        async (id, body, caller, options) => {
            expiryCalls += 1;

            await new Promise((resolve) => {
                options.signal.addEventListener(
                    "abort",
                    resolve,
                    { once: true },
                );
            });

            phaseFinished = true;
            return null;
        },
    );

    await within(expectCode(
        () => expiry.waitForOrder(
            operationId,
            input,
            actor,
            { timeoutMs: 50 },
        ),
        "SERVICE_UNAVAILABLE",
    ));

    assert.equal(expiryCalls, 1);
    assert.equal(phaseFinished, true);

    console.log("Budget expiry aborts active work and waits for settlement: OK");

    const idleController = new AbortController();
    let idleCalls = 0;

    const idle = makeWaiter(async () => {
        idleCalls += 1;
        return null;
    });

    const idleAbortTimer = setTimeout(
        () => idleController.abort(),
        30,
    );

    try {
        await within(expectCode(
            () => idle.waitForOrder(
                operationId,
                input,
                actor,
                {
                    timeoutMs: 2000,
                    intervalMs: 1000,
                    signal: idleController.signal,
                },
            ),
            "SERVICE_UNAVAILABLE",
        ), 500);
    } finally {
        clearTimeout(idleAbortTimer);
    }

    assert.equal(idleCalls, 1);

    assert.equal(
        getEventListeners(idleController.signal, "abort").length,
        0,
    );

    console.log("External abort interrupts idle wait without another attempt: OK");

    const activeController = new AbortController();
    const entered = deferred();
    const abortSeen = deferred();
    const release = deferred();

    let activeFinished = false;
    let waiterSettled = false;

    const active = makeWaiter(
        async (id, body, caller, options) => {
            entered.resolve();

            await new Promise((resolve) => {
                if (options.signal.aborted) {
                    abortSeen.resolve();
                    resolve();
                    return;
                }

                options.signal.addEventListener(
                    "abort",
                    () => {
                        abortSeen.resolve();
                        resolve();
                    },
                    { once: true },
                );
            });

            await release.promise;

            activeFinished = true;
            return null;
        },
    );

    const activeResult = expectCode(
        () => active.waitForOrder(
            operationId,
            input,
            actor,
            {
                timeoutMs: 2000,
                signal: activeController.signal,
            },
        ),
        "SERVICE_UNAVAILABLE",
    );

    activeResult.then(
        () => { waiterSettled = true; },
        () => { waiterSettled = true; },
    );

    try {
        await within(entered.promise);

        activeController.abort();

        await within(abortSeen.promise);

        assert.equal(activeFinished, false);
        assert.equal(waiterSettled, false);

        release.resolve();

        await within(activeResult);

        assert.equal(activeFinished, true);
    } finally {
        release.resolve();
        activeController.abort();
        await within(activeResult).catch(() => { });
    }

    console.log("Cancellation waits for the active phase before returning: OK");

    const parents = [
        new AbortController(),
        new AbortController(),
    ];

    const finishedSignals = [];

    const successful = makeWaiter(
        async (id, body, caller, options) => {
            finishedSignals.push(options.signal);
            return completed;
        },
    );

    await successful.waitForOrder(
        operationId,
        input,
        actor,
        {
            timeoutMs: 200,
            signal: parents[0].signal,
        },
    );

    const originalError = new Error("Fixture failure");

    const failing = makeWaiter(
        async (id, body, caller, options) => {
            finishedSignals.push(options.signal);
            throw originalError;
        },
    );

    await assert.rejects(
        () => failing.waitForOrder(
            operationId,
            input,
            actor,
            {
                timeoutMs: 200,
                signal: parents[1].signal,
            },
        ),
        (error) => error === originalError,
    );

    for (const parent of parents) {
        assert.equal(
            getEventListeners(parent.signal, "abort").length,
            0,
        );

        parent.abort();
    }

    // Timer cũ không được tiếp tục hủy request đã kết thúc.
    await new Promise((resolve) => {
        setTimeout(resolve, 250);
    });

    for (const signal of finishedSignals) {
        assert.equal(signal.aborted, false);

        assert.equal(
            getEventListeners(signal, "abort").length,
            0,
        );
    }

    console.log("Success and failure dispose timers and parent listeners: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Creation wait cancellation check failed:",
        error.code ?? error.name,
    );

    process.exitCode = 1;
} finally {
    customerClient.close();
    productClient.close();
    shipmentClient.close();

    console.log("Wait cancellation test connections closed: OK");
}