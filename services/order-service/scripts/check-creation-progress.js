import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import {
    assertSameCreationTask,
} from "../src/domain/order-creation.rules.js";
import {
    createOrderCreationApplication,
} from "../src/application/order-creation.application.js";
import {
    createOrderCreationResultReader,
} from "../src/application/order-creation.result.js";
import {
    createOrderCreationProgress,
} from "../src/application/order-creation.progress.js";
import {
    customerClient,
} from "../src/infrastructure/grpc/customer.client.js";
import {
    productClient,
} from "../src/infrastructure/grpc/product.client.js";
import {
    shipmentClient,
} from "../src/infrastructure/grpc/shipment.client.js";

const actor = { uid: 13, role: "CUSTOMER" };
const input = { items: [{ pid: 7, qty: 2 }] };

const tasks = new Map();
const orders = new Map();
const shipments = new Map();

const dispatchCalls = [];

let due = true;
let submitError = null;
let dispatchError = null;
let readError = null;

const submission = createOrderCreationApplication({
    tasks: {
        async createTask(request) {
            if (submitError) {
                throw submitError;
            }

            const existing = tasks.get(request.operationId);

            if (existing) {
                assertSameCreationTask(existing, request);
                return structuredClone(existing);
            }

            const task = {
                operation_id: request.operationId,
                uid: request.uid,
                actor_role: request.actorRole,
                items: structuredClone(request.items),
                status: "PROCESSING",
                score: null,
                failure_code: null,
            };

            tasks.set(request.operationId, task);

            return structuredClone(task);
        },
    },
});

const results = createOrderCreationResultReader({
    tasks: {
        async findByOperationId(id) {
            if (readError) {
                throw readError;
            }

            return structuredClone(tasks.get(id) ?? null);
        },
    },
    orders: {
        async findByOperationId(id) {
            return structuredClone(orders.get(id) ?? null);
        },
    },
    shipments: {
        async getShipmentBySourceEvent(id, oid, caller) {
            assert.equal(caller.uid, actor.uid);

            const shipment = shipments.get(id);

            if (!shipment) {
                throw new DomainError(
                    "NOT_FOUND",
                    "Shipment not found",
                );
            }

            assert.equal(shipment.oid, oid);

            return structuredClone(shipment);
        },
    },
});

const progress = createOrderCreationProgress({
    submission,
    results,
    dispatcher: {
        async processDueTask(id) {
            dispatchCalls.push(id);

            if (dispatchError) {
                throw dispatchError;
            }

            const task = tasks.get(id);

            if (!due) {
                return { outcome: "NOT_DUE" };
            }

            if (task.status === "PROCESSING") {
                if (task.score === null) {
                    task.score = 100;

                    return { outcome: "SCORE_FROZEN" };
                }

                orders.set(id, {
                    oid: 41,
                    uid: task.uid,
                    discount: "2.55",
                    items: [{
                        pid: 7,
                        qty: 2,
                        unit_price: "25.50",
                    }],
                });

                task.status = "CONFIRM_PENDING";

                return { outcome: "ORDER_STORED" };
            }

            if (task.status === "CONFIRM_PENDING") {
                task.status = "COMPLETED";

                return { outcome: "COMPLETED" };
            }

            return { outcome: "ALREADY_COMPLETED" };
        },
    },
});

async function expectCode(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function advance(id, body = input, caller = actor) {
    return progress.advanceOrder(id, body, caller);
}

async function main() {
    const id = randomUUID();
    const unchangedInput = structuredClone(input);

    assert.equal(await advance(id), null);
    assert.equal(dispatchCalls.length, 1);
    assert.equal(tasks.get(id).score, 100);
    assert.equal(tasks.get(id).status, "PROCESSING");
    assert.equal(orders.has(id), false);

    assert.equal(await advance(id), null);
    assert.equal(dispatchCalls.length, 2);
    assert.equal(tasks.get(id).status, "CONFIRM_PENDING");
    assert.equal(orders.has(id), true);

    assert.equal(await advance(id), null);
    assert.equal(dispatchCalls.length, 3);
    assert.equal(tasks.get(id).status, "COMPLETED");

    const storedOrder = structuredClone(orders.get(id));

    // Shipment chưa xuất hiện: task và Order vẫn được giữ nguyên.
    assert.equal(await advance(id), null);
    assert.deepEqual(orders.get(id), storedOrder);
    assert.equal(tasks.get(id).status, "COMPLETED");

    shipments.set(id, {
        shipid: 61,
        oid: 41,
        status: "PENDING",
    });

    const expected = {
        ...storedOrder,
        shipment: {
            shipid: 61,
            oid: 41,
            status: "PENDING",
        },
    };

    assert.deepEqual(await advance(id), expected);
    assert.deepEqual(input, unchangedInput);

    console.log("Single-phase creation progress and Shipment readiness: OK");

    const beforeReplay = dispatchCalls.length;

    assert.deepEqual(await advance(id), expected);
    assert.equal(dispatchCalls.length, beforeReplay);
    assert.equal(tasks.size, 1);
    assert.equal(orders.size, 1);

    console.log("Ready result replay skips dispatcher: OK");

    const beforeInvalid = dispatchCalls.length;

    await expectCode(
        () => advance(randomUUID(), input, null),
        "UNAUTHENTICATED",
    );

    await expectCode(
        () => advance("invalid"),
        "INVALID_ARGUMENT",
    );

    await expectCode(
        () => advance(randomUUID(), {
            items: [
                { pid: 7, qty: 1 },
                { pid: 7, qty: 2 },
            ],
        }),
        "INVALID_ARGUMENT",
    );

    await expectCode(
        () => advance(id, input, {
            uid: 14,
            role: "CUSTOMER",
        }),
        "FORBIDDEN",
    );

    await expectCode(
        () => advance(id, {
            items: [{ pid: 7, qty: 3 }],
        }),
        "CONFLICT",
    );

    await expectCode(
        () => advance(id, input, {
            uid: 13,
            role: "ADMIN",
        }),
        "CONFLICT",
    );

    assert.equal(dispatchCalls.length, beforeInvalid);

    console.log("Invalid and rebound requests cannot dispatch tasks: OK");

    const deferredId = randomUUID();
    due = false;

    const beforeDeferred = dispatchCalls.length;

    assert.equal(await advance(deferredId), null);
    assert.equal(
        dispatchCalls.length,
        beforeDeferred + 1,
    );

    assert.equal(tasks.get(deferredId).score, null);
    assert.equal(
        tasks.get(deferredId).status,
        "PROCESSING",
    );
    assert.equal(orders.has(deferredId), false);

    due = true;

    console.log("Deferred task decisions remain with dispatcher: OK");

    const submissionFailure = new Error(
        "Injected submission failure",
    );

    submitError = submissionFailure;

    const beforeSubmissionFailure = dispatchCalls.length;

    await assert.rejects(
        () => advance(randomUUID()),
        (error) => error === submissionFailure,
    );

    assert.equal(
        dispatchCalls.length,
        beforeSubmissionFailure,
    );

    submitError = null;

    const dispatchFailure = new Error(
        "Injected dispatch failure",
    );

    dispatchError = dispatchFailure;

    const failedDispatchId = randomUUID();
    const beforeDispatchFailure = dispatchCalls.length;

    await assert.rejects(
        () => advance(failedDispatchId),
        (error) => error === dispatchFailure,
    );

    assert.equal(
        dispatchCalls.length,
        beforeDispatchFailure + 1,
    );

    assert.equal(
        tasks.get(failedDispatchId).status,
        "PROCESSING",
    );

    dispatchError = null;

    const readFailure = new Error(
        "Injected result read failure",
    );

    readError = readFailure;

    const beforeReadFailure = dispatchCalls.length;

    await assert.rejects(
        () => advance(id),
        (error) => error === readFailure,
    );

    assert.equal(dispatchCalls.length, beforeReadFailure);
    assert.deepEqual(orders.get(id), storedOrder);
    assert.equal(tasks.get(id).status, "COMPLETED");

    readError = null;

    console.log("Failures propagate without hidden retries or release: OK");

    const failedId = randomUUID();

    await submission.submitOrder(failedId, input, actor);

    tasks.get(failedId).status = "FAILED";
    tasks.get(failedId).failure_code = "CONFLICT";

    const beforeFailed = dispatchCalls.length;

    await expectCode(
        () => advance(failedId),
        "CONFLICT",
    );

    assert.equal(dispatchCalls.length, beforeFailed);
    assert.equal(tasks.get(failedId).failure_code, "CONFLICT");
    assert.equal(tasks.get(failedId).status, "FAILED");

    assert.throws(
        () => createOrderCreationProgress({
            dispatcher: {},
        }),
        TypeError,
    );

    console.log("Durable failure preserved before dispatch: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Creation progress check failed:",
        error.code ?? error.name,
    );

    process.exitCode = 1;
} finally {
    tasks.clear();
    orders.clear();
    shipments.clear();

    customerClient.close();
    productClient.close();
    shipmentClient.close();
}