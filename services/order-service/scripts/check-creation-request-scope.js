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

function scope(controller = new AbortController()) {
    return {
        deadline: new Date(Date.now() + 60_000),
        signal: controller.signal,
        timeoutMs: 1000,
    };
}

function fixture({
    status = "PROCESSING",
    afterSubmit,
    afterDispatch,
    duringShipment,
} = {}) {
    const operationId = randomUUID();

    const state = {
        persisted: false,
        task: {
            operation_id: operationId,
            uid: 13,
            actor_role: "CUSTOMER",
            items: structuredClone(input.items),
            status,
            score: 100,
            failure_code: status === "FAILED"
                ? "CONFLICT"
                : null,
        },
        order: {
            oid: 41,
            uid: 13,
            discount: "2.55",
            items: [{
                pid: 7,
                qty: 2,
                unit_price: "25.50",
            }],
        },
        calls: {
            submit: 0,
            read: 0,
            order: 0,
            shipment: 0,
            dispatch: 0,
        },
        rpcOptions: [],
    };

    const submission = createOrderCreationApplication({
        tasks: {
            async createTask(request) {
                state.calls.submit += 1;

                assertSameCreationTask(state.task, request);

                state.persisted = true;
                afterSubmit?.();

                return structuredClone(state.task);
            },
        },
    });

    const results = createOrderCreationResultReader({
        tasks: {
            async findByOperationId(id) {
                state.calls.read += 1;
                assert.equal(id, operationId);

                return state.persisted
                    ? structuredClone(state.task)
                    : null;
            },
        },
        orders: {
            async findByOperationId(id) {
                state.calls.order += 1;
                assert.equal(id, operationId);

                return structuredClone(state.order);
            },
        },
        shipments: {
            async getShipmentBySourceEvent(
                id,
                oid,
                caller,
                options,
            ) {
                state.calls.shipment += 1;

                assert.equal(id, operationId);
                assert.equal(oid, 41);
                assert.deepEqual(caller, actor);

                state.rpcOptions.push({
                    target: "shipment",
                    options,
                });

                duringShipment?.();

                return {
                    shipid: 61,
                    oid: 41,
                    status: "PENDING",
                };
            },
        },
    });

    const progress = createOrderCreationProgress({
        submission,
        results,
        dispatcher: {
            async processDueTask(id, options) {
                state.calls.dispatch += 1;
                assert.equal(id, operationId);

                state.rpcOptions.push({
                    target: "dispatcher",
                    options,
                });

                state.task.status = "COMPLETED";

                afterDispatch?.();
            },
        },
    });

    return {
        state,
        advance(options, caller = actor) {
            return progress.advanceOrder(
                operationId,
                input,
                caller,
                options,
            );
        },
    };
}

async function expectCode(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    const normal = fixture();
    const options = scope();

    const result = await normal.advance(options);

    assert.equal(result.oid, 41);
    assert.equal(result.discount, "2.55");
    assert.equal(result.items[0].unit_price, "25.50");

    assert.deepEqual(result.shipment, {
        shipid: 61,
        oid: 41,
        status: "PENDING",
    });

    assert.deepEqual(
        normal.state.rpcOptions.map((call) => call.target),
        ["dispatcher", "shipment"],
    );

    for (const call of normal.state.rpcOptions) {
        assert.equal(call.options.signal, options.signal);

        assert.equal(
            call.options.deadline.getTime(),
            options.deadline.getTime(),
        );

        assert.equal(call.options.timeoutMs, 1000);
    }

    const defaults = fixture();
    await defaults.advance();

    for (const call of defaults.state.rpcOptions) {
        assert.deepEqual(call.options, {});
    }

    console.log("Request options reach dispatcher and Shipment lookup: OK");

    const controller = new AbortController();
    controller.abort();

    for (const invalidOptions of [
        { signal: controller.signal },
        { deadline: new Date(Date.now() - 1000) },
    ]) {
        const test = fixture();

        await expectCode(
            () => test.advance(invalidOptions),
            "SERVICE_UNAVAILABLE",
        );

        assert.equal(test.state.calls.submit, 0);
        assert.equal(test.state.calls.read, 0);
        assert.equal(test.state.calls.dispatch, 0);
    }

    for (const invalidOptions of [
        { signal: {} },
        { deadline: new Date("invalid") },
        { timeoutMs: 0 },
    ]) {
        const test = fixture();

        await expectCode(
            () => test.advance(invalidOptions),
            "INVALID_ARGUMENT",
        );

        assert.equal(test.state.calls.submit, 0);
    }

    console.log("Ended and invalid request scopes stop before submission: OK");

    const submitController = new AbortController();

    const afterSubmit = fixture({
        afterSubmit() {
            submitController.abort();
        },
    });

    await expectCode(
        () => afterSubmit.advance(scope(submitController)),
        "SERVICE_UNAVAILABLE",
    );

    assert.equal(afterSubmit.state.persisted, true);
    assert.equal(afterSubmit.state.calls.submit, 1);
    assert.equal(afterSubmit.state.calls.read, 0);
    assert.equal(afterSubmit.state.calls.dispatch, 0);
    assert.equal(afterSubmit.state.task.status, "PROCESSING");

    console.log("Cancellation after submission preserves task and stops reads: OK");

    const dispatchController = new AbortController();

    const afterDispatch = fixture({
        afterDispatch() {
            dispatchController.abort();
        },
    });

    await expectCode(
        () => afterDispatch.advance(scope(dispatchController)),
        "SERVICE_UNAVAILABLE",
    );

    assert.equal(afterDispatch.state.calls.read, 1);
    assert.equal(afterDispatch.state.calls.dispatch, 1);
    assert.equal(afterDispatch.state.calls.shipment, 0);

    assert.equal(
        afterDispatch.state.task.status,
        "COMPLETED",
    );

    assert.equal(
        afterDispatch.state.order.items[0].unit_price,
        "25.50",
    );

    console.log("Active phase completes before cancellation stops follow-up work: OK");

    const shipmentController = new AbortController();

    const duringShipment = fixture({
        status: "COMPLETED",
        duringShipment() {
            shipmentController.abort();
        },
    });

    const storedOrder = structuredClone(
        duringShipment.state.order,
    );

    await expectCode(
        () => duringShipment.advance(scope(shipmentController)),
        "SERVICE_UNAVAILABLE",
    );

    assert.equal(duringShipment.state.calls.dispatch, 0);

    assert.deepEqual(
        duringShipment.state.order,
        storedOrder,
    );

    assert.equal(
        duringShipment.state.task.status,
        "COMPLETED",
    );

    console.log("Cancelled Shipment lookup cannot return success or change Order: OK");

    const wrongOwner = fixture();

    await expectCode(
        () => wrongOwner.advance(scope(), {
            uid: 14,
            role: "CUSTOMER",
        }),
        "FORBIDDEN",
    );

    assert.equal(wrongOwner.state.calls.dispatch, 0);

    const failed = fixture({ status: "FAILED" });

    await expectCode(
        () => failed.advance(scope()),
        "CONFLICT",
    );

    assert.equal(failed.state.task.failure_code, "CONFLICT");
    assert.equal(failed.state.calls.dispatch, 0);

    const pendingShipment = fixture({
        status: "COMPLETED",
        duringShipment() {
            throw new DomainError(
                "NOT_FOUND",
                "Shipment not found",
            );
        },
    });

    assert.equal(
        await pendingShipment.advance(scope()),
        null,
    );

    assert.equal(
        pendingShipment.state.task.status,
        "COMPLETED",
    );

    const serviceError = new DomainError(
        "SERVICE_UNAVAILABLE",
        "Shipment unavailable",
    );

    const unavailable = fixture({
        status: "COMPLETED",
        duringShipment() {
            throw serviceError;
        },
    });

    await assert.rejects(
        () => unavailable.advance(scope()),
        (error) => error === serviceError,
    );

    assert.equal(unavailable.state.calls.dispatch, 0);

    console.log("Ownership, durable failures and pending Shipment behavior preserved: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Creation request scope check failed:",
        error.code ?? error.name,
    );

    process.exitCode = 1;
} finally {
    customerClient.close();
    productClient.close();
    shipmentClient.close();

    console.log("Creation request scope test connections closed: OK");
}