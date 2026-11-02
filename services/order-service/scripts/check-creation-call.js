import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import grpc from "@grpc/grpc-js";
import { DomainError } from "../src/domain/errors.js";
import {
    readCreationOperationId,
    createCreationCallScope,
} from "../src/interfaces/grpc/creation-call.js";

const scopes = [];

function fakeCall(
    deadline = Infinity,
    operationId = randomUUID(),
) {
    const call = new EventEmitter();

    call.metadata = new grpc.Metadata();

    call.metadata.set(
        "x-order-operation-id",
        operationId,
    );

    call.cancelled = false;
    call.getDeadline = () => deadline;

    return call;
}

function openScope(call, options) {
    const scope = createCreationCallScope(call, options);
    scopes.push(scope);
    return scope;
}

function expectCode(operation, code) {
    assert.throws(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

function main() {
    const operationId = randomUUID();
    const call = fakeCall(Infinity, operationId);

    assert.equal(
        readCreationOperationId(call.metadata),
        operationId,
    );

    const scope = openScope(call);

    assert.equal(scope.operationId, operationId);

    assert.ok(
        scope.waitOptions.deadline instanceof Date,
    );

    assert.ok(
        scope.waitOptions.signal instanceof AbortSignal,
    );

    assert.equal(scope.waitOptions.signal.aborted, false);
    assert.equal(scope.waitOptions.timeoutMs, 5000);

    console.log("Canonical operation identity from internal metadata: OK");

    const duplicate = fakeCall();

    duplicate.metadata.add(
        "x-order-operation-id",
        randomUUID(),
    );

    expectCode(
        () => createCreationCallScope(duplicate),
        "INVALID_ARGUMENT",
    );

    assert.equal(
        duplicate.listenerCount("cancelled"),
        0,
    );

    for (const values of [
        [],
        null,
        [Buffer.from(operationId)],
        ["invalid"],
        ["ABCDEF12-1234-1234-1234-123456789ABC"],
    ]) {
        const invalid = fakeCall();

        invalid.metadata = {
            get() {
                return values;
            },
        };

        expectCode(
            () => createCreationCallScope(invalid),
            "INVALID_ARGUMENT",
        );

        assert.equal(
            invalid.listenerCount("cancelled"),
            0,
        );
    }

    expectCode(
        () => readCreationOperationId(null),
        "INVALID_ARGUMENT",
    );

    console.log("Invalid and duplicate operation metadata rejected: OK");

    const parentDeadline = new Date(Date.now() + 1000);
    const originalDeadline = parentDeadline.getTime();

    const parent = openScope(fakeCall(parentDeadline));

    assert.equal(
        parent.waitOptions.deadline.getTime(),
        originalDeadline,
    );

    assert.equal(
        parentDeadline.getTime(),
        originalDeadline,
    );

    assert.notEqual(
        parent.waitOptions.deadline,
        parentDeadline,
    );

    for (const received of [
        Infinity,
        Date.now() + 60_000,
    ]) {
        const before = Date.now();
        const capped = openScope(fakeCall(received));
        const after = Date.now();

        assert.ok(
            capped.waitOptions.deadline.getTime() >= before + 5000,
        );

        assert.ok(
            capped.waitOptions.deadline.getTime() <= after + 5000,
        );
    }

    console.log("Caller deadline and server wait cap preserved: OK");

    const alreadyCancelled = fakeCall();
    alreadyCancelled.cancelled = true;

    const cancelledScope = openScope(alreadyCancelled);

    assert.equal(
        cancelledScope.waitOptions.signal.aborted,
        true,
    );

    const expired = openScope(
        fakeCall(Date.now() - 1000),
    );

    assert.equal(
        expired.waitOptions.signal.aborted,
        true,
    );

    console.log("Already cancelled and expired calls produce aborted scopes: OK");

    const activeCall = fakeCall();
    const active = openScope(activeCall);

    assert.equal(activeCall.listenerCount("cancelled"), 1);

    activeCall.cancelled = true;
    activeCall.emit("cancelled");

    assert.equal(active.waitOptions.signal.aborted, true);

    active.dispose();
    active.dispose();

    assert.equal(activeCall.listenerCount("cancelled"), 0);

    const finishedCall = fakeCall();
    const finished = openScope(finishedCall);

    finished.dispose();
    finishedCall.emit("cancelled");

    assert.equal(
        finished.waitOptions.signal.aborted,
        false,
    );

    console.log("Active cancellation and idempotent disposal: OK");

    const firstCall = fakeCall();
    const secondCall = fakeCall();

    const first = openScope(firstCall);
    const second = openScope(secondCall);

    firstCall.emit("cancelled");

    assert.equal(first.waitOptions.signal.aborted, true);
    assert.equal(second.waitOptions.signal.aborted, false);

    assert.notEqual(first.operationId, second.operationId);
    assert.notEqual(
        first.waitOptions.signal,
        second.waitOptions.signal,
    );

    const invalidTimeoutCall = fakeCall();

    expectCode(
        () => createCreationCallScope(
            invalidTimeoutCall,
            { timeoutMs: 0 },
        ),
        "INVALID_ARGUMENT",
    );

    assert.equal(
        invalidTimeoutCall.listenerCount("cancelled"),
        0,
    );

    expectCode(
        () => createCreationCallScope(null),
        "INTERNAL",
    );

    for (const deadline of [
        NaN,
        -Infinity,
        "invalid",
        new Date("invalid"),
    ]) {
        const invalid = fakeCall(deadline);

        expectCode(
            () => createCreationCallScope(invalid),
            "INTERNAL",
        );

        assert.equal(
            invalid.listenerCount("cancelled"),
            0,
        );
    }

    console.log("Concurrent call scopes remain isolated: OK");
}

try {
    main();
} catch (error) {
    console.error(
        "Creation call check failed:",
        error.code ?? error.name,
    );

    process.exitCode = 1;
} finally {
    for (const scope of scopes) {
        scope.dispose();
    }
}