import assert from "node:assert/strict";
import { DomainError } from "../src/domain/errors.js";
import {
    buildOutboxRetryPlan,
} from "../src/domain/outbox.retry.js";

function expectInvalid(operation) {
    assert.throws(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, "INVALID_ARGUMENT");
        return true;
    });
}

try {
    const expectedDelays = [
        1000,
        2000,
        4000,
        8000,
        16000,
        32000,
        60000,
        60000,
    ];

    for (
        let attempts = 0;
        attempts < expectedDelays.length;
        attempts += 1
    ) {
        assert.deepEqual(
            buildOutboxRetryPlan(
                attempts,
                "SERVICE_UNAVAILABLE"
            ),
            {
                attempts: attempts + 1,
                delayMs: expectedDelays[attempts],
                errorCode: "SERVICE_UNAVAILABLE",
            }
        );
    }

    assert.deepEqual(
        buildOutboxRetryPlan(10000, "INTERNAL"),
        {
            attempts: 10001,
            delayMs: 60000,
            errorCode: "INTERNAL",
        }
    );

    console.log("Exponential Outbox retry delay and cap: OK");

    for (const attempts of [
        2147483646,
        2147483647,
    ]) {
        assert.deepEqual(
            buildOutboxRetryPlan(attempts, "INTERNAL"),
            {
                attempts: 2147483647,
                delayMs: 60000,
                errorCode: "INTERNAL",
            }
        );
    }

    console.log("Outbox retry counter upper bound: OK");

    for (const attempts of [
        -1,
        1.5,
        2147483648,
        NaN,
        Infinity,
        "1",
        null,
        undefined,
    ]) {
        expectInvalid(() =>
            buildOutboxRetryPlan(
                attempts,
                "SERVICE_UNAVAILABLE"
            )
        );
    }

    console.log("Invalid Outbox retry counts rejected: OK");

    for (const errorCode of [
        "",
        "UNKNOWN",
        "Raw connection failure details",
        null,
        undefined,
        new Error("Connection failed"),
    ]) {
        expectInvalid(() =>
            buildOutboxRetryPlan(0, errorCode)
        );
    }

    console.log("Invalid Outbox retry error codes rejected: OK");
} catch (error) {
    console.error(
        "Outbox retry policy check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
}