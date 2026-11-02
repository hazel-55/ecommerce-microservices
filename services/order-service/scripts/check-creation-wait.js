import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
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

const actor = {
    uid: 13,
    role: "CUSTOMER",
};

const input = {
    items: [{ pid: 7, qty: 2 }],
};

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

function fixture(advance) {
    let clock = 0;
    let active = false;

    const calls = [];
    const waits = [];

    const waiter = createOrderCreationWaiter({
        now: () => clock,

        async sleep(milliseconds) {
            assert.equal(active, false);
            assert.ok(milliseconds > 0);

            waits.push(milliseconds);
            clock += milliseconds;
        },

        progress: {
            async advanceOrder(...args) {
                assert.equal(active, false);
                active = true;

                // Giữ nguyên reference của AbortSignal.
                calls.push([
                    args[0],
                    structuredClone(args[1]),
                    structuredClone(args[2]),
                    args[3],
                ]);

                try {
                    return await advance({
                        args,
                        count: calls.length,

                        elapse(milliseconds) {
                            clock += milliseconds;
                        },
                    });
                } finally {
                    active = false;
                }
            },
        },
    });

    return { waiter, calls, waits };
}

async function expectCode(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    const operationId = randomUUID();

    const success = fixture(async ({ count }) => {
        return count === 3 ? completed : null;
    });

    assert.deepEqual(
        await success.waiter.waitForOrder(
            operationId,
            input,
            actor,
            {
                timeoutMs: 500,
                intervalMs: 100,
            },
        ),
        completed,
    );

    assert.equal(success.calls.length, 3);
    assert.deepEqual(success.waits, [100, 100]);

    for (const args of success.calls) {
        assert.deepEqual(args.slice(0, 3), [
            operationId,
            input,
            actor,
        ]);

        assert.ok(args[3].deadline instanceof Date);
        assert.ok(args[3].signal instanceof AbortSignal);

        assert.equal(
            args[3].deadline,
            success.calls[0][3].deadline,
        );

        assert.equal(
            args[3].signal,
            success.calls[0][3].signal,
        );
    }

    console.log("Sequential wait preserves operation and request: OK");

    const persisted = new Set();
    let ready = false;

    const pending = fixture(async ({ args }) => {
        persisted.add(args[0]);

        return ready ? completed : null;
    });

    await expectCode(
        () => pending.waiter.waitForOrder(
            operationId,
            input,
            actor,
            {
                timeoutMs: 250,
                intervalMs: 100,
            },
        ),
        "SERVICE_UNAVAILABLE",
    );

    assert.equal(pending.calls.length, 3);
    assert.deepEqual(pending.waits, [100, 100, 50]);

    assert.equal(persisted.size, 1);
    assert.ok(persisted.has(operationId));

    ready = true;

    assert.deepEqual(
        await pending.waiter.waitForOrder(
            operationId,
            input,
            actor,
        ),
        completed,
    );

    assert.equal(persisted.size, 1);

    assert.equal(
        pending.calls.at(-1)[0],
        operationId,
    );

    console.log("Wait expiry and same-operation recovery: OK");

    for (const error of [
        new DomainError("CONFLICT", "Test conflict"),
        new DomainError("FORBIDDEN", "Test permission"),
        new DomainError(
            "SERVICE_UNAVAILABLE",
            "Test service outage",
        ),
        new Error("Test database failure"),
    ]) {
        const failed = fixture(async () => {
            throw error;
        });

        await assert.rejects(
            () => failed.waiter.waitForOrder(
                operationId,
                input,
                actor,
            ),
            (actual) => actual === error,
        );

        assert.equal(failed.calls.length, 1);
        assert.equal(failed.waits.length, 0);
    }

    console.log("Failures propagate without automatic retry: OK");

    const invalid = fixture(async () => completed);

    for (const options of [
        { timeoutMs: 0 },
        { timeoutMs: 60_001 },
        { timeoutMs: 1.5 },
        { intervalMs: 0 },
        { intervalMs: 1001 },
    ]) {
        await expectCode(
            () => invalid.waiter.waitForOrder(
                operationId,
                input,
                actor,
                options,
            ),
            "INVALID_ARGUMENT",
        );
    }

    await expectCode(
        () => invalid.waiter.waitForOrder(
            operationId,
            input,
            null,
        ),
        "UNAUTHENTICATED",
    );

    await expectCode(
        () => invalid.waiter.waitForOrder(
            "invalid",
            input,
            actor,
        ),
        "INVALID_ARGUMENT",
    );

    await expectCode(
        () => invalid.waiter.waitForOrder(
            operationId,
            { items: [] },
            actor,
        ),
        "INVALID_ARGUMENT",
    );

    assert.equal(invalid.calls.length, 0);
    assert.equal(invalid.waits.length, 0);

    assert.throws(
        () => createOrderCreationWaiter({
            progress: {},
        }),
        TypeError,
    );

    console.log("Invalid wait options and requests rejected: OK");

    const mutableInput = structuredClone(input);
    const mutableActor = structuredClone(actor);

    const snapshot = fixture(async ({ args, count }) => {
        assert.deepEqual(args.slice(0, 3), [
            operationId,
            input,
            actor,
        ]);

        if (count === 1) {
            mutableInput.items[0].qty = 999;
            mutableActor.uid = 14;
            mutableActor.role = "ADMIN";

            return null;
        }

        return completed;
    });

    assert.deepEqual(
        await snapshot.waiter.waitForOrder(
            operationId,
            mutableInput,
            mutableActor,
        ),
        completed,
    );

    assert.equal(snapshot.calls.length, 2);

    console.log("Request snapshot survives external mutation: OK");

    let activePhaseFinished = false;

    const overrun = fixture(async ({ elapse }) => {
        elapse(300);
        await Promise.resolve();

        activePhaseFinished = true;

        return null;
    });

    await expectCode(
        () => overrun.waiter.waitForOrder(
            operationId,
            input,
            actor,
            {
                timeoutMs: 250,
                intervalMs: 100,
            },
        ),
        "SERVICE_UNAVAILABLE",
    );

    assert.equal(activePhaseFinished, true);
    assert.equal(overrun.calls.length, 1);
    assert.equal(overrun.waits.length, 0);

    console.log("Active phase settles before wait expiry returns: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Creation wait check failed:",
        error.code ?? error.name,
    );

    process.exitCode = 1;
} finally {
    customerClient.close();
    productClient.close();
    shipmentClient.close();
}