import assert from "node:assert/strict";
import {
    setTimeout as delay,
    setImmediate as nextTurn,
} from "node:timers/promises";
import { DomainError } from "../src/domain/errors.js";
import {
    createOutboxLoop,
} from "../src/application/outbox.loop.js";

const loops = [];
const gates = [];

const silentLogger = {
    error() { },
};

function gate() {
    const deferred = Promise.withResolvers();
    gates.push(deferred);
    return deferred;
}

function createLoop(options) {
    const loop = createOutboxLoop({
        logger: silentLogger,
        ...options,
    });

    loops.push(loop);
    return loop;
}

async function withDeadline(promise) {
    let timer;

    try {
        return await Promise.race([
            promise,
            new Promise((resolve, reject) => {
                timer = setTimeout(
                    () => reject(new Error("Loop check timed out")),
                    5000
                );
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}

async function main() {
    let calls = 0;
    let active = 0;
    let maxActive = 0;

    const threeBatches = gate();

    const sequential = createLoop({
        intervalMs: 1,

        worker: {
            async runOnce() {
                active += 1;
                maxActive = Math.max(maxActive, active);

                try {
                    await delay(2);
                    calls += 1;

                    if (calls === 3) {
                        threeBatches.resolve();
                    }
                } finally {
                    active -= 1;
                }
            },
        },
    });

    assert.equal(sequential.start(), true);
    assert.equal(sequential.start(), false);
    assert.equal(sequential.isRunning, true);

    await withDeadline(threeBatches.promise);
    await withDeadline(sequential.stop());

    assert.equal(calls, 3);
    assert.equal(maxActive, 1);
    assert.equal(active, 0);
    assert.equal(sequential.isRunning, false);

    console.log(
        "Outbox loop starts once and runs sequential batches: OK"
    );

    const logs = [];
    const recovered = gate();
    let recoveryCalls = 0;

    const recovery = createLoop({
        intervalMs: 1,

        logger: {
            error(fields, message) {
                logs.push({ fields, message });
            },
        },

        worker: {
            async runOnce() {
                recoveryCalls += 1;

                if (recoveryCalls === 1) {
                    throw new DomainError(
                        "SERVICE_UNAVAILABLE",
                        "Simulated service failure"
                    );
                }

                if (recoveryCalls === 2) {
                    throw new Error("Internal failure details");
                }

                recovered.resolve();
            },
        },
    });

    recovery.start();

    await withDeadline(recovered.promise);
    await withDeadline(recovery.stop());

    assert.deepEqual(logs, [
        {
            fields: { code: "SERVICE_UNAVAILABLE" },
            message: "Outbox worker batch failed",
        },
        {
            fields: { code: "INTERNAL" },
            message: "Outbox worker batch failed",
        },
    ]);

    assert.equal(
        JSON.stringify(logs).includes("Internal failure details"),
        false
    );

    console.log(
        "Batch failures use Outbox logs and recover: OK"
    );

    const idleBatch = gate();
    let idleCalls = 0;

    const idle = createLoop({
        intervalMs: 60_000,

        worker: {
            async runOnce() {
                idleCalls += 1;
                idleBatch.resolve();
            },
        },
    });

    idle.start();

    await withDeadline(idleBatch.promise);
    await nextTurn();

    await withDeadline(idle.stop());
    await nextTurn();

    assert.equal(idleCalls, 1);
    assert.equal(idle.isRunning, false);

    console.log(
        "Stop interrupts idle wait without another batch: OK"
    );

    const entered = gate();
    const release = gate();
    const restarted = gate();

    let drainCalls = 0;

    const draining = createLoop({
        intervalMs: 60_000,

        worker: {
            async runOnce() {
                drainCalls += 1;

                if (drainCalls === 1) {
                    entered.resolve();
                    await release.promise;
                } else {
                    restarted.resolve();
                }
            },
        },
    });

    draining.start();
    await withDeadline(entered.promise);

    let stopFinished = false;

    const stopping = draining.stop();

    void stopping.then(() => {
        stopFinished = true;
    });

    await nextTurn();

    assert.equal(stopFinished, false);
    assert.equal(draining.start(), false);

    release.resolve();

    await withDeadline(stopping);

    assert.equal(draining.isRunning, false);
    assert.equal(draining.start(), true);

    await withDeadline(restarted.promise);
    await withDeadline(draining.stop());

    assert.equal(drainCalls, 2);
    assert.equal(draining.isRunning, false);

    console.log(
        "Stop drains active batch and permits restart: OK"
    );

    const worker = {
        async runOnce() { },
    };

    for (const intervalMs of [
        0, -1, 60_001, 1.5, "1000", null, NaN,
    ]) {
        assert.throws(
            () => createOutboxLoop({
                worker,
                intervalMs,
                logger: silentLogger,
            }),
            (error) => {
                assert.ok(error instanceof DomainError);
                assert.equal(error.code, "INVALID_ARGUMENT");
                return true;
            }
        );
    }

    assert.throws(
        () => createOutboxLoop({
            worker: {},
            logger: silentLogger,
        }),
        TypeError
    );

    assert.throws(
        () => createOutboxLoop({
            worker,
            logger: {},
        }),
        TypeError
    );

    const stopped = createLoop({ worker });

    await stopped.stop();
    await stopped.stop();

    assert.equal(stopped.isRunning, false);

    console.log(
        "Loop options and stopped state validated: OK"
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Outbox loop check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    for (const deferred of gates) {
        deferred.resolve();
    }

    let cleaned = true;

    for (const loop of loops) {
        try {
            await withDeadline(loop.stop());
            assert.equal(loop.isRunning, false);
        } catch (error) {
            cleaned = false;
            process.exitCode = 1;

            console.error(
                "Outbox loop cleanup failed:",
                error.code ?? error.message
            );
        }
    }

    if (cleaned) {
        console.log("Loop fixtures and timers removed: OK");
    }
}