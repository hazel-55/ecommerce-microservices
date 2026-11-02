import assert from "node:assert/strict";
import { DomainError } from "../src/domain/errors.js";
import {
    createOrderCreationLoop,
} from "../src/application/order-creation.loop.js";

const loops = [];
const releases = [];
const logs = [];

const logger = {
    error(data, message) {
        logs.push({
            data: structuredClone(data),
            message,
        });
    },
};

function gate() {
    let resolve;

    const promise = new Promise((release) => {
        resolve = release;
    });

    releases.push(resolve);

    return { promise, resolve };
}

function loopFor(options) {
    const loop = createOrderCreationLoop({
        logger,
        ...options,
    });

    loops.push(loop);

    return loop;
}

async function until(predicate, timeoutMs = 2000) {
    const deadline = Date.now() + timeoutMs;

    while (!predicate()) {
        if (Date.now() >= deadline) {
            throw new Error("Timed out waiting for loop condition");
        }

        await new Promise((resolve) => {
            setTimeout(resolve, 5);
        });
    }
}

async function within(promise, timeoutMs = 2000) {
    let timer;

    try {
        return await Promise.race([
            promise,
            new Promise((_, reject) => {
                timer = setTimeout(
                    () => reject(
                        new Error("Loop operation timed out"),
                    ),
                    timeoutMs,
                );
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}

function nextTurn() {
    return new Promise((resolve) => {
        setImmediate(resolve);
    });
}

async function main() {
    const firstEntered = gate();
    const finishFirst = gate();

    let calls = 0;
    let active = 0;
    let maxActive = 0;

    const sequential = loopFor({
        intervalMs: 5,
        worker: {
            async runOnce() {
                calls++;
                active++;
                maxActive = Math.max(maxActive, active);

                try {
                    if (calls === 1) {
                        firstEntered.resolve();
                        await finishFirst.promise;
                    } else {
                        await Promise.resolve();
                    }
                } finally {
                    active--;
                }
            },
        },
    });

    assert.equal(sequential.start(), true);

    await within(firstEntered.promise);

    assert.equal(sequential.start(), false);
    assert.equal(calls, 1);

    finishFirst.resolve();

    await until(() => calls >= 3);
    await within(sequential.stop());

    assert.equal(maxActive, 1);
    assert.equal(active, 0);
    assert.equal(sequential.isRunning, false);

    const callsAfterStop = calls;

    await nextTurn();

    assert.equal(calls, callsAfterStop);

    console.log(
        "Background loop starts once and runs sequential batches: OK",
    );

    let attempts = 0;

    const retrying = loopFor({
        intervalMs: 5,
        worker: {
            async runOnce() {
                attempts++;

                if (attempts === 1) {
                    throw new DomainError(
                        "SERVICE_UNAVAILABLE",
                        "Synthetic scan outage",
                    );
                }

                if (attempts === 2) {
                    throw new Error(
                        "Synthetic unexpected scan failure",
                    );
                }
            },
        },
    });

    retrying.start();

    await until(() => attempts >= 3);
    await within(retrying.stop());

    assert.deepEqual(
        logs.map((entry) => entry.data),
        [
            { code: "SERVICE_UNAVAILABLE" },
            { code: "INTERNAL" },
        ],
    );

    console.log(
        "Batch scan failure is logged and retried: OK",
    );

    let idleCalls = 0;

    const idle = loopFor({
        intervalMs: 60_000,
        worker: {
            async runOnce() {
                idleCalls++;
            },
        },
    });

    idle.start();

    await until(() => idleCalls === 1);
    await nextTurn();

    // stop phải đánh thức thời gian nghỉ 60 giây.
    await within(idle.stop());

    assert.equal(idleCalls, 1);
    assert.equal(idle.isRunning, false);

    console.log(
        "Stop interrupts idle wait without another batch: OK",
    );

    const batchEntered = gate();
    const finishBatch = gate();

    let drainCalls = 0;

    const draining = loopFor({
        intervalMs: 60_000,
        worker: {
            async runOnce() {
                drainCalls++;

                if (drainCalls === 1) {
                    batchEntered.resolve();
                    await finishBatch.promise;
                }
            },
        },
    });

    draining.start();

    await within(batchEntered.promise);

    let stopped = false;

    const stopping = draining.stop().then(() => {
        stopped = true;
    });

    await nextTurn();

    assert.equal(stopped, false);
    assert.equal(draining.isRunning, true);
    assert.equal(draining.start(), false);

    finishBatch.resolve();

    await within(stopping);

    assert.equal(stopped, true);
    assert.equal(draining.isRunning, false);
    assert.equal(drainCalls, 1);

    assert.equal(draining.start(), true);

    await until(() => drainCalls === 2);
    await within(draining.stop());

    assert.equal(draining.isRunning, false);
    assert.equal(drainCalls, 2);

    console.log(
        "Stop waits for the active batch and allows restart: OK",
    );

    for (const intervalMs of [
        0,
        -1,
        60_001,
        1.5,
        "1000",
    ]) {
        assert.throws(
            () => createOrderCreationLoop({
                worker: { async runOnce() { } },
                intervalMs,
                logger,
            }),
            (error) =>
                error instanceof DomainError &&
                error.code === "INVALID_ARGUMENT",
        );
    }

    for (const worker of [
        undefined,
        null,
        {},
        { runOnce: true },
    ]) {
        assert.throws(
            () => createOrderCreationLoop({
                worker,
                logger,
            }),
            TypeError,
        );
    }

    const neverStarted = loopFor({
        worker: { async runOnce() { } },
    });

    assert.equal(neverStarted.isRunning, false);

    await neverStarted.stop();
    await neverStarted.stop();

    assert.equal(neverStarted.isRunning, false);

    console.log(
        "Loop options and stopped state validation: OK",
    );
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Creation worker loop check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    try {
        // Giải phóng batch giả lập nếu assertion trước đó lỗi.
        for (const release of releases) {
            release();
        }

        await Promise.all(
            loops.map((loop) => loop.stop()),
        );

        assert.ok(
            loops.every((loop) => !loop.isRunning),
        );

        console.log(
            "Loop fixtures and timers removed: OK",
        );
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Creation worker loop cleanup failed:",
            error.code ?? error.name,
            error.message,
        );
    }
}