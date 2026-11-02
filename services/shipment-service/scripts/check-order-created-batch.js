import assert from "node:assert/strict";
import { DomainError } from "../src/domain/errors.js";
import {
    createOrderCreatedBatchHandler,
} from "../src/infrastructure/kafka/order-created.batch.js";
import {
    orderCreatedTopic,
} from "../src/infrastructure/kafka/topics.js";

function fixture({
    offsets = ["0", "1", "2"],
    handle = async () => ({ outcome: "CREATED" }),
    commit = async () => { },
    isRunning = () => true,
    isStale = () => false,
} = {}) {
    const calls = {
        handled: [],
        commits: [],
        resolved: [],
        trace: [],
    };

    const process = createOrderCreatedBatchHandler({
        async handler(record) {
            calls.handled.push(record.message.offset);

            const result = await handle(record);

            calls.trace.push(
                `handled:${record.message.offset}`
            );

            return result;
        },

        async commitOffsets(offsets) {
            await commit(offsets);

            calls.commits.push(structuredClone(offsets));
            calls.trace.push(`commit:${offsets[0].offset}`);
        },
    });

    const payload = {
        batch: {
            topic: orderCreatedTopic,
            partition: 0,
            messages: offsets.map((offset) => ({
                offset,
                key: Buffer.from("1001"),
                value: Buffer.from("{}"),
            })),
        },

        resolveOffset(offset) {
            calls.resolved.push(offset);
            calls.trace.push(`resolve:${offset}`);
        },

        async heartbeat() { },

        isRunning,
        isStale,
    };

    return { process, payload, calls };
}

async function waitFor(promise) {
    let timer;

    try {
        return await Promise.race([
            promise,
            new Promise((resolve, reject) => {
                timer = setTimeout(
                    () => reject(new Error("Test timed out")),
                    5000
                );
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}

function isInternal(error) {
    assert.ok(error instanceof DomainError);
    assert.equal(error.code, "INTERNAL");
    return true;
}

async function main() {
    const enteredHandler = Promise.withResolvers();
    const finishPersistence = Promise.withResolvers();
    const enteredCommit = Promise.withResolvers();
    const finishCommit = Promise.withResolvers();

    const first = fixture({
        async handle({ message }) {
            if (message.offset === "0") {
                enteredHandler.resolve();
                await finishPersistence.promise;
            }

            return {
                outcome: {
                    0: "CREATED",
                    1: "DUPLICATE",
                    2: "REJECTED",
                }[message.offset],
            };
        },

        async commit(offsets) {
            if (offsets[0].offset === "1") {
                enteredCommit.resolve();
                await finishCommit.promise;
            }
        },
    });

    const processing = first.process(first.payload);

    // Make early processing failures visible to both waiting gates.
    void processing.catch((error) => {
        enteredHandler.reject(error);
        enteredCommit.reject(error);
    });

    try {
        await waitFor(enteredHandler.promise);

        assert.deepEqual(first.calls.commits, []);
        assert.deepEqual(first.calls.resolved, []);

        finishPersistence.resolve();

        await waitFor(enteredCommit.promise);

        // Commit was requested, but its acknowledgment is still pending.
        assert.deepEqual(first.calls.resolved, []);

        finishCommit.resolve();
        await processing;
    } finally {
        finishPersistence.resolve();
        finishCommit.resolve();
        await processing;
    }

    assert.deepEqual(first.calls.handled, ["0", "1", "2"]);
    assert.deepEqual(first.calls.resolved, ["0", "1", "2"]);

    assert.deepEqual(
        first.calls.commits.map((offsets) => offsets[0]),
        ["1", "2", "3"].map((offset) => ({
            topic: orderCreatedTopic,
            partition: 0,
            offset,
        }))
    );

    assert.deepEqual(first.calls.trace, [
        "handled:0", "commit:1", "resolve:0",
        "handled:1", "commit:2", "resolve:1",
        "handled:2", "commit:3", "resolve:2",
    ]);

    console.log(
        "Offsets committed only after completed message handling: OK"
    );

    const large = fixture({
        offsets: ["9007199254740993"],
    });

    await large.process(large.payload);

    assert.equal(
        large.calls.commits[0][0].offset,
        "9007199254740994"
    );

    console.log(
        "Large Kafka offsets advanced without precision loss: OK"
    );

    const persistenceError = new DomainError(
        "CONFLICT",
        "Shipment ID limit reached"
    );

    const failedHandler = fixture({
        async handle({ message }) {
            if (message.offset === "1") {
                throw persistenceError;
            }

            return { outcome: "CREATED" };
        },
    });

    await assert.rejects(
        () => failedHandler.process(failedHandler.payload),
        (error) => error === persistenceError
    );

    assert.deepEqual(
        failedHandler.calls.handled,
        ["0", "1"]
    );

    assert.deepEqual(
        failedHandler.calls.resolved,
        ["0"]
    );

    assert.deepEqual(
        failedHandler.calls.commits.map(
            (offsets) => offsets[0].offset
        ),
        ["1"]
    );

    console.log(
        "Handler failure keeps failed and later messages uncommitted: OK"
    );

    const commitError = new Error("Commit unavailable");

    const failedCommit = fixture({
        async commit() {
            throw commitError;
        },
    });

    await assert.rejects(
        () => failedCommit.process(failedCommit.payload),
        (error) => error === commitError
    );

    assert.deepEqual(failedCommit.calls.handled, ["0"]);
    assert.deepEqual(failedCommit.calls.resolved, []);

    // Simulate replay of a message that was already persisted.
    const replay = fixture({
        offsets: ["0"],
        handle: async () => ({ outcome: "DUPLICATE" }),
    });

    await replay.process(replay.payload);
    assert.deepEqual(replay.calls.resolved, ["0"]);

    console.log(
        "Commit failure propagates and does not resolve local offset: OK"
    );

    let running = true;

    const stopping = fixture({
        isRunning: () => running,

        async handle() {
            running = false;
            return { outcome: "CREATED" };
        },
    });

    await stopping.process(stopping.payload);

    assert.deepEqual(stopping.calls.handled, ["0"]);
    assert.deepEqual(stopping.calls.resolved, ["0"]);

    for (const options of [
        { isRunning: () => false },
        { isStale: () => true },
    ]) {
        const skipped = fixture(options);

        await skipped.process(skipped.payload);

        assert.deepEqual(skipped.calls.handled, []);
        assert.deepEqual(skipped.calls.commits, []);
    }

    let stale = false;

    const staleAfterSave = fixture({
        isStale: () => stale,

        async handle() {
            stale = true;
            return { outcome: "CREATED" };
        },
    });

    await staleAfterSave.process(staleAfterSave.payload);

    assert.deepEqual(staleAfterSave.calls.handled, ["0"]);
    assert.deepEqual(staleAfterSave.calls.commits, []);
    assert.deepEqual(staleAfterSave.calls.resolved, []);

    console.log(
        "Stop and stale batches preserve processing boundaries: OK"
    );

    const unknown = fixture({
        handle: async () => ({ outcome: "UNKNOWN" }),
    });

    await assert.rejects(
        () => unknown.process(unknown.payload),
        isInternal
    );

    assert.deepEqual(unknown.calls.commits, []);
    assert.deepEqual(unknown.calls.resolved, []);

    const invalidCallback = fixture();
    delete invalidCallback.payload.heartbeat;

    await assert.rejects(
        () => invalidCallback.process(invalidCallback.payload),
        isInternal
    );

    assert.deepEqual(invalidCallback.calls.handled, []);

    const invalidOffset = fixture({ offsets: ["01"] });

    await assert.rejects(
        () => invalidOffset.process(invalidOffset.payload),
        isInternal
    );

    assert.deepEqual(invalidOffset.calls.handled, []);

    assert.throws(
        () => createOrderCreatedBatchHandler({
            handler: null,
            commitOffsets: async () => { },
        }),
        TypeError
    );

    assert.throws(
        () => createOrderCreatedBatchHandler({
            handler: async () => ({ outcome: "CREATED" }),
            commitOffsets: null,
        }),
        TypeError
    );

    console.log(
        "Unknown outcomes and invalid batch callbacks rejected: OK"
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "OrderCreated batch check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
}