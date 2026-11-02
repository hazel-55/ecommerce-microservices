import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    createOutboxWorker,
} from "../src/application/outbox.worker.js";

const silentLogger = {
    error() { },
};

const successfulPublisher = {
    async publishEvent(eventId) {
        return {
            handled: true,
            result: {
                eventId,
                outcome: "PUBLISHED",
            },
        };
    },
};

const emptyStats = {
    scanned: 0,
    published: 0,
    retried: 0,
    skipped: 0,
    failed: 0,
};

async function withDeadline(promise) {
    let timer;

    try {
        return await Promise.race([
            promise,
            new Promise((resolve, reject) => {
                timer = setTimeout(
                    () => reject(new Error("Worker check timed out")),
                    5000
                );
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}

async function main() {
    const ids = Array.from({ length: 5 }, () => randomUUID());
    const seen = [];
    const logs = [];

    let active = 0;
    let maxActive = 0;

    const worker = createOutboxWorker({
        batchSize: 3,

        repository: {
            async listDueEventIds({ limit }) {
                assert.equal(limit, 3);
                return ids.slice(0, limit);
            },
        },

        publisher: {
            async publishEvent(eventId) {
                seen.push(eventId);
                active += 1;
                maxActive = Math.max(maxActive, active);

                try {
                    await Promise.resolve();

                    if (eventId === ids[0]) {
                        const error = new Error(
                            "Simulated storage detail"
                        );

                        error.code = "08006";
                        throw error;
                    }

                    return {
                        handled: true,
                        result: {
                            eventId,
                            outcome: eventId === ids[1]
                                ? "PUBLISHED"
                                : "RETRY_SCHEDULED",
                        },
                    };
                } finally {
                    active -= 1;
                }
            },
        },

        logger: {
            error(fields) {
                logs.push(fields);
            },
        },
    });

    assert.deepEqual(await worker.runOnce(), {
        scanned: 3,
        published: 1,
        retried: 1,
        skipped: 0,
        failed: 1,
    });

    assert.deepEqual(seen, ids.slice(0, 3));
    assert.equal(maxActive, 1);
    assert.equal(logs.length, 1);
    assert.equal(logs[0].code, "08006");

    assert.equal(
        JSON.stringify(logs).includes("Simulated storage detail"),
        false
    );

    console.log(
        "Bounded sequential batch continues after event failure: OK"
    );

    const outcomeWorker = createOutboxWorker({
        batchSize: 2,
        logger: silentLogger,

        repository: {
            async listDueEventIds() {
                return ids.slice(3);
            },
        },

        publisher: {
            async publishEvent(eventId) {
                if (eventId === ids[3]) {
                    return { handled: false };
                }

                return {
                    handled: true,
                    result: {
                        eventId,
                        outcome: "UNKNOWN",
                    },
                };
            },
        },
    });

    assert.deepEqual(await outcomeWorker.runOnce(), {
        scanned: 2,
        published: 0,
        retried: 0,
        skipped: 1,
        failed: 1,
    });

    console.log("Worker outcome statistics and invalid results: OK");

    const entered = Promise.withResolvers();
    const release = Promise.withResolvers();

    let scans = 0;

    const overlapWorker = createOutboxWorker({
        batchSize: 1,
        logger: silentLogger,

        repository: {
            async listDueEventIds() {
                scans += 1;
                return [ids[0]];
            },
        },

        publisher: {
            async publishEvent(eventId) {
                entered.resolve();
                await release.promise;

                return successfulPublisher.publishEvent(eventId);
            },
        },
    });

    const first = overlapWorker.runOnce();
    void first.catch(entered.reject);

    try {
        await withDeadline(entered.promise);

        assert.equal(await overlapWorker.runOnce(), null);
        assert.equal(scans, 1);
    } finally {
        release.resolve();
        await first;
    }

    assert.equal(
        (await overlapWorker.runOnce()).published,
        1
    );
    assert.equal(scans, 2);

    console.log("Overlapping worker batches are blocked: OK");

    let scanMode = "throw";
    let publishCalls = 0;

    const recoveryWorker = createOutboxWorker({
        batchSize: 1,
        logger: silentLogger,

        repository: {
            async listDueEventIds() {
                if (scanMode === "throw") {
                    throw new Error("Forced scan failure");
                }

                if (scanMode === "oversized") {
                    return ids.slice(0, 2);
                }

                return [];
            },
        },

        publisher: {
            async publishEvent(eventId) {
                publishCalls += 1;
                return successfulPublisher.publishEvent(eventId);
            },
        },
    });

    await assert.rejects(
        () => recoveryWorker.runOnce(),
        /Forced scan failure/
    );

    scanMode = "oversized";

    await assert.rejects(
        () => recoveryWorker.runOnce(),
        /Invalid Outbox candidate batch/
    );

    scanMode = "empty";

    assert.deepEqual(
        await recoveryWorker.runOnce(),
        emptyStats
    );

    assert.equal(publishCalls, 0);

    console.log("Scan failure and invalid batch reset worker state: OK");

    for (const batchSize of [
        0, -1, 101, 1.5, "2", null, NaN,
    ]) {
        assert.throws(
            () => createOutboxWorker({
                batchSize,
                publisher: successfulPublisher,
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
        () => createOutboxWorker({
            publisher: {},
            logger: silentLogger,
        }),
        TypeError
    );

    console.log("Worker batch options and dependencies validated: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Outbox worker batch check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    await pool.end();
}