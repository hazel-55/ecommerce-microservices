import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    orderCreationRepository,
} from "../src/infrastructure/repositories/order-creation.repository.js";
import {
    orderCreationTransaction,
} from "../src/infrastructure/database/order-creation.transaction.js";
import {
    customerClient,
} from "../src/infrastructure/grpc/customer.client.js";
import {
    productClient,
} from "../src/infrastructure/grpc/product.client.js";
import {
    createOrderCreationDispatcher,
} from "../src/application/order-creation.dispatcher.js";
import {
    createOrderCreationWorker,
} from "../src/application/order-creation.worker.js";

const operationIds = [];
const past = new Date("2000-01-01T00:00:00.000Z");
const future = new Date(Date.now() + 86_400_000);
const logs = [];

const logger = {
    error(data, message) {
        logs.push({
            data: structuredClone(data),
            message,
        });
    },
};

function locked(id, operation) {
    return orderCreationTransaction.withLockedTask(
        id,
        operation,
    );
}

function taskOf(id) {
    return orderCreationRepository.findByOperationId(id);
}

async function fixture(nextAttemptAt = past) {
    const id = randomUUID();
    operationIds.push(id);

    await orderCreationRepository.createTask(
        {
            operationId: id,
            uid: 13,
            actorRole: "CUSTOMER",
            items: [{ pid: 1, qty: 1 }],
        },
        { nextAttemptAt },
    );

    return id;
}

function candidates(ids) {
    return {
        async listDueOperationIds({ limit }) {
            return ids.slice(0, limit);
        },
    };
}

const dispatcher = createOrderCreationDispatcher({
    customers: {
        async getCustomer(uid) {
            return { uid, score: 100 };
        },
    },
    products: {
        async reserveStock() {
            assert.fail("This check only dispatches score phases");
        },
        async confirmStock() {
            assert.fail("Unexpected confirmation");
        },
        async releaseStock() {
            assert.fail("Unexpected release");
        },
    },
});

async function orderSnapshot(id) {
    const result = await pool.query(
        `
        SELECT
            to_jsonb(o) AS header,
            (
                SELECT jsonb_agg(to_jsonb(d) ORDER BY d.pid)
                FROM order_details d
                WHERE d.oid = o.oid
            ) AS details,
            (
                SELECT jsonb_agg(
                    to_jsonb(e) ORDER BY e.event_id
                )
                FROM outbox_events e
                WHERE e.oid = o.oid
            ) AS events
        FROM orders o
        WHERE o.operation_id = $1::uuid
        `,
        [id],
    );

    return result.rows;
}

async function main() {
    const failedId = await fixture();
    const goodId = await fixture();
    const outsideBatchId = await fixture();

    const visited = [];
    let active = 0;
    let maxActive = 0;

    const worker = createOrderCreationWorker({
        repository: candidates([
            failedId,
            goodId,
            outsideBatchId,
        ]),
        dispatcher: {
            async processDueTask(id) {
                visited.push(id);
                active++;
                maxActive = Math.max(maxActive, active);

                try {
                    await Promise.resolve();

                    if (id === failedId) {
                        throw new DomainError(
                            "SERVICE_UNAVAILABLE",
                            "Synthetic processing failure",
                        );
                    }

                    return await dispatcher.processDueTask(id);
                } finally {
                    active--;
                }
            },
        },
        logger,
        batchSize: 2,
    });

    assert.deepEqual(await worker.runOnce(), {
        scanned: 2,
        handled: 1,
        skipped: 0,
        failed: 1,
    });

    assert.deepEqual(visited, [failedId, goodId]);
    assert.equal(maxActive, 1);

    const failed = await taskOf(failedId);

    assert.equal(failed.status, "PROCESSING");
    assert.equal(failed.score, null);
    assert.equal(failed.attempts, 1);
    assert.equal(
        failed.last_error_code,
        "SERVICE_UNAVAILABLE",
    );

    assert.equal((await taskOf(goodId)).score, 100);
    assert.equal(
        (await taskOf(outsideBatchId)).score,
        null,
    );

    console.log(
        "Bounded worker batch continues after task failure: OK",
    );

    const storedId = await fixture();

    await locked(
        storedId,
        (_, context) => context.freezeScore(100),
    );

    await locked(
        storedId,
        (_, context) => context.commitReservedOrder(
            {
                operationId: storedId,
                uid: 13,
                status: "RESERVED",
                items: [{
                    pid: 1,
                    qty: 1,
                    unit_price: "10.00",
                }],
            },
            { nextAttemptAt: future },
        ),
    );

    // Đưa riêng fixture về đến hạn để kiểm tra recovery.
    await pool.query(
        `
        UPDATE order_creation_tasks
        SET next_attempt_at = $2::timestamptz
        WHERE operation_id = $1::uuid
        `,
        [storedId, past],
    );

    const storedBefore = await orderSnapshot(storedId);

    const storedWorker = createOrderCreationWorker({
        repository: candidates([storedId]),
        dispatcher: {
            async processDueTask() {
                throw new Error("Synthetic unexpected failure");
            },
        },
        logger,
    });

    await storedWorker.runOnce();

    const storedTask = await taskOf(storedId);

    assert.equal(storedTask.status, "CONFIRM_PENDING");
    assert.equal(storedTask.score, 100);
    assert.equal(storedTask.failure_code, null);
    assert.equal(storedTask.attempts, 1);
    assert.equal(storedTask.last_error_code, "INTERNAL");

    assert.deepEqual(
        await orderSnapshot(storedId),
        storedBefore,
    );

    console.log(
        "Worker retry preserves stored order and task state: OK",
    );

    const futureId = await fixture(future);
    const closedId = await fixture();

    await locked(
        closedId,
        (_, context) => context.requestRelease("NOT_FOUND"),
    );

    await locked(
        closedId,
        (_, context) => context.finishRelease({
            operationId: closedId,
            uid: 13,
            status: "RELEASED",
            items: [],
        }),
    );

    const futureBefore = await taskOf(futureId);
    const closedBefore = await taskOf(closedId);

    const guardedWorker = createOrderCreationWorker({
        repository: candidates([futureId, closedId]),
        dispatcher: {
            async processDueTask() {
                throw new Error("Synthetic stale candidate failure");
            },
        },
        logger,
    });

    await guardedWorker.runOnce();

    assert.deepEqual(
        await taskOf(futureId),
        futureBefore,
    );

    assert.deepEqual(
        await taskOf(closedId),
        closedBefore,
    );

    console.log(
        "Retry recovery preserves future and closed tasks: OK",
    );

    const brokenRecoveryId = await fixture();
    const nextGoodId = await fixture();

    const recoveryWorker = createOrderCreationWorker({
        repository: candidates([
            brokenRecoveryId,
            nextGoodId,
        ]),
        dispatcher: {
            async processDueTask(id) {
                if (id === brokenRecoveryId) {
                    throw new Error("Synthetic task failure");
                }

                return dispatcher.processDueTask(id);
            },
        },
        transactions: {
            withLockedTask(id, operation) {
                if (id === brokenRecoveryId) {
                    throw new Error("Synthetic recovery DB failure");
                }

                return locked(id, operation);
            },
        },
        logger,
    });

    assert.deepEqual(await recoveryWorker.runOnce(), {
        scanned: 2,
        handled: 1,
        skipped: 0,
        failed: 1,
    });

    assert.equal((await taskOf(nextGoodId)).score, 100);

    // Recovery thất bại nên không được giả là đã lưu retry.
    assert.equal(
        (await taskOf(brokenRecoveryId)).attempts,
        0,
    );

    for (const entry of logs) {
        assert.deepEqual(
            Object.keys(entry.data).sort(),
            ["code", "operationId"],
        );

        assert.ok(
            ["INTERNAL", "SERVICE_UNAVAILABLE"]
                .includes(entry.data.code),
        );
    }

    console.log(
        "Retry persistence failure does not stop other tasks: OK",
    );

    let finishScan;
    let scanCalls = 0;

    const scanGate = new Promise((resolve) => {
        finishScan = resolve;
    });

    const overlapWorker = createOrderCreationWorker({
        repository: {
            async listDueOperationIds() {
                scanCalls++;
                return scanGate;
            },
        },
        logger,
    });

    const firstRun = overlapWorker.runOnce();

    assert.equal(await overlapWorker.runOnce(), null);
    assert.equal(scanCalls, 1);

    finishScan([]);

    assert.deepEqual(await firstRun, {
        scanned: 0,
        handled: 0,
        skipped: 0,
        failed: 0,
    });

    let scans = 0;
    const scanMarker = new Error("TEST_SCAN_FAILURE");

    const scanFailureWorker = createOrderCreationWorker({
        repository: {
            async listDueOperationIds() {
                scans++;

                if (scans === 1) {
                    throw scanMarker;
                }

                return [];
            },
        },
        logger,
    });

    await assert.rejects(
        () => scanFailureWorker.runOnce(),
        (error) => error === scanMarker,
    );

    assert.deepEqual(await scanFailureWorker.runOnce(), {
        scanned: 0,
        handled: 0,
        skipped: 0,
        failed: 0,
    });

    for (const batchSize of [0, 101, 1.5, "20"]) {
        assert.throws(
            () => createOrderCreationWorker({
                batchSize,
                logger,
            }),
            (error) =>
                error instanceof DomainError &&
                error.code === "INVALID_ARGUMENT",
        );
    }

    console.log(
        "Worker overlap guard and scan failure reset: OK",
    );
}

async function cleanup() {
    if (operationIds.length === 0) {
        return;
    }

    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        const selected = await client.query(
            `
            SELECT oid FROM orders
            WHERE operation_id = ANY($1::uuid[])
            `,
            [operationIds],
        );

        const orderIds = selected.rows.map((row) => row.oid);

        await client.query(
            `DELETE FROM outbox_events
             WHERE oid = ANY($1::integer[])`,
            [orderIds],
        );

        await client.query(
            `DELETE FROM order_details
             WHERE oid = ANY($1::integer[])`,
            [orderIds],
        );

        await client.query(
            `DELETE FROM orders
             WHERE operation_id = ANY($1::uuid[])`,
            [operationIds],
        );

        await client.query(
            `DELETE FROM order_creation_tasks
             WHERE operation_id = ANY($1::uuid[])`,
            [operationIds],
        );

        const remaining = await client.query(
            `
            SELECT
                (SELECT COUNT(*) FROM order_creation_tasks
                 WHERE operation_id = ANY($1::uuid[]))
                +
                (SELECT COUNT(*) FROM orders
                 WHERE operation_id = ANY($1::uuid[]))
                +
                (SELECT COUNT(*) FROM order_details
                 WHERE oid = ANY($2::integer[]))
                +
                (SELECT COUNT(*) FROM outbox_events
                 WHERE oid = ANY($2::integer[]))
                AS count
            `,
            [operationIds, orderIds],
        );

        assert.equal(Number(remaining.rows[0].count), 0);

        await client.query("COMMIT");

        console.log(
            "Scoped worker batch fixtures removed: OK",
        );
    } catch (error) {
        await client.query("ROLLBACK");
        throw error;
    } finally {
        client.release();
    }
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Creation worker batch check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    try {
        await cleanup();
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Creation worker batch cleanup failed:",
            error.code ?? error.name,
            error.message,
        );
    } finally {
        customerClient.close();
        productClient.close();
        await pool.end();
    }
}