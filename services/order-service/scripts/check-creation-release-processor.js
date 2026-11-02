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
    createOrderCreationProcessor,
} from "../src/application/order-creation.processor.js";

const operationIds = [];
const future = new Date(Date.now() + 86_400_000);

function locked(id, operation) {
    return orderCreationTransaction.withLockedTask(
        id,
        operation,
    );
}

function taskOf(id) {
    return orderCreationRepository.findByOperationId(id);
}

function acknowledgment(id) {
    return {
        operationId: id,
        uid: 13,
        status: "RELEASED",
        items: [],
    };
}

async function fixture(status = "RELEASE_PENDING") {
    const id = randomUUID();
    operationIds.push(id);

    await orderCreationRepository.createTask(
        {
            operationId: id,
            uid: 13,
            actorRole: "CUSTOMER",
            items: [{ pid: 1, qty: 2 }],
        },
        { nextAttemptAt: future },
    );

    if (status === "RELEASE_PENDING") {
        await locked(
            id,
            (_, context) => context.requestRelease(
                "NOT_FOUND",
                { nextAttemptAt: future },
            ),
        );
    }

    if (status === "CONFIRM_PENDING") {
        await locked(
            id,
            (_, context) => context.freezeScore(100),
        );

        await locked(
            id,
            (_, context) => context.commitReservedOrder(
                {
                    operationId: id,
                    uid: 13,
                    status: "RESERVED",
                    items: [{
                        pid: 1,
                        qty: 2,
                        unit_price: "25.50",
                    }],
                },
                { nextAttemptAt: future },
            ),
        );
    }

    return id;
}

function processorFor(
    releaseStock,
    transactions = orderCreationTransaction,
) {
    return createOrderCreationProcessor({
        transactions,
        products: { releaseStock },
        customers: {
            async getCustomer() {
                assert.fail("Release phase must not call Customer");
            },
        },
    });
}

async function main() {
    const calls = [];
    const released = new Set();

    async function releaseStock(id, actor) {
        assert.deepEqual(actor, {
            uid: 13,
            role: "CUSTOMER",
        });

        calls.push(id);
        released.add(id);

        return acknowledgment(id);
    }

    const processor = processorFor(releaseStock);

    const firstId = await fixture();

    const first = await processor.releaseFailedCreation(
        firstId,
    );

    assert.equal(first.outcome, "ORDER_FAILED");
    assert.equal(first.status, "FAILED");
    assert.equal(first.failureCode, "NOT_FOUND");

    const closed = await taskOf(firstId);

    assert.equal(closed.status, "FAILED");
    assert.equal(closed.failure_code, "NOT_FOUND");

    const replay = await processor.releaseFailedCreation(
        firstId,
    );

    assert.equal(replay.outcome, "ALREADY_FAILED");
    assert.deepEqual(await taskOf(firstId), closed);

    assert.equal(
        calls.filter((id) => id === firstId).length,
        1,
    );

    const concurrentId = await fixture();

    const concurrent = await Promise.all([
        processor.releaseFailedCreation(concurrentId),
        processor.releaseFailedCreation(concurrentId),
    ]);

    assert.deepEqual(
        concurrent.map((value) => value.outcome).sort(),
        ["ALREADY_FAILED", "ORDER_FAILED"],
    );

    assert.equal(
        calls.filter((id) => id === concurrentId).length,
        1,
    );

    console.log(
        "Released stock closes failure with replay and concurrency: OK",
    );

    for (const status of [
        "PROCESSING",
        "CONFIRM_PENDING",
    ]) {
        const id = await fixture(status);

        assert.equal(
            (
                await processor.releaseFailedCreation(id)
            ).outcome,
            "NOT_RELEASE_PENDING",
        );

        assert.equal(calls.includes(id), false);
    }

    // Cố tình tạo dữ liệu không nhất quán trong fixture:
    // task RELEASE_PENDING nhưng đã có Order.
    const inconsistentId = await fixture();

    await pool.query(
        `
        INSERT INTO orders (
            uid,
            discount,
            operation_id
        )
        VALUES (13, 0, $1::uuid)
        `,
        [inconsistentId],
    );

    const beforeGuard = await taskOf(inconsistentId);

    await assert.rejects(
        () => processor.releaseFailedCreation(inconsistentId),
        (error) =>
            error instanceof DomainError &&
            error.code === "CONFLICT",
    );

    assert.equal(calls.includes(inconsistentId), false);

    assert.deepEqual(
        await taskOf(inconsistentId),
        beforeGuard,
    );

    console.log(
        "Missing release decisions and stored orders block release: OK",
    );

    for (const code of [
        "SERVICE_UNAVAILABLE",
        "NOT_FOUND",
        "CONFLICT",
    ]) {
        const id = await fixture();

        const failing = processorFor(async () => {
            throw new DomainError(
                code,
                "Synthetic release failure",
            );
        });

        const result = await failing.releaseFailedCreation(id);
        const task = await taskOf(id);

        assert.equal(result.outcome, "RETRY_SCHEDULED");
        assert.equal(task.status, "RELEASE_PENDING");
        assert.equal(task.failure_code, "NOT_FOUND");
        assert.equal(task.attempts, 1);

        assert.equal(
            task.last_error_code,
            code === "SERVICE_UNAVAILABLE"
                ? "SERVICE_UNAVAILABLE"
                : "INTERNAL",
        );
    }

    console.log(
        "Release failures preserve the original durable decision: OK",
    );

    for (const kind of ["wrong-uid", "not-released"]) {
        const id = await fixture();

        const invalid = processorFor(async () => {
            const response = acknowledgment(id);

            if (kind === "wrong-uid") {
                response.uid = 14;
            } else {
                response.status = "RESERVED";
                response.items = [{
                    pid: 1,
                    qty: 2,
                    unit_price: "25.50",
                }];
            }

            return response;
        });

        assert.equal(
            (
                await invalid.releaseFailedCreation(id)
            ).outcome,
            "RETRY_SCHEDULED",
        );

        const task = await taskOf(id);

        assert.equal(task.status, "RELEASE_PENDING");
        assert.equal(task.failure_code, "NOT_FOUND");
        assert.equal(task.last_error_code, "INTERNAL");
    }

    console.log(
        "Release acknowledgment identity and status protected: OK",
    );

    const rollbackId = await fixture();
    const beforeRollback = await taskOf(rollbackId);
    const rollbackMarker = new Error("TEST_SQL_ROLLBACK");

    const rollbackProcessor = processorFor(
        releaseStock,
        {
            withLockedTask(id, operation) {
                return locked(
                    id,
                    async (task, context) => {
                        await operation(task, context);
                        throw rollbackMarker;
                    },
                );
            },
        },
    );

    await assert.rejects(
        () => rollbackProcessor.releaseFailedCreation(rollbackId),
        (error) => error === rollbackMarker,
    );

    assert.ok(released.has(rollbackId));

    assert.deepEqual(
        await taskOf(rollbackId),
        beforeRollback,
    );

    assert.equal(
        (
            await processor.releaseFailedCreation(rollbackId)
        ).outcome,
        "ORDER_FAILED",
    );

    const lostResultId = await fixture();
    const lostResultMarker = new Error("TEST_LOST_RESULT");

    const lostResultProcessor = processorFor(
        releaseStock,
        {
            async withLockedTask(id, operation) {
                await locked(id, operation);
                throw lostResultMarker;
            },
        },
    );

    await assert.rejects(
        () => lostResultProcessor.releaseFailedCreation(lostResultId),
        (error) => error === lostResultMarker,
    );

    assert.equal(
        (await taskOf(lostResultId)).status,
        "FAILED",
    );

    assert.equal(
        (
            await processor.releaseFailedCreation(lostResultId)
        ).outcome,
        "ALREADY_FAILED",
    );

    assert.equal(
        calls.filter((id) => id === lostResultId).length,
        1,
    );

    console.log(
        "Released stock survives SQL rollback and lost result: OK",
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
            "Scoped release processor fixtures removed: OK",
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
        "Creation release processor check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    try {
        await cleanup();
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Creation release processor cleanup failed:",
            error.code ?? error.name,
            error.message,
        );
    } finally {
        customerClient.close();
        productClient.close();
        await pool.end();
    }
}