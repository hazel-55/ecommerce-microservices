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
    createOrderCreationProcessor,
} from "../src/application/order-creation.processor.js";

const operationIds = [];
const future = new Date(Date.now() + 86_400_000);

async function fixture() {
    const operationId = randomUUID();
    operationIds.push(operationId);

    await orderCreationRepository.createTask(
        {
            operationId,
            uid: 13,
            actorRole: "CUSTOMER",
            items: [{ pid: 1, qty: 2 }],
        },
        { nextAttemptAt: future },
    );

    return operationId;
}

function taskOf(operationId) {
    return orderCreationRepository.findByOperationId(
        operationId,
    );
}

function processorFor(
    getCustomer,
    transactions = orderCreationTransaction,
) {
    return createOrderCreationProcessor({
        transactions,
        customers: { getCustomer },
    });
}

async function main() {
    let calls = 0;

    const processor = processorFor(
        async (uid, actor) => {
            calls++;

            assert.equal(uid, 13);
            assert.deepEqual(actor, {
                uid: 13,
                role: "CUSTOMER",
            });

            return { uid: 13, score: 100 };
        },
    );

    const firstId = await fixture();

    const first = await processor.freezeCustomerScore(
        firstId,
    );

    assert.equal(first.outcome, "SCORE_FROZEN");
    assert.equal(first.status, "PROCESSING");

    const stored = await taskOf(firstId);

    assert.equal(stored.score, 100);
    assert.equal(stored.attempts, 0);

    const orders = await pool.query(
        `
        SELECT COUNT(*) AS count
        FROM orders
        WHERE operation_id = $1::uuid
        `,
        [firstId],
    );

    assert.equal(Number(orders.rows[0].count), 0);

    console.log(
        "Customer score frozen before stock processing: OK",
    );

    const replay = await processor.freezeCustomerScore(
        firstId,
    );

    assert.equal(replay.outcome, "ALREADY_FROZEN");
    assert.equal(calls, 1);

    const concurrentId = await fixture();

    const concurrent = await Promise.all([
        processor.freezeCustomerScore(concurrentId),
        processor.freezeCustomerScore(concurrentId),
    ]);

    assert.deepEqual(
        concurrent.map((result) => result.outcome).sort(),
        ["ALREADY_FROZEN", "SCORE_FROZEN"],
    );

    assert.equal(calls, 2);
    assert.equal(
        (await taskOf(concurrentId)).score,
        100,
    );

    console.log(
        "Frozen score replay and concurrent processing: OK",
    );

    for (const failureCode of [
        "NOT_FOUND",
        "FORBIDDEN",
    ]) {
        const id = await fixture();
        let failureCalls = 0;

        const failing = processorFor(async () => {
            failureCalls++;

            throw new DomainError(
                failureCode,
                "Synthetic customer failure",
            );
        });

        const result = await failing.freezeCustomerScore(id);
        const task = await taskOf(id);

        assert.equal(result.outcome, "RELEASE_PENDING");
        assert.equal(task.status, "RELEASE_PENDING");
        assert.equal(task.failure_code, failureCode);
        assert.equal(task.score, null);
        assert.equal(task.attempts, 0);

        const repeat = await failing.freezeCustomerScore(id);

        assert.equal(repeat.outcome, "NOT_PROCESSING");
        assert.equal(failureCalls, 1);
        assert.deepEqual(await taskOf(id), task);
    }

    // Task đã đóng cũng không gọi Customer.
    const closedId = await fixture();

    await orderCreationTransaction.withLockedTask(
        closedId,
        (_, context) => context.requestRelease("NOT_FOUND"),
    );

    await orderCreationTransaction.withLockedTask(
        closedId,
        (_, context) => context.finishRelease({
            operationId: closedId,
            uid: 13,
            status: "RELEASED",
            items: [],
        }),
    );

    const closedProcessor = processorFor(async () => {
        assert.fail("Closed task must not call Customer");
    });

    assert.equal(
        (
            await closedProcessor.freezeCustomerScore(
                closedId,
            )
        ).outcome,
        "NOT_PROCESSING",
    );

    console.log(
        "Permanent customer failures require durable release: OK",
    );

    const failures = [
        {
            call: async () => {
                throw new DomainError(
                    "SERVICE_UNAVAILABLE",
                    "Synthetic unavailable",
                );
            },
            code: "SERVICE_UNAVAILABLE",
        },
        {
            call: async () => {
                throw new Error("Synthetic unknown failure");
            },
            code: "INTERNAL",
        },
        {
            call: async () => null,
            code: "INTERNAL",
        },
        {
            call: async () => ({ uid: 14, score: 100 }),
            code: "INTERNAL",
        },
        {
            call: async () => ({ uid: 13, score: "100" }),
            code: "INTERNAL",
        },
        {
            call: async () => ({ uid: 13, score: -1 }),
            code: "INTERNAL",
        },
        {
            call: async () => ({
                uid: 13,
                score: 2147483648,
            }),
            code: "INTERNAL",
        },
    ];

    for (const scenario of failures) {
        const id = await fixture();

        const failing = processorFor(scenario.call);

        const result = await failing.freezeCustomerScore(id);
        const task = await taskOf(id);

        assert.equal(result.outcome, "RETRY_SCHEDULED");
        assert.equal(task.status, "PROCESSING");
        assert.equal(task.score, null);
        assert.equal(task.failure_code, null);
        assert.equal(task.attempts, 1);
        assert.equal(
            task.last_error_code,
            scenario.code,
        );
        assert.ok(
            task.next_attempt_at instanceof Date,
        );
    }

    console.log(
        "Transient and invalid customer responses schedule retry: OK",
    );

    const rollbackId = await fixture();
    const beforeRollback = await taskOf(rollbackId);
    const rollbackMarker = new Error("TEST_SQL_ROLLBACK");

    const rollbackTransactions = {
        withLockedTask(operationId, operation) {
            return orderCreationTransaction.withLockedTask(
                operationId,
                async (task, context) => {
                    await operation(task, context);

                    throw rollbackMarker;
                },
            );
        },
    };

    const rollbackProcessor = processorFor(
        async () => ({ uid: 13, score: 100 }),
        rollbackTransactions,
    );

    await assert.rejects(
        () => rollbackProcessor.freezeCustomerScore(
            rollbackId,
        ),
        (error) => error === rollbackMarker,
    );

    assert.deepEqual(
        await taskOf(rollbackId),
        beforeRollback,
    );

    // Giả lập caller mất kết quả sau khi SQL đã commit.
    const lostResultId = await fixture();
    const lostResultMarker = new Error("TEST_LOST_RESULT");
    let lostResultCalls = 0;

    const lostResultTransactions = {
        async withLockedTask(operationId, operation) {
            await orderCreationTransaction.withLockedTask(
                operationId,
                operation,
            );

            throw lostResultMarker;
        },
    };

    const lostResultProcessor = processorFor(
        async () => {
            lostResultCalls++;

            return { uid: 13, score: 500 };
        },
        lostResultTransactions,
    );

    await assert.rejects(
        () => lostResultProcessor.freezeCustomerScore(
            lostResultId,
        ),
        (error) => error === lostResultMarker,
    );

    assert.equal(
        (await taskOf(lostResultId)).score,
        500,
    );

    const recoveredProcessor = processorFor(async () => {
        assert.fail("Committed score must be reused");
    });

    const recovered =
        await recoveredProcessor.freezeCustomerScore(
            lostResultId,
        );

    assert.equal(recovered.outcome, "ALREADY_FROZEN");
    assert.equal(lostResultCalls, 1);

    console.log(
        "Task transaction rollback and lost result recovery: OK",
    );
}

async function cleanup() {
    if (operationIds.length === 0) {
        return;
    }

    // Score phase không tạo Order hoặc Outbox.
    const orders = await pool.query(
        `
        SELECT COUNT(*) AS count
        FROM orders
        WHERE operation_id = ANY($1::uuid[])
        `,
        [operationIds],
    );

    assert.equal(Number(orders.rows[0].count), 0);

    await pool.query(
        `
        DELETE FROM order_creation_tasks
        WHERE operation_id = ANY($1::uuid[])
        `,
        [operationIds],
    );

    const remaining = await pool.query(
        `
        SELECT COUNT(*) AS count
        FROM order_creation_tasks
        WHERE operation_id = ANY($1::uuid[])
        `,
        [operationIds],
    );

    assert.equal(Number(remaining.rows[0].count), 0);

    console.log(
        "Scoped score processor fixtures removed: OK",
    );
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Creation score processor check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    try {
        await cleanup();
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Creation score processor cleanup failed:",
            error.code ?? error.name,
            error.message,
        );
    } finally {
        customerClient.close();
        await pool.end();
    }
}