import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import { DomainError } from "../src/domain/errors.js";
import {
    orderCreationRepository,
} from "../src/infrastructure/repositories/order-creation.repository.js";
import {
    createOrderCreationTransaction,
} from "../src/infrastructure/database/order-creation.transaction.js";

const operationIds = [];

const schedule = {
    nextAttemptAt: new Date(Date.now() + 24 * 60 * 60 * 1000),
};

const transactions = createOrderCreationTransaction();
const otherTransactions = createOrderCreationTransaction();

async function createFixture() {
    const operationId = randomUUID();
    operationIds.push(operationId);

    await orderCreationRepository.createTask(
        {
            operationId,
            uid: 13,
            actorRole: "CUSTOMER",
            items: [{ pid: 1, qty: 1 }],
        },
        schedule
    );

    return operationId;
}

async function expectDomainError(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    const firstId = await createFixture();
    let closedContext;

    const frozenScore = await transactions.withLockedTask(
        firstId,
        async (task, context) => {
            assert.equal(task.score, null);
            closedContext = context;

            // Pool supplies another connection while this task is locked.
            await assert.rejects(
                () => pool.query(
                    `
                    SELECT operation_id
                    FROM order_creation_tasks
                    WHERE operation_id = $1
                    FOR UPDATE NOWAIT
                    `,
                    [firstId]
                ),
                (error) => {
                    assert.equal(error.code, "55P03");
                    return true;
                }
            );

            assert.equal(await context.freezeScore(100), 100);
            assert.equal(await context.freezeScore(500), 100);

            return 100;
        }
    );

    assert.equal(frozenScore, 100);

    assert.equal(
        (await orderCreationRepository.findByOperationId(firstId))
            .score,
        100
    );

    console.log("Task row lock and immutable score snapshot: OK");

    const parallelId = await createFixture();
    const observedScores = [];

    const candidates = [100, 500];
    const runners = [transactions, otherTransactions];

    const results = await Promise.allSettled(
        candidates.map((candidate, index) =>
            runners[index].withLockedTask(
                parallelId,
                async (task, context) => {
                    observedScores.push(task.score);

                    return context.freezeScore(candidate);
                }
            )
        )
    );

    for (const result of results) {
        if (result.status === "rejected") {
            throw result.reason;
        }
    }

    const winner = results[0].value;

    assert.ok([100, 500].includes(winner));
    assert.equal(results[1].value, winner);

    assert.equal(
        observedScores.filter((score) => score === null).length,
        1
    );

    assert.equal(
        observedScores.filter((score) => score === winner).length,
        1
    );

    assert.equal(
        (await orderCreationRepository.findByOperationId(parallelId))
            .score,
        winner
    );

    console.log("Concurrent score freeze keeps first committed value: OK");

    const rollbackId = await createFixture();
    const simulatedFailure = new Error(
        "Simulated task transaction failure"
    );

    await assert.rejects(
        () => transactions.withLockedTask(
            rollbackId,
            async (task, context) => {
                assert.equal(task.score, null);

                await context.freezeScore(500);

                throw simulatedFailure;
            }
        ),
        (error) => error === simulatedFailure
    );

    const rolledBack =
        await orderCreationRepository.findByOperationId(rollbackId);

    assert.equal(rolledBack.score, null);
    assert.equal(rolledBack.status, "PROCESSING");

    console.log("Task transaction failure rolls back score changes: OK");

    let missingCallbackCalled = false;

    await expectDomainError(
        () => transactions.withLockedTask(
            randomUUID(),
            async () => {
                missingCallbackCalled = true;
            }
        ),
        "NOT_FOUND"
    );

    assert.equal(missingCallbackCalled, false);

    await expectDomainError(
        () => transactions.withLockedTask(
            "invalid",
            async () => { }
        ),
        "INVALID_ARGUMENT"
    );

    await expectDomainError(
        () => closedContext.freezeScore(500),
        "INTERNAL"
    );

    // Change only this test fixture to a terminal state.
    await pool.query(
        `
        UPDATE order_creation_tasks
        SET status = 'FAILED',
            failure_code = 'TEST_FIXTURE'
        WHERE operation_id = $1
        `,
        [rollbackId]
    );

    await expectDomainError(
        () => transactions.withLockedTask(
            rollbackId,
            async (task, context) => {
                assert.equal(task.score, null);

                return context.freezeScore(100);
            }
        ),
        "CONFLICT"
    );

    console.log("Missing and closed task transactions rejected: OK");
}

async function cleanup() {
    await pool.query(
        `
        DELETE FROM order_creation_tasks
        WHERE operation_id = ANY($1::uuid[])
        `,
        [operationIds]
    );

    const remaining = await pool.query(
        `
        SELECT COUNT(*)::int AS count
        FROM order_creation_tasks
        WHERE operation_id = ANY($1::uuid[])
        `,
        [operationIds]
    );

    assert.equal(remaining.rows[0].count, 0);
}

try {
    await main();
} catch (error) {
    console.error(
        "Creation task lock check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        await cleanup();

        console.log("Scoped task lock fixtures removed: OK");
    } catch (error) {
        console.error(
            "Task lock fixture cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture operation IDs:", operationIds);
        process.exitCode = 1;
    } finally {
        await pool.end();
    }
}