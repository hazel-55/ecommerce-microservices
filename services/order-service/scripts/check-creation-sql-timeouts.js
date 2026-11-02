import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    createOrderCreationTransaction,
} from "../src/infrastructure/database/order-creation.transaction.js";
import {
    orderCreationRepository,
} from "../src/infrastructure/repositories/order-creation.repository.js";

const operationId = randomUUID();

let session;
let holder;
let sessionTransaction = false;
let holderTransaction = false;

const releases = [];

async function expectUnavailable(operation) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, "SERVICE_UNAVAILABLE");
        return true;
    });
}

async function readSettings() {
    const result = await session.query(`
        SELECT
            current_setting('lock_timeout') AS lock_timeout,
            current_setting('statement_timeout') AS statement_timeout
    `);

    return result.rows[0];
}

async function main() {
    let connections = 0;

    const unusedDatabase = {
        async connect() {
            connections += 1;
            throw new Error("Unexpected database access");
        },
    };

    for (const options of [
        { lockTimeoutMs: 0 },
        { statementTimeoutMs: 0 },
        { lockTimeoutMs: 1.5 },
        { statementTimeoutMs: 60_001 },
        { lockTimeoutMs: 500, statementTimeoutMs: 100 },
    ]) {
        assert.throws(
            () => createOrderCreationTransaction({
                database: unusedDatabase,
                ...options,
            }),
            (error) =>
                error instanceof DomainError &&
                error.code === "INVALID_ARGUMENT",
        );
    }

    assert.equal(connections, 0);

    console.log("Invalid SQL timeout options rejected before connection: OK");

    await orderCreationRepository.createTask(
        {
            operationId,
            uid: 13,
            actorRole: "CUSTOMER",
            items: [{ pid: 1, qty: 1 }],
        },
        {
            // Worker không chọn task thử nghiệm này.
            nextAttemptAt: new Date(
                Date.now() + 24 * 60 * 60 * 1000,
            ),
        },
    );

    session = await pool.connect();

    const originalSettings = await readSettings();

    // Giữ một connection thật để kiểm tra cấu hình sau transaction.
    // Wrapper chỉ ghi nhận release; finally sẽ trả connection về pool.
    const database = {
        async connect() {
            return {
                async query(...args) {
                    const result = await session.query(...args);

                    if (args[0] === "BEGIN") {
                        sessionTransaction = true;
                    }

                    if (
                        args[0] === "COMMIT" ||
                        args[0] === "ROLLBACK"
                    ) {
                        sessionTransaction = false;
                    }

                    return result;
                },

                release(error) {
                    releases.push(error);
                },
            };
        },
    };

    const localLimits = createOrderCreationTransaction({
        database,
        lockTimeoutMs: 100,
        statementTimeoutMs: 500,
    });

    const returned = await localLimits.withLockedTask(
        operationId,
        async () => {
            assert.deepEqual(await readSettings(), {
                lock_timeout: "100ms",
                statement_timeout: "500ms",
            });

            return "done";
        },
    );

    assert.equal(returned, "done");

    assert.deepEqual(
        await readSettings(),
        originalSettings,
    );

    console.log("Transaction-local SQL limits and session restoration: OK");

    const originalTask =
        await orderCreationRepository.findByOperationId(
            operationId,
        );

    holder = await pool.connect();

    await holder.query("BEGIN");
    holderTransaction = true;

    await holder.query(
        `
        SELECT operation_id
        FROM order_creation_tasks
        WHERE operation_id = $1
        FOR UPDATE
        `,
        [operationId],
    );

    const lockLimited = createOrderCreationTransaction({
        database,
        lockTimeoutMs: 50,
        statementTimeoutMs: 500,
    });

    let callbackCalls = 0;

    try {
        await expectUnavailable(
            () => lockLimited.withLockedTask(
                operationId,
                async () => {
                    callbackCalls += 1;
                },
            ),
        );
    } finally {
        await holder.query("ROLLBACK");
        holderTransaction = false;
    }

    assert.equal(callbackCalls, 0);

    assert.deepEqual(
        await orderCreationRepository.findByOperationId(
            operationId,
        ),
        originalTask,
    );

    assert.deepEqual(
        await readSettings(),
        originalSettings,
    );

    console.log("Contended task lock times out before callback: OK");

    const statementLimited = createOrderCreationTransaction({
        database,
        lockTimeoutMs: 50,
        statementTimeoutMs: 100,
    });

    await expectUnavailable(
        () => statementLimited.withLockedTask(
            operationId,
            async (task, context) => {
                await context.freezeScore(100);

                // PostgreSQL tự hủy câu SQL này.
                await session.query(
                    "SELECT pg_sleep(0.25)",
                );
            },
        ),
    );

    // Việc ghi score trước câu SQL bị timeout phải rollback.
    assert.deepEqual(
        await orderCreationRepository.findByOperationId(
            operationId,
        ),
        originalTask,
    );

    assert.deepEqual(
        await readSettings(),
        originalSettings,
    );

    console.log("Statement timeout rolls back task changes: OK");

    await localLimits.withLockedTask(
        operationId,
        async (task, context) => {
            await context.freezeScore(200);
        },
    );

    const recovered =
        await orderCreationRepository.findByOperationId(
            operationId,
        );

    assert.equal(recovered.score, 200);
    assert.equal(recovered.status, "PROCESSING");

    assert.deepEqual(
        await readSettings(),
        originalSettings,
    );

    assert.equal(releases.length, 4);

    for (const error of releases) {
        assert.equal(error, undefined);
    }

    console.log("Connection remains reusable after timeout rollback: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Creation SQL timeout check failed:",
        error.code ?? error.name,
    );

    process.exitCode = 1;
} finally {
    if (holder) {
        try {
            if (holderTransaction) {
                await holder.query("ROLLBACK");
            }
        } catch {
            process.exitCode = 1;
        } finally {
            holder.release();
        }
    }

    if (session) {
        try {
            if (sessionTransaction) {
                await session.query("ROLLBACK");
            }
        } catch {
            process.exitCode = 1;
        } finally {
            session.release();
        }
    }

    try {
        await pool.query(
            `
            DELETE FROM order_creation_tasks
            WHERE operation_id = $1
            `,
            [operationId],
        );

        const remaining = await pool.query(
            `
            SELECT COUNT(*)::integer AS count
            FROM order_creation_tasks
            WHERE operation_id = $1
            `,
            [operationId],
        );

        assert.equal(remaining.rows[0].count, 0);

        console.log("Scoped SQL timeout fixtures removed: OK");
    } catch (error) {
        console.error(
            "SQL timeout fixture cleanup failed:",
            error.code ?? error.name,
        );

        console.error("Fixture operation ID:", operationId);
        process.exitCode = 1;
    } finally {
        await pool.end();
    }
}