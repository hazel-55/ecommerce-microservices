import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import { DomainError } from "../src/domain/errors.js";
import {
    createOrderCreationRepository,
} from "../src/infrastructure/repositories/order-creation.repository.js";

let client;
let transactionStarted = false;

async function main() {
    client = await pool.connect();

    await client.query("BEGIN");
    transactionStarted = true;

    // Chỉ kết nối kiểm tra này nhìn thấy bảng tạm.
    // Các truy vấn repository sẽ dùng bảng tạm cùng tên.
    await client.query(
        `
        CREATE TEMP TABLE order_creation_tasks (
            LIKE public.order_creation_tasks INCLUDING ALL
        )
        ON COMMIT DROP
        `,
    );

    const repository = createOrderCreationRepository({
        database: client,
    });

    const clock = await client.query(
        "SELECT CURRENT_TIMESTAMP AS now",
    );

    const now = clock.rows[0].now;
    const past = new Date(now.getTime() - 60_000);
    const future = new Date(now.getTime() + 60_000);
    const createdAt = new Date(now.getTime() - 120_000);

    const states = [
        "PROCESSING",
        "CONFIRM_PENDING",
        "RELEASE_PENDING",
    ];

    const dueIds = [
        randomUUID(),
        randomUUID(),
        randomUUID(),
    ];

    const boundaryId = randomUUID();
    const futureId = randomUUID();
    const completedId = randomUUID();
    const failedId = randomUUID();

    async function insertTask(
        operationId,
        status,
        nextAttemptAt,
    ) {
        await client.query(
            `
            INSERT INTO order_creation_tasks (
                operation_id,
                uid,
                actor_role,
                items,
                status,
                next_attempt_at,
                created_at
            )
            VALUES (
                $1::uuid,
                13,
                'CUSTOMER',
                $2::jsonb,
                $3::text,
                $4::timestamptz,
                $5::timestamptz
            )
            `,
            [
                operationId,
                JSON.stringify([{ pid: 1, qty: 1 }]),
                status,
                nextAttemptAt,
                createdAt,
            ],
        );
    }

    for (let index = 0; index < states.length; index++) {
        await insertTask(
            dueIds[index],
            states[index],
            past,
        );
    }

    // Đúng bằng thời điểm hiện tại cũng được tính đến hạn.
    await insertTask(boundaryId, "PROCESSING", now);

    await insertTask(futureId, "PROCESSING", future);
    await insertTask(completedId, "COMPLETED", past);
    await insertTask(failedId, "FAILED", past);

    const result = await repository.listDueOperationIds({
        limit: 100,
    });

    assert.equal(result.length, 4);

    assert.deepEqual(
        new Set(result),
        new Set([...dueIds, boundaryId]),
    );

    console.log("Due pending task states and time boundary: OK");

    for (const excluded of [
        futureId,
        completedId,
        failedId,
    ]) {
        assert.equal(result.includes(excluded), false);
    }

    console.log("Future and closed tasks excluded: OK");

    // Ba task đầu có cùng lịch và thời điểm tạo.
    // UUID là tiêu chí cuối để giữ thứ tự ổn định.
    const expected = [
        ...dueIds.toSorted(),
        boundaryId,
    ];

    assert.deepEqual(result, expected);

    assert.deepEqual(
        await repository.listDueOperationIds({ limit: 2 }),
        expected.slice(0, 2),
    );

    assert.deepEqual(
        await repository.listDueOperationIds({ limit: 1 }),
        expected.slice(0, 1),
    );

    assert.deepEqual(
        await repository.listDueOperationIds(),
        expected,
    );

    console.log("Stable ordering and bounded batch size: OK");

    for (const limit of [
        0,
        -1,
        101,
        1.5,
        "20",
        null,
        NaN,
        Infinity,
    ]) {
        await assert.rejects(
            () => repository.listDueOperationIds({ limit }),
            (error) =>
                error instanceof DomainError &&
                error.code === "INVALID_ARGUMENT",
        );
    }

    console.log("Invalid pending task limits rejected: OK");

    const before = await client.query(
        `
        SELECT *
        FROM order_creation_tasks
        ORDER BY operation_id
        `,
    );

    for (let index = 0; index < 3; index++) {
        assert.deepEqual(
            await repository.listDueOperationIds({
                limit: 100,
            }),
            expected,
        );
    }

    const after = await client.query(
        `
        SELECT *
        FROM order_creation_tasks
        ORDER BY operation_id
        `,
    );

    assert.deepEqual(after.rows, before.rows);

    console.log("Pending scan leaves task data unchanged: OK");
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Creation pending check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    try {
        if (client && transactionStarted) {
            await client.query("ROLLBACK");
            transactionStarted = false;

            const remaining = await client.query(
                `
                SELECT to_regclass(
                    'pg_temp.order_creation_tasks'
                ) AS table_name
                `,
            );

            assert.equal(
                remaining.rows[0].table_name,
                null,
            );

            console.log(
                "Temporary pending task fixtures removed: OK",
            );
        }
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Creation pending cleanup failed:",
            error.code ?? error.name,
            error.message,
        );
    } finally {
        client?.release();
        await pool.end();
    }
}