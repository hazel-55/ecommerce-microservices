import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import { DomainError } from "../src/domain/errors.js";
import {
    createOutboxRepository,
} from "../src/infrastructure/repositories/outbox.repository.js";

let client;
let transactionStarted = false;

async function main() {
    client = await pool.connect();

    await client.query("BEGIN");
    transactionStarted = true;

    await client.query(`
        CREATE TEMP TABLE order_creation_tasks
        (LIKE public.order_creation_tasks INCLUDING ALL)
        ON COMMIT DROP
    `);

    await client.query(`
        CREATE TEMP TABLE orders
        (LIKE public.orders INCLUDING ALL)
        ON COMMIT DROP
    `);

    await client.query(`
        CREATE TEMP TABLE outbox_events
        (LIKE public.outbox_events INCLUDING ALL)
        ON COMMIT DROP
    `);

    const clock = await client.query(
        "SELECT CURRENT_TIMESTAMP AS now"
    );

    const now = clock.rows[0].now;
    const earlier = new Date(now.getTime() - 1000);
    const future = new Date(now.getTime() + 60_000);

    const repository = createOutboxRepository({
        database: client,
    });

    async function fixture({
        status = "COMPLETED",
        nextAttemptAt = now,
        published = false,
        legacy = false,
        eventType = "OrderCreated",
        wrongEventId = false,
        wrongOwner = false,
    } = {}) {
        const operationId = randomUUID();
        const eventId = wrongEventId
            ? randomUUID()
            : operationId;

        if (!legacy) {
            await client.query(
                `
                INSERT INTO order_creation_tasks (
                    operation_id,
                    uid,
                    actor_role,
                    items,
                    score,
                    status
                )
                VALUES (
                    $1, 13, 'CUSTOMER',
                    '[{"pid":1,"qty":1}]'::jsonb,
                    0, $2
                )
                `,
                [operationId, status]
            );
        }

        const inserted = await client.query(
            `
            INSERT INTO orders (
                uid,
                discount,
                operation_id
            )
            VALUES ($1, '0.00', $2)
            RETURNING oid
            `,
            [
                wrongOwner ? 14 : 13,
                legacy ? null : operationId,
            ]
        );

        const oid = inserted.rows[0].oid;

        const payload = {
            eventId,
            eventType,
            version: 1,
            oid,
            uid: wrongOwner ? 14 : 13,
            discount: "0.00",
            items: [
                { pid: 1, qty: 1, unit_price: "10.00" },
            ],
        };

        await client.query(
            `
            INSERT INTO outbox_events (
                event_id,
                event_type,
                oid,
                payload,
                created_at,
                next_attempt_at,
                published_at
            )
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
            `,
            [
                eventId,
                eventType,
                oid,
                JSON.stringify(payload),
                earlier,
                nextAttemptAt,
                published ? now : null,
            ]
        );

        return eventId;
    }

    const first = await fixture({ nextAttemptAt: earlier });
    const second = await fixture({ nextAttemptAt: earlier });
    const boundary = await fixture();

    const excluded = [];

    for (const status of [
        "PROCESSING",
        "CONFIRM_PENDING",
        "RELEASE_PENDING",
        "FAILED",
    ]) {
        excluded.push(await fixture({ status }));
    }

    excluded.push(
        await fixture({ nextAttemptAt: future }),
        await fixture({ published: true }),
        await fixture({ legacy: true }),
        await fixture({ eventType: "OtherEvent" }),
        await fixture({ wrongEventId: true }),
        await fixture({ wrongOwner: true })
    );

    const before = await client.query(`
        SELECT *
        FROM outbox_events
        ORDER BY event_id
    `);

    const tied = [first, second].sort();
    const expected = [...tied, boundary];

    const candidates = await repository.listDueEventIds();

    assert.deepEqual(candidates, expected);

    console.log(
        "Completed order events and due-time boundary: OK"
    );

    for (const eventId of excluded) {
        assert.equal(candidates.includes(eventId), false);
    }

    console.log(
        "Unready, published and unrelated events excluded: OK"
    );

    assert.deepEqual(
        await repository.listDueEventIds({ limit: 1 }),
        expected.slice(0, 1)
    );

    assert.deepEqual(
        await repository.listDueEventIds({ limit: 2 }),
        expected.slice(0, 2)
    );

    assert.deepEqual(
        await repository.listDueEventIds({ limit: 100 }),
        expected
    );

    console.log("Stable ordering and bounded Outbox batch: OK");

    for (const limit of [
        0, -1, 101, 1.5, "2", null, NaN,
    ]) {
        await assert.rejects(
            () => repository.listDueEventIds({ limit }),
            (error) => {
                assert.ok(error instanceof DomainError);
                assert.equal(error.code, "INVALID_ARGUMENT");
                return true;
            }
        );
    }

    console.log("Invalid Outbox batch limits rejected: OK");

    const after = await client.query(`
        SELECT *
        FROM outbox_events
        ORDER BY event_id
    `);

    assert.deepEqual(after.rows, before.rows);

    console.log("Pending scan leaves Outbox data unchanged: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Outbox pending check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        if (transactionStarted) {
            await client.query("ROLLBACK");
            console.log("Temporary Outbox fixtures removed: OK");
        }
    } catch (error) {
        console.error(
            "Outbox fixture cleanup failed:",
            error.code ?? error.message
        );

        process.exitCode = 1;
    } finally {
        client?.release();
        await pool.end();
    }
}