import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import { DomainError } from "../src/domain/errors.js";
import {
    createOutboxTransaction,
} from "../src/infrastructure/database/outbox.transaction.js";

let client;
const contexts = [];

async function expectDomain(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    client = await pool.connect();

    for (const table of [
        "order_creation_tasks",
        "orders",
        "outbox_events",
    ]) {
        await client.query(`
            CREATE TEMP TABLE ${table}
            (LIKE public.${table} INCLUDING ALL)
            ON COMMIT PRESERVE ROWS
        `);
    }

    const transactions = createOutboxTransaction({
        database: {
            async connect() {
                return {
                    query: client.query.bind(client),
                    release() { },
                };
            },
        },
    });

    async function fixture(attempts = 0) {
        const eventId = randomUUID();

        await client.query(
            `
            INSERT INTO order_creation_tasks (
                operation_id, uid, actor_role,
                items, score, status
            )
            VALUES (
                $1, 13, 'CUSTOMER',
                '[{"pid":1,"qty":1}]'::jsonb,
                0, 'COMPLETED'
            )
            `,
            [eventId]
        );

        const order = await client.query(
            `
            INSERT INTO orders (uid, discount, operation_id)
            VALUES (13, '0.00', $1)
            RETURNING oid
            `,
            [eventId]
        );

        const payload = {
            eventId,
            eventType: "OrderCreated",
            version: 1,
            oid: order.rows[0].oid,
            uid: 13,
            discount: "0.00",
            items: [
                { pid: 1, qty: 1, unit_price: "10.00" },
            ],
        };

        await client.query(
            `
            INSERT INTO outbox_events (
                event_id, event_type, oid, payload, attempts
            )
            VALUES (
                $1, 'OrderCreated', $2, $3::jsonb, $4
            )
            `,
            [
                eventId,
                order.rows[0].oid,
                JSON.stringify(payload),
                attempts,
            ]
        );

        return eventId;
    }

    async function readEvent(eventId) {
        const result = await client.query(
            "SELECT * FROM outbox_events WHERE event_id = $1",
            [eventId]
        );

        return result.rows[0];
    }

    async function databaseNow() {
        const result = await client.query(
            "SELECT clock_timestamp() AS now"
        );

        return result.rows[0].now.getTime();
    }

    const eventId = await fixture();
    const before = await readEvent(eventId);
    const clockBefore = await databaseNow();

    let repeated;
    let retryBlockedPublication = false;

    const retried = await transactions.withLockedEvent(
        eventId,
        async (event, context) => {
            contexts.push(context);

            const first = await context.scheduleRetry(
                "SERVICE_UNAVAILABLE"
            );

            repeated = await context.scheduleRetry("INTERNAL");

            await expectDomain(
                () => context.markPublished(),
                "CONFLICT"
            );

            retryBlockedPublication = true;

            return first;
        }
    );

    const clockAfter = await databaseNow();
    const stored = await readEvent(eventId);

    assert.equal(retried.handled, true);
    assert.equal(stored.attempts, 1);
    assert.equal(stored.last_error, "SERVICE_UNAVAILABLE");
    assert.equal(stored.published_at, null);

    const nextAttempt = stored.next_attempt_at.getTime();

    assert.ok(nextAttempt >= clockBefore + 1000);
    assert.ok(nextAttempt <= clockAfter + 1000);

    console.log(
        "Durable Outbox retry metadata and database clock: OK"
    );

    assert.deepEqual(retried.result, repeated);

    assert.deepEqual(stored, {
        ...before,
        attempts: 1,
        last_error: "SERVICE_UNAVAILABLE",
        next_attempt_at: stored.next_attempt_at,
    });

    console.log(
        "Retry preserves event snapshot and first failure: OK"
    );

    const publicationId = await fixture();

    await transactions.withLockedEvent(
        publicationId,
        async (event, context) => {
            contexts.push(context);

            await context.markPublished();

            await expectDomain(
                () => context.scheduleRetry("INTERNAL"),
                "CONFLICT"
            );
        }
    );

    assert.equal(retryBlockedPublication, true);
    assert.ok(
        (await readEvent(publicationId)).published_at instanceof Date
    );

    console.log(
        "Retry and publication decisions cannot mix: OK"
    );

    const rollbackId = await fixture();
    const beforeRollback = await readEvent(rollbackId);

    await assert.rejects(
        () => transactions.withLockedEvent(
            rollbackId,
            async (event, context) => {
                contexts.push(context);

                await context.scheduleRetry("INTERNAL");

                throw new Error("Forced retry rollback");
            }
        ),
        /Forced retry rollback/
    );

    assert.deepEqual(
        await readEvent(rollbackId),
        beforeRollback
    );

    for (const context of contexts) {
        await expectDomain(
            () => context.scheduleRetry("INTERNAL"),
            "INTERNAL"
        );

        await expectDomain(
            () => context.markPublished(),
            "INTERNAL"
        );
    }

    console.log(
        "Retry rollback and closed context protection: OK"
    );

    const boundedId = await fixture(2147483646);

    for (let round = 0; round < 2; round += 1) {
        await transactions.withLockedEvent(
            boundedId,
            async (event, context) => {
                const updated = await context.scheduleRetry(
                    "INTERNAL"
                );

                assert.equal(updated.attempts, 2147483647);
                assert.equal(updated.published_at, null);
            }
        );

        // Make only this temporary fixture due for the next round.
        await client.query(
            `
            UPDATE outbox_events
            SET next_attempt_at = CURRENT_TIMESTAMP
            WHERE event_id = $1
            `,
            [boundedId]
        );
    }

    const invalidBefore = await readEvent(rollbackId);

    await transactions.withLockedEvent(
        rollbackId,
        async (event, context) => {
            await expectDomain(
                () => context.scheduleRetry("Raw error details"),
                "INVALID_ARGUMENT"
            );
        }
    );

    assert.deepEqual(
        await readEvent(rollbackId),
        invalidBefore
    );

    console.log(
        "Retry counter bound and invalid error codes: OK"
    );
}

async function cleanup() {
    for (const table of [
        "outbox_events",
        "orders",
        "order_creation_tasks",
    ]) {
        await client.query(
            `DROP TABLE IF EXISTS pg_temp.${table}`
        );
    }

    const result = await client.query(`
        SELECT
            to_regclass('pg_temp.outbox_events') AS events,
            to_regclass('pg_temp.orders') AS orders,
            to_regclass('pg_temp.order_creation_tasks') AS tasks
    `);

    assert.deepEqual(result.rows[0], {
        events: null,
        orders: null,
        tasks: null,
    });
}

try {
    await main();
} catch (error) {
    console.error(
        "Outbox retry check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        if (client) {
            await cleanup();

            console.log(
                "Temporary Outbox retry fixtures removed: OK"
            );
        }
    } catch (error) {
        console.error(
            "Outbox retry cleanup failed:",
            error.code ?? error.message
        );

        process.exitCode = 1;
    } finally {
        client?.release();
        await pool.end();
    }
}