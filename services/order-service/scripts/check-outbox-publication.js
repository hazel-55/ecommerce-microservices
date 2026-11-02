import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import { DomainError } from "../src/domain/errors.js";
import {
    createOutboxTransaction,
} from "../src/infrastructure/database/outbox.transaction.js";

let client;
const closedContexts = [];

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

    // The test owns this connection for its entire lifetime.
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

    async function fixture() {
        const eventId = randomUUID();

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
                event_id,
                event_type,
                oid,
                payload,
                attempts,
                last_error
            )
            VALUES (
                $1, 'OrderCreated', $2, $3::jsonb,
                4, 'SERVICE_UNAVAILABLE'
            )
            `,
            [
                eventId,
                order.rows[0].oid,
                JSON.stringify(payload),
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

    const firstId = await fixture();
    const before = await readEvent(firstId);

    let repeatedMark;

    const completed = await transactions.withLockedEvent(
        firstId,
        async (event, context) => {
            closedContexts.push(context);

            const firstMark = await context.markPublished();
            repeatedMark = await context.markPublished();

            return firstMark;
        }
    );

    assert.equal(completed.handled, true);

    const published = await readEvent(firstId);

    assert.ok(published.published_at instanceof Date);
    assert.equal(published.last_error, null);

    assert.deepEqual(published, {
        ...before,
        published_at: published.published_at,
        last_error: null,
    });

    console.log(
        "Publication timestamp and unchanged event snapshot: OK"
    );

    assert.deepEqual(completed.result, repeatedMark);
    assert.deepEqual(completed.result, published);

    console.log("Repeated publication marker is idempotent: OK");

    const replay = await transactions.withLockedEvent(
        firstId,
        () => assert.fail("Published event must be skipped")
    );

    assert.deepEqual(replay, { handled: false });

    console.log("Committed published event excluded from replay: OK");

    const rollbackId = await fixture();
    const beforeRollback = await readEvent(rollbackId);

    await assert.rejects(
        () => transactions.withLockedEvent(
            rollbackId,
            async (event, context) => {
                closedContexts.push(context);

                await context.markPublished();

                throw new Error("Forced publication rollback");
            }
        ),
        /Forced publication rollback/
    );

    assert.deepEqual(
        await readEvent(rollbackId),
        beforeRollback
    );

    const recovered = await transactions.withLockedEvent(
        rollbackId,
        async (event, context) => {
            closedContexts.push(context);
            return context.markPublished();
        }
    );

    assert.equal(recovered.handled, true);
    assert.ok(
        (await readEvent(rollbackId)).published_at instanceof Date
    );

    console.log(
        "Publication rollback preserves retry and recovery: OK"
    );

    for (const context of closedContexts) {
        await assert.rejects(
            () => context.markPublished(),
            (error) => {
                assert.ok(error instanceof DomainError);
                assert.equal(error.code, "INTERNAL");
                return true;
            }
        );
    }

    console.log("Closed publication contexts rejected: OK");
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

    const remaining = await client.query(`
        SELECT
            to_regclass('pg_temp.outbox_events') AS events,
            to_regclass('pg_temp.orders') AS orders,
            to_regclass('pg_temp.order_creation_tasks') AS tasks
    `);

    assert.deepEqual(remaining.rows[0], {
        events: null,
        orders: null,
        tasks: null,
    });
}

try {
    await main();
} catch (error) {
    console.error(
        "Outbox publication check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        if (client) {
            await cleanup();

            console.log(
                "Temporary publication fixtures removed: OK"
            );
        }
    } catch (error) {
        console.error(
            "Publication fixture cleanup failed:",
            error.code ?? error.message
        );

        process.exitCode = 1;
    } finally {
        client?.release();
        await pool.end();
    }
}