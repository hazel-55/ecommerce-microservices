import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import { DomainError } from "../src/domain/errors.js";
import {
    createOutboxTransaction,
} from "../src/infrastructure/database/outbox.transaction.js";

const operationIds = [];
const transactions = createOutboxTransaction();

async function withDeadline(promise) {
    let timer;

    try {
        return await Promise.race([
            promise,
            new Promise((resolve, reject) => {
                timer = setTimeout(
                    () => reject(new Error("Lock check timed out")),
                    5000
                );
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}

async function fixture({
    status = "COMPLETED",
    future = false,
    published = false,
} = {}) {
    const operationId = randomUUID();
    operationIds.push(operationId);

    await pool.query(
        `
        WITH task AS (
            INSERT INTO order_creation_tasks (
                operation_id,
                uid,
                actor_role,
                items,
                score,
                status,
                next_attempt_at
            )
            VALUES (
                $1::uuid,
                13,
                'CUSTOMER',
                '[{"pid":1,"qty":1}]'::jsonb,
                0,
                $2::text,
                CURRENT_TIMESTAMP + INTERVAL '1 day'
            )
            RETURNING operation_id, uid
        ),
        stored_order AS (
            INSERT INTO orders (uid, discount, operation_id)
            SELECT uid, '0.00', operation_id
            FROM task
            RETURNING oid, uid, operation_id
        )
        INSERT INTO outbox_events (
            event_id,
            event_type,
            oid,
            payload,
            next_attempt_at,
            published_at
        )
        SELECT
            operation_id,
            'OrderCreated',
            oid,
            jsonb_build_object(
                'eventId', operation_id,
                'eventType', 'OrderCreated',
                'version', 1,
                'oid', oid,
                'uid', uid,
                'discount', '0.00',
                'items',
                '[{"pid":1,"qty":1,"unit_price":"10.00"}]'::jsonb
            ),
            CASE
                WHEN $3::boolean
                    THEN CURRENT_TIMESTAMP + INTERVAL '1 day'
                ELSE CURRENT_TIMESTAMP
            END,
            CASE
                WHEN $4::boolean THEN CURRENT_TIMESTAMP
                ELSE NULL
            END
        FROM stored_order
        `,
        [operationId, status, future, published]
    );

    return operationId;
}

async function main() {
    const ready = await fixture();
    const future = await fixture({ future: true });
    const published = await fixture({ published: true });
    const unready = await fixture({
        status: "CONFIRM_PENDING",
    });

    const handled = await transactions.withLockedEvent(
        ready,
        async (event) => {
            assert.equal(event.event_id, ready);
            assert.equal(event.payload.eventId, ready);
            assert.equal(event.payload.discount, "0.00");

            event.payload.uid = 999;

            return "checked";
        }
    );

    assert.deepEqual(handled, {
        handled: true,
        result: "checked",
    });

    const stored = await pool.query(
        "SELECT payload FROM outbox_events WHERE event_id = $1",
        [ready]
    );

    assert.equal(stored.rows[0].payload.uid, 13);

    console.log("Eligible Outbox event locked and snapshot protected: OK");

    for (const eventId of [
        future,
        published,
        unready,
        randomUUID(),
    ]) {
        const skipped = await transactions.withLockedEvent(
            eventId,
            () => assert.fail("Ineligible callback must not run")
        );

        assert.deepEqual(skipped, { handled: false });
    }

    // Simulate a retry schedule changing after candidate discovery.
    await pool.query(
        `
        UPDATE outbox_events
        SET next_attempt_at =
            CURRENT_TIMESTAMP + INTERVAL '1 day'
        WHERE event_id = $1
        `,
        [ready]
    );

    assert.deepEqual(
        await transactions.withLockedEvent(
            ready,
            () => assert.fail("Deferred event must be skipped")
        ),
        { handled: false }
    );

    await pool.query(
        `
        UPDATE outbox_events
        SET next_attempt_at = CURRENT_TIMESTAMP
        WHERE event_id = $1
        `,
        [ready]
    );

    console.log("Outbox eligibility rechecked before callback: OK");

    const entered = Promise.withResolvers();
    const release = Promise.withResolvers();

    const first = transactions.withLockedEvent(
        ready,
        async () => {
            entered.resolve();
            await release.promise;
            return "first";
        }
    );

    void first.catch(entered.reject);

    try {
        await withDeadline(entered.promise);

        const second = await withDeadline(
            transactions.withLockedEvent(
                ready,
                () => assert.fail("Busy event must be skipped")
            )
        );

        assert.deepEqual(second, { handled: false });
    } finally {
        release.resolve();
        await first;
    }

    const reclaimed = await transactions.withLockedEvent(
        ready,
        () => "available"
    );

    assert.equal(reclaimed.handled, true);
    assert.equal(reclaimed.result, "available");

    console.log("Concurrent lock skipped and released row reusable: OK");

    // Capture the transaction connection only for rollback testing.
    let activeClient;

    const rollbackTransactions = createOutboxTransaction({
        database: {
            async connect() {
                activeClient = await pool.connect();
                return activeClient;
            },
        },
    });

    await assert.rejects(
        () => rollbackTransactions.withLockedEvent(
            ready,
            async () => {
                await activeClient.query(
                    `
                    UPDATE outbox_events
                    SET attempts = attempts + 1
                    WHERE event_id = $1
                    `,
                    [ready]
                );

                throw new Error("Forced callback failure");
            }
        ),
        /Forced callback failure/
    );

    const afterRollback = await pool.query(
        `
        SELECT attempts, published_at
        FROM outbox_events
        WHERE event_id = $1
        `,
        [ready]
    );

    assert.equal(afterRollback.rows[0].attempts, 0);
    assert.equal(afterRollback.rows[0].published_at, null);

    assert.equal(
        (
            await transactions.withLockedEvent(
                ready,
                () => "recovered"
            )
        ).handled,
        true
    );

    console.log("Callback failure rolls back and releases Outbox lock: OK");

    let connectionAttempts = 0;

    const validationOnly = createOutboxTransaction({
        database: {
            async connect() {
                connectionAttempts += 1;
                throw new Error("Unexpected database access");
            },
        },
    });

    for (const eventId of ["invalid", "", null, 123]) {
        await assert.rejects(
            () => validationOnly.withLockedEvent(
                eventId,
                () => { }
            ),
            (error) => {
                assert.ok(error instanceof DomainError);
                assert.equal(error.code, "INVALID_ARGUMENT");
                return true;
            }
        );
    }

    await assert.rejects(
        () => validationOnly.withLockedEvent(ready, null),
        TypeError
    );

    assert.equal(connectionAttempts, 0);

    console.log("Invalid Outbox lock input rejected before DB access: OK");
}

async function cleanup() {
    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        await client.query(
            "DELETE FROM outbox_events WHERE event_id = ANY($1::uuid[])",
            [operationIds]
        );

        await client.query(
            `
            DELETE FROM order_details
            WHERE oid IN (
                SELECT oid
                FROM orders
                WHERE operation_id = ANY($1::uuid[])
            )
            `,
            [operationIds]
        );

        await client.query(
            "DELETE FROM orders WHERE operation_id = ANY($1::uuid[])",
            [operationIds]
        );

        await client.query(
            `
            DELETE FROM order_creation_tasks
            WHERE operation_id = ANY($1::uuid[])
            `,
            [operationIds]
        );

        const remaining = await client.query(
            `
            SELECT
                (
                    SELECT COUNT(*)
                    FROM outbox_events
                    WHERE event_id = ANY($1::uuid[])
                ) +
                (
                    SELECT COUNT(*)
                    FROM orders
                    WHERE operation_id = ANY($1::uuid[])
                ) +
                (
                    SELECT COUNT(*)
                    FROM order_creation_tasks
                    WHERE operation_id = ANY($1::uuid[])
                ) AS total
            `,
            [operationIds]
        );

        assert.equal(Number(remaining.rows[0].total), 0);

        await client.query("COMMIT");
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
    console.error(
        "Outbox lock check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        await cleanup();
        console.log("Scoped Outbox lock fixtures removed: OK");
    } catch (error) {
        console.error(
            "Outbox lock cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture operation IDs:", operationIds);
        process.exitCode = 1;
    } finally {
        await pool.end();
    }
}