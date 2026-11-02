import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import { DomainError } from "../src/domain/errors.js";
import {
    createOutboxTransaction,
} from "../src/infrastructure/database/outbox.transaction.js";
import {
    createOutboxPublisher,
} from "../src/application/outbox.publisher.js";

let client;
let failNextCommit = false;
let sendHandler = async () => [];

const sent = [];

async function withDeadline(promise) {
    let timer;

    try {
        return await Promise.race([
            promise,
            new Promise((resolve, reject) => {
                timer = setTimeout(
                    () => reject(new Error("Publisher check timed out")),
                    5000
                );
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
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
                    async query(sql, parameters) {
                        if (
                            sql.trim() === "COMMIT" &&
                            failNextCommit
                        ) {
                            failNextCommit = false;
                            throw new Error("Forced SQL commit failure");
                        }

                        return await client.query(sql, parameters);
                    },

                    release() { },
                };
            },
        },
    });

    const publisher = createOutboxPublisher({
        transactions,
        producer: {
            async publish(event) {
                sent.push(structuredClone(event));
                return await sendHandler(event);
            },
        },
    });

    async function fixture() {
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
                event_id, event_type, oid, payload
            )
            VALUES ($1, 'OrderCreated', $2, $3::jsonb)
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

    const successId = await fixture();
    const entered = Promise.withResolvers();
    const acknowledgment = Promise.withResolvers();

    sendHandler = async () => {
        entered.resolve();
        await acknowledgment.promise;
        return [];
    };

    const pending = publisher.publishEvent(successId);
    void pending.catch(entered.reject);

    let completed;

    try {
        await withDeadline(entered.promise);

        assert.equal(
            (await readEvent(successId)).published_at,
            null
        );
    } finally {
        acknowledgment.resolve();
        completed = await pending;
    }

    assert.deepEqual(completed, {
        handled: true,
        result: {
            eventId: successId,
            outcome: "PUBLISHED",
        },
    });

    assert.ok(
        (await readEvent(successId)).published_at instanceof Date
    );

    console.log(
        "Kafka acknowledgment precedes Outbox publication commit: OK"
    );

    const kafkaError = new Error("Simulated connection failure");
    kafkaError.name = "KafkaJSConnectionError";

    const retryId = await fixture();

    sendHandler = async () => {
        throw kafkaError;
    };

    const retryResult = await publisher.publishEvent(retryId);
    const retryStored = await readEvent(retryId);

    assert.equal(
        retryResult.result.outcome,
        "RETRY_SCHEDULED"
    );
    assert.equal(
        retryResult.result.errorCode,
        "SERVICE_UNAVAILABLE"
    );
    assert.equal(retryStored.attempts, 1);
    assert.equal(
        retryStored.last_error,
        "SERVICE_UNAVAILABLE"
    );
    assert.equal(retryStored.published_at, null);

    const invalidId = await fixture();

    sendHandler = async () => {
        throw new DomainError(
            "INTERNAL",
            "Invalid stored event"
        );
    };

    await publisher.publishEvent(invalidId);

    assert.equal(
        (await readEvent(invalidId)).last_error,
        "INTERNAL"
    );
    assert.equal(
        (await readEvent(invalidId)).published_at,
        null
    );

    console.log(
        "Send failures schedule durable retry without publication: OK"
    );

    // Keep this fixture safely in the future for the skip check.
    await client.query(
        `
        UPDATE outbox_events
        SET next_attempt_at =
            CURRENT_TIMESTAMP + INTERVAL '1 day'
        WHERE event_id = $1
        `,
        [retryId]
    );

    const sentBeforeSkip = sent.length;

    for (const eventId of [
        successId,
        retryId,
        randomUUID(),
    ]) {
        assert.deepEqual(
            await publisher.publishEvent(eventId),
            { handled: false }
        );
    }

    assert.equal(sent.length, sentBeforeSkip);

    console.log(
        "Published and deferred events do not call producer: OK"
    );

    const sqlFailureId = await fixture();
    const beforeSqlFailure = await readEvent(sqlFailureId);

    sendHandler = async () => [];
    failNextCommit = true;

    const sendsBeforeSqlFailure = sent.length;

    await assert.rejects(
        () => publisher.publishEvent(sqlFailureId),
        /Forced SQL commit failure/
    );

    assert.deepEqual(
        await readEvent(sqlFailureId),
        beforeSqlFailure
    );

    assert.equal(sent.length, sendsBeforeSqlFailure + 1);

    await publisher.publishEvent(sqlFailureId);

    const firstSend = sent[sendsBeforeSqlFailure];
    const replaySend = sent[sendsBeforeSqlFailure + 1];

    assert.equal(firstSend.event_id, replaySend.event_id);
    assert.deepEqual(firstSend.payload, replaySend.payload);

    assert.ok(
        (await readEvent(sqlFailureId)).published_at instanceof Date
    );

    console.log(
        "SQL failure after Kafka acknowledgment remains replayable: OK"
    );

    const retryFailureId = await fixture();
    const beforeRetryFailure = await readEvent(retryFailureId);

    sendHandler = async () => {
        throw kafkaError;
    };

    failNextCommit = true;

    await assert.rejects(
        () => publisher.publishEvent(retryFailureId),
        /Forced SQL commit failure/
    );

    assert.deepEqual(
        await readEvent(retryFailureId),
        beforeRetryFailure
    );

    const recovered = await publisher.publishEvent(
        retryFailureId
    );

    assert.equal(
        recovered.result.outcome,
        "RETRY_SCHEDULED"
    );
    assert.equal(
        (await readEvent(retryFailureId)).attempts,
        1
    );

    console.log(
        "Retry persistence failure rolls back and propagates: OK"
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
        "Outbox publisher check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        if (client) {
            await cleanup();

            console.log(
                "Scoped publisher fixtures removed: OK"
            );
        }
    } catch (error) {
        console.error(
            "Publisher fixture cleanup failed:",
            error.code ?? error.message
        );

        process.exitCode = 1;
    } finally {
        client?.release();
        await pool.end();
    }
}