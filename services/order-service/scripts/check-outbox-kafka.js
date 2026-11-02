import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    createOutboxRepository,
} from "../src/infrastructure/repositories/outbox.repository.js";
import {
    createOutboxTransaction,
} from "../src/infrastructure/database/outbox.transaction.js";
import {
    createKafkaClient,
} from "../src/infrastructure/kafka/client.js";
import {
    createOrderEventProducer,
} from "../src/infrastructure/kafka/order-event.producer.js";
import {
    createOutboxPublisher,
} from "../src/application/outbox.publisher.js";
import {
    createOutboxWorker,
} from "../src/application/outbox.worker.js";

const fixtureId = randomUUID();
const topic = `check-outbox-${fixtureId}`;
const groupId = `check-outbox-${fixtureId}`;

const kafka = createKafkaClient({
    clientId: `outbox-check-${fixtureId}`,
});

const admin = kafka.admin();

const producer = createOrderEventProducer({
    kafka,
    topic,
});

const consumer = kafka.consumer({
    groupId,
    allowAutoTopicCreation: false,
    maxWaitTimeInMs: 1000,
    retry: {
        retries: 8,
        initialRetryTime: 300,
        maxRetryTime: 3000,
    },
});

let client;
let adminConnected = false;
let topicOwned = false;
let groupOwned = false;
let consumerFailure;

const joined = Promise.withResolvers();
const received = new Map();
const waiters = new Map();

void joined.promise.catch(() => { });

function failConsumer(error) {
    consumerFailure = error;
    joined.reject(error);

    for (const waiter of waiters.values()) {
        waiter.reject(error);
    }
}

consumer.on(consumer.events.GROUP_JOIN, () => {
    joined.resolve();
});

consumer.on(consumer.events.CRASH, ({ payload }) => {
    failConsumer(
        payload.error ?? new Error("Consumer crashed")
    );
});

async function withDeadline(promise, label) {
    let timer;

    try {
        return await Promise.race([
            promise,
            new Promise((resolve, reject) => {
                timer = setTimeout(
                    () => reject(new Error(`${label} timed out`)),
                    45_000
                );
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}

async function waitForEvent(eventId) {
    if (consumerFailure) {
        throw consumerFailure;
    }

    if (received.has(eventId)) {
        return received.get(eventId);
    }

    const waiter = Promise.withResolvers();
    waiters.set(eventId, waiter);

    try {
        return await withDeadline(
            waiter.promise,
            "Outbox event consumption"
        );
    } finally {
        waiters.delete(eventId);
    }
}

async function waitUntil(check) {
    const deadline = Date.now() + 20_000;

    while (true) {
        if (await check()) {
            return;
        }

        if (Date.now() >= deadline) {
            throw new Error("Fixture cleanup timed out");
        }

        await delay(250);
    }
}

function expectBatch(stats, scanned, published) {
    assert.deepEqual(stats, {
        scanned,
        published,
        retried: 0,
        skipped: 0,
        failed: 0,
    });
}

async function fixture(status) {
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
            0, $2
        )
        `,
        [eventId, status]
    );

    const order = await client.query(
        `
        INSERT INTO orders (uid, discount, operation_id)
        VALUES (13, '0.00', $1)
        RETURNING oid
        `,
        [eventId]
    );

    const oid = order.rows[0].oid;

    const payload = {
        eventId,
        eventType: "OrderCreated",
        version: 1,
        oid,
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
        [eventId, oid, JSON.stringify(payload)]
    );

    return { eventId, oid, payload };
}

async function readEvent(eventId) {
    const result = await client.query(
        "SELECT * FROM outbox_events WHERE event_id = $1",
        [eventId]
    );

    return result.rows[0];
}

async function topicOffsets() {
    const offsets = await admin.fetchTopicOffsets(topic);

    return offsets.map(({ partition, high }) => ({
        partition,
        high,
    }));
}

function expectMessage(message, expected) {
    assert.equal(message.topic, topic);
    assert.equal(message.partition, 0);
    assert.equal(message.key, String(expected.oid));
    assert.deepEqual(message.payload, expected.payload);
    assert.equal(typeof message.payload.discount, "string");

    assert.equal(
        typeof message.payload.items[0].unit_price,
        "string"
    );
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

    const ready = await fixture("COMPLETED");
    const blocked = await fixture("CONFIRM_PENDING");

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

    const publisher = createOutboxPublisher({
        transactions,
        producer,
    });

    const worker = createOutboxWorker({
        repository: createOutboxRepository({
            database: client,
        }),
        publisher,
        batchSize: 5,
    });

    await admin.connect();
    adminConnected = true;

    assert.equal(
        (await admin.listTopics()).includes(topic),
        false
    );

    assert.equal(
        (await admin.listGroups()).groups.some(
            (group) => group.groupId === groupId
        ),
        false
    );

    topicOwned = true;

    await admin.createTopics({
        waitForLeaders: true,
        timeout: 10000,
        topics: [
            {
                topic,
                numPartitions: 1,
                replicationFactor: 1,
            },
        ],
    });

    groupOwned = true;

    await consumer.connect();

    await consumer.subscribe({
        topics: [topic],
        fromBeginning: true,
    });

    await consumer.run({
        autoCommit: false,

        eachMessage: async ({
            topic: receivedTopic,
            partition,
            message,
        }) => {
            try {
                const payload = JSON.parse(
                    message.value.toString("utf8")
                );

                const record = {
                    topic: receivedTopic,
                    partition,
                    offset: message.offset,
                    key: message.key?.toString("utf8"),
                    payload,
                };

                received.set(payload.eventId, record);
                waiters.get(payload.eventId)?.resolve(record);
            } catch (error) {
                failConsumer(error);
            }
        },
    });

    await withDeadline(joined.promise, "Consumer group join");
    await producer.connect();

    console.log(
        "Isolated PostgreSQL and Kafka fixtures ready: OK"
    );

    expectBatch(await worker.runOnce(), 1, 1);

    expectMessage(
        await waitForEvent(ready.eventId),
        ready
    );

    assert.ok(
        (await readEvent(ready.eventId)).published_at instanceof Date
    );

    console.log(
        "Completed Outbox event published and consumed: OK"
    );

    assert.equal(
        (await readEvent(blocked.eventId)).published_at,
        null
    );

    assert.equal(received.has(blocked.eventId), false);

    console.log(
        "Pending stock confirmation blocks event publication: OK"
    );

    const offsetsBeforeReplay = await topicOffsets();

    expectBatch(await worker.runOnce(), 0, 0);

    assert.deepEqual(
        await topicOffsets(),
        offsetsBeforeReplay
    );

    console.log(
        "Published event replay leaves Kafka offsets unchanged: OK"
    );

    // Only transition this temporary fixture.
    await client.query(
        `
        UPDATE order_creation_tasks
        SET status = 'COMPLETED',
            updated_at = CURRENT_TIMESTAMP
        WHERE operation_id = $1
        `,
        [blocked.eventId]
    );

    expectBatch(await worker.runOnce(), 1, 1);

    expectMessage(
        await waitForEvent(blocked.eventId),
        blocked
    );

    assert.ok(
        (await readEvent(blocked.eventId)).published_at instanceof Date
    );

    console.log(
        "Task completion enables the next Outbox publication: OK"
    );
}

async function cleanupStep(label, operation) {
    try {
        await operation();
        return true;
    } catch (error) {
        console.error(
            `${label} failed:`,
            error.code ?? error.message
        );

        process.exitCode = 1;
        return false;
    }
}

try {
    await main();
} catch (error) {
    console.error(
        "Live Outbox integration failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    for (const waiter of waiters.values()) {
        waiter.reject(new Error("Integration test stopped"));
    }

    const stopped = await cleanupStep(
        "Consumer stop",
        () => consumer.stop()
    );

    const consumerClosed = await cleanupStep(
        "Consumer disconnect",
        () => consumer.disconnect()
    );

    const producerClosed = await cleanupStep(
        "Producer disconnect",
        () => producer.disconnect()
    );

    let sqlCleaned = true;
    let groupCleaned = true;
    let topicCleaned = true;

    if (client) {
        sqlCleaned = await cleanupStep(
            "Temporary SQL cleanup",
            async () => {
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
        );
    }

    if (adminConnected && groupOwned) {
        groupCleaned = await cleanupStep(
            "Isolated group cleanup",
            async () => {
                const result = await admin.listGroups();

                if (
                    result.groups.some(
                        (group) => group.groupId === groupId
                    )
                ) {
                    await admin.deleteGroups([groupId]);
                }

                await waitUntil(async () =>
                    !(await admin.listGroups()).groups.some(
                        (group) => group.groupId === groupId
                    )
                );
            }
        );
    }

    if (adminConnected && topicOwned) {
        topicCleaned = await cleanupStep(
            "Isolated topic cleanup",
            async () => {
                if ((await admin.listTopics()).includes(topic)) {
                    await admin.deleteTopics({
                        topics: [topic],
                        timeout: 10000,
                    });
                }

                await waitUntil(async () =>
                    !(await admin.listTopics()).includes(topic)
                );
            }
        );
    }

    if (
        topicOwned &&
        groupOwned &&
        sqlCleaned &&
        groupCleaned &&
        topicCleaned
    ) {
        console.log(
            "Scoped Outbox SQL and Kafka fixtures removed: OK"
        );
    }

    const adminClosed = await cleanupStep(
        "Admin disconnect",
        () => admin.disconnect()
    );

    const poolClosed = await cleanupStep(
        "PostgreSQL disconnect",
        async () => {
            client?.release();
            client = undefined;
            await pool.end();
        }
    );

    if (
        stopped &&
        consumerClosed &&
        producerClosed &&
        adminClosed &&
        poolClosed
    ) {
        console.log(
            "Outbox integration connections closed: OK"
        );
    }

    if (process.exitCode) {
        console.error("Fixture topic:", topic);
        console.error("Fixture group:", groupId);
    }
}