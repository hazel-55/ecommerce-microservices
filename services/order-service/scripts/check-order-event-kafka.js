import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import {
    createKafkaClient,
} from "../src/infrastructure/kafka/client.js";
import {
    createOrderEventProducer,
} from "../src/infrastructure/kafka/order-event.producer.js";

const fixtureId = randomUUID();
const topic = `check-order-created-${fixtureId}`;
const groupId = `check-order-event-${fixtureId}`;

const kafka = createKafkaClient({
    clientId: `order-event-check-${fixtureId}`,
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

let adminConnected = false;
let topicOwned = false;
let groupOwned = false;

const joined = Promise.withResolvers();
const receivedReady = Promise.withResolvers();
const received = [];

// Keep early consumer errors observable until main() awaits them.
void joined.promise.catch(() => { });
void receivedReady.promise.catch(() => { });

consumer.on(consumer.events.GROUP_JOIN, () => {
    joined.resolve();
});

consumer.on(consumer.events.CRASH, ({ payload }) => {
    const error = payload.error ??
        new Error("Kafka test consumer crashed");

    joined.reject(error);
    receivedReady.reject(error);
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

async function waitUntil(check) {
    const deadline = Date.now() + 20_000;

    while (true) {
        if (await check()) {
            return;
        }

        if (Date.now() >= deadline) {
            throw new Error("Kafka fixture cleanup timed out");
        }

        await delay(250);
    }
}

async function main() {
    await admin.connect();
    adminConnected = true;

    assert.equal(
        (await admin.listTopics()).includes(topic),
        false
    );

    const existingGroups = await admin.listGroups();

    assert.equal(
        existingGroups.groups.some(
            (group) => group.groupId === groupId
        ),
        false
    );

    // The unique name was absent before this creation attempt.
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
                received.push({
                    topic: receivedTopic,
                    partition,
                    offset: message.offset,
                    key: message.key?.toString("utf8"),
                    payload: JSON.parse(
                        message.value.toString("utf8")
                    ),
                });

                if (received.length >= 2) {
                    receivedReady.resolve(
                        received.slice(0, 2)
                    );
                }
            } catch (error) {
                receivedReady.reject(error);
            }
        },
    });

    await withDeadline(joined.promise, "Consumer group join");
    await producer.connect();

    console.log("Isolated Kafka topic and consumer ready: OK");

    const eventId = randomUUID();

    const payload = {
        eventId,
        eventType: "OrderCreated",
        version: 1,
        oid: 101,
        uid: 13,
        discount: "2.55",
        items: [
            { pid: 22, qty: 2, unit_price: "25.50" },
        ],
    };

    const event = {
        event_id: eventId,
        event_type: "OrderCreated",
        oid: 101,
        payload,
    };

    // Intentionally send the same event twice.
    for (let index = 0; index < 2; index += 1) {
        const metadata = await producer.publish(event);

        assert.ok(Array.isArray(metadata));
        assert.ok(metadata.length > 0);

        for (const record of metadata) {
            assert.equal(record.topicName, topic);
            assert.equal(record.partition, 0);
            assert.equal(record.errorCode, 0);
        }
    }

    console.log("Live Kafka producer acknowledgments: OK");

    const messages = await withDeadline(
        receivedReady.promise,
        "OrderCreated roundtrip"
    );

    for (const message of messages) {
        assert.equal(message.topic, topic);
        assert.equal(message.partition, 0);
        assert.equal(message.key, "101");
        assert.deepEqual(message.payload, payload);

        assert.equal(
            typeof message.payload.discount,
            "string"
        );

        assert.equal(
            typeof message.payload.items[0].unit_price,
            "string"
        );
    }

    console.log(
        "Live OrderCreated key and decimal payload: OK"
    );

    assert.equal(
        messages[0].payload.eventId,
        messages[1].payload.eventId
    );

    assert.ok(
        BigInt(messages[1].offset) >
        BigInt(messages[0].offset)
    );

    console.log(
        "Repeated event identity and distinct Kafka offsets: OK"
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
        "Live Order event check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
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

    let groupRemoved = true;
    let topicRemoved = true;

    if (adminConnected && groupOwned) {
        groupRemoved = await cleanupStep(
            "Isolated consumer group cleanup",
            async () => {
                const result = await admin.listGroups();

                if (
                    result.groups.some(
                        (group) => group.groupId === groupId
                    )
                ) {
                    await admin.deleteGroups([groupId]);
                }

                await waitUntil(async () => {
                    const remaining = await admin.listGroups();

                    return !remaining.groups.some(
                        (group) => group.groupId === groupId
                    );
                });
            }
        );
    }

    if (adminConnected && topicOwned) {
        topicRemoved = await cleanupStep(
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
        groupRemoved &&
        topicRemoved
    ) {
        console.log(
            "Isolated Kafka topic and group removed: OK"
        );
    }

    const adminClosed = await cleanupStep(
        "Admin disconnect",
        () => admin.disconnect()
    );

    if (
        stopped &&
        consumerClosed &&
        producerClosed &&
        adminClosed
    ) {
        console.log("Kafka test connections closed: OK");
    }

    if (process.exitCode) {
        console.error("Fixture Kafka topic:", topic);
        console.error("Fixture consumer group:", groupId);
    }
}