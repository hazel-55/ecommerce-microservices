import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import kafkaJs from "kafkajs";

import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import {
    ShipmentModel,
} from "../src/infrastructure/database/models/shipment.model.js";
import {
    CounterModel,
} from "../src/infrastructure/database/models/counter.model.js";
import {
    createShipmentIdAllocator,
} from "../src/infrastructure/database/shipment-id.js";
import {
    createShipmentRepository,
} from "../src/infrastructure/repositories/shipment.repository.js";
import {
    createShipmentApplication,
} from "../src/application/shipment.application.js";
import {
    createKafkaClient,
} from "../src/infrastructure/kafka/client.js";
import {
    createOrderCreatedHandler,
} from "../src/infrastructure/kafka/order-created.handler.js";
import {
    createOrderCreatedConsumer,
} from "../src/infrastructure/kafka/order-created.consumer.js";

const {
    Partitioners,
    KafkaJSNonRetriableError,
} = kafkaJs;

const fixtureId = randomUUID();
const suffix = fixtureId.replaceAll("-", "");

const topic = `shipment-check-${fixtureId}`;
const groupId = `shipment-check-group-${fixtureId}`;

const shipmentCollection = `shipment_check_${suffix}`;
const counterCollection = `counter_check_${suffix}`;

const kafka = createKafkaClient({
    clientId: `shipment-integration-${fixtureId}`,
});

const admin = kafka.admin();

const producer = kafka.producer({
    allowAutoTopicCreation: false,
    createPartitioner: Partitioners.DefaultPartitioner,
});

const event = {
    eventId: randomUUID(),
    eventType: "OrderCreated",
    version: 1,
    oid: 1001,
    uid: 13,
    discount: "0.00",
    items: [
        {
            pid: 1,
            qty: 2,
            unit_price: "25.50",
        },
    ],
};

let connection;
let ShipmentFixture;
let CounterFixture;
let consumer;
let removeCrashListener;
let crashError;

let adminConnected = false;
let topicCreated = false;
let shipmentCollectionCreated = false;
let counterCollectionCreated = false;

const outcomes = [];

function reportFixtures() {
    console.error("Integration fixture identifiers:", {
        topic,
        groupId,
        shipmentCollection,
        counterCollection,
    });
}

async function publishEvent(input = event) {
    await producer.send({
        topic,
        acks: -1,
        messages: [
            {
                key: String(input.oid),
                value: JSON.stringify(input),
            },
        ],
    });

    const offsets = await admin.fetchTopicOffsets(topic);
    const partition = offsets.find(
        (item) => item.partition === 0
    );

    assert.ok(partition);

    return partition.offset;
}

async function committedOffset() {
    const offsets = await admin.fetchOffsets({
        groupId,
        topics: [topic],
    });

    return offsets
        .find((item) => item.topic === topic)
        ?.partitions.find((item) => item.partition === 0)
        ?.offset;
}

async function waitForCommit(expected) {
    const deadline = Date.now() + 60_000;

    while (Date.now() < deadline) {
        if (crashError) {
            throw crashError;
        }

        const offset = await committedOffset();

        if (offset === expected) {
            return;
        }

        await delay(200);
    }

    throw new Error(
        `Consumer did not commit expected offset ${expected}`
    );
}

async function closeConsumer() {
    if (!consumer) {
        return;
    }

    await consumer.stop();

    removeCrashListener?.();
    removeCrashListener = undefined;

    await consumer.disconnect();
    consumer = undefined;
}

async function main() {
    connection = await connectDatabase();
    assert.equal(connection.name, "shipment_db");

    // Reuse the real schemas with isolated collection names.
    ShipmentFixture = connection.model(
        `ShipmentCheck_${suffix}`,
        ShipmentModel.schema.clone(),
        shipmentCollection
    );

    CounterFixture = connection.model(
        `ShipmentCounterCheck_${suffix}`,
        CounterModel.schema.clone(),
        counterCollection
    );

    await ShipmentFixture.createCollection();
    shipmentCollectionCreated = true;

    await CounterFixture.createCollection();
    counterCollectionCreated = true;

    await ShipmentFixture.createIndexes();
    await CounterFixture.createIndexes();

    const allocateId = createShipmentIdAllocator({
        model: CounterFixture,
    });

    const repository = createShipmentRepository({
        model: ShipmentFixture,
        allocateId,
    });

    const application = createShipmentApplication({
        repository,
    });

    const handleRecord = createOrderCreatedHandler({
        application,
        topic,
    });

    await admin.connect();
    adminConnected = true;

    topicCreated = await admin.createTopics({
        waitForLeaders: true,
        topics: [
            {
                topic,
                numPartitions: 1,
                replicationFactor: 1,
            },
        ],
    });

    assert.equal(topicCreated, true);

    await producer.connect();

    console.log(
        "Isolated MongoDB and Kafka fixtures ready: OK"
    );

    // Publish before starting the new group.
    const firstNextOffset = await publishEvent();
    assert.equal(firstNextOffset, "1");

    consumer = createOrderCreatedConsumer({
        kafka,
        topic,
        groupId,

        async handler(record) {
            const result = await handleRecord(record);
            outcomes.push(result.outcome);
            return result;
        },
    });

    removeCrashListener = consumer.onCrash((notification) => {
        crashError =
            notification?.payload?.error ??
            new Error("Integration consumer crashed");
    });

    await consumer.connect();
    await consumer.run();

    await waitForCommit(firstNextOffset);

    const first = await ShipmentFixture.findOne({
        sourceEventId: event.eventId,
    }).lean();

    assert.ok(first);
    assert.ok(Number.isInteger(first.shipid));
    assert.ok(first.shipid > 0);
    assert.equal(first.oid, event.oid);
    assert.equal(first.uid, event.uid);
    assert.equal(first.status, "PENDING");

    assert.equal(
        await ShipmentFixture.countDocuments(),
        1
    );

    assert.deepEqual(outcomes, ["CREATED"]);

    console.log(
        "Live OrderCreated creates PENDING Shipment: OK"
    );

    // Change status before replaying the original event.
    await application.updateShipmentStatus(
        first.shipid,
        { status: "SHIPPING" },
        { uid: 13, role: "SHIPMENT_STAFF" }
    );

    const beforeReplay = await ShipmentFixture.findOne({
        sourceEventId: event.eventId,
    }).lean();

    assert.equal(beforeReplay.status, "SHIPPING");

    const counterBefore = await CounterFixture
        .findById("shipment")
        .lean();

    assert.ok(counterBefore);
    assert.equal(counterBefore.seq, 1);

    const secondNextOffset = await publishEvent();
    assert.equal(secondNextOffset, "2");

    await waitForCommit(secondNextOffset);

    const afterReplay = await ShipmentFixture.findOne({
        sourceEventId: event.eventId,
    }).lean();

    const counterAfter = await CounterFixture
        .findById("shipment")
        .lean();

    // Includes identity, status and stored timestamps.
    assert.deepEqual(afterReplay, beforeReplay);

    assert.equal(
        await ShipmentFixture.countDocuments(),
        1
    );

    assert.equal(counterAfter.seq, counterBefore.seq);
    assert.deepEqual(outcomes, ["CREATED", "DUPLICATE"]);

    console.log(
        "Duplicate event preserves Shipment identity and status: OK"
    );

    await closeConsumer();

    // The broker retains the committed offset after disconnect.
    assert.equal(await committedOffset(), "2");

    const topicOffsets = await admin.fetchTopicOffsets(topic);

    assert.equal(
        topicOffsets.find((item) => item.partition === 0)?.offset,
        "2"
    );

    console.log(
        "Kafka group commits the next processed offsets: OK"
    );
    // The first consumer is already stopped and disconnected.
    const pendingEvent = {
        ...event,
        eventId: randomUUID(),
        oid: event.oid + 1,
    };

    const pendingNextOffset = await publishEvent(pendingEvent);

    assert.equal(pendingNextOffset, "3");
    assert.equal(await committedOffset(), "2");

    assert.equal(
        await ShipmentFixture.findOne({
            sourceEventId: pendingEvent.eventId,
        }).lean(),
        null
    );

    assert.equal(
        await ShipmentFixture.countDocuments(),
        1
    );

    console.log(
        "Pending event waits while the consumer is stopped: OK"
    );

    const restartedOffsets = [];
    crashError = undefined;

    // New consumer instance, same topic and consumer group.
    consumer = createOrderCreatedConsumer({
        kafka,
        topic,
        groupId,

        async handler(record) {
            restartedOffsets.push(record.message.offset);

            const result = await handleRecord(record);

            outcomes.push(result.outcome);
            return result;
        },
    });

    removeCrashListener = consumer.onCrash((notification) => {
        crashError =
            notification?.payload?.error ??
            new Error("Restarted integration consumer crashed");
    });

    await consumer.connect();
    await consumer.run();

    await waitForCommit(pendingNextOffset);

    // Offsets 0 and 1 must not be processed again.
    assert.deepEqual(restartedOffsets, ["2"]);

    assert.deepEqual(outcomes, [
        "CREATED",
        "DUPLICATE",
        "CREATED",
    ]);

    const recovered = await ShipmentFixture.findOne({
        sourceEventId: pendingEvent.eventId,
    }).lean();

    assert.ok(recovered);
    assert.ok(Number.isInteger(recovered.shipid));
    assert.ok(recovered.shipid > 0);
    assert.notEqual(recovered.shipid, first.shipid);

    assert.equal(recovered.oid, pendingEvent.oid);
    assert.equal(recovered.uid, pendingEvent.uid);
    assert.equal(recovered.status, "PENDING");

    const originalAfterRestart = await ShipmentFixture.findOne({
        sourceEventId: event.eventId,
    }).lean();

    assert.deepEqual(originalAfterRestart, beforeReplay);

    assert.equal(
        await ShipmentFixture.countDocuments(),
        2
    );

    const counterAfterRestart = await CounterFixture
        .findById("shipment")
        .lean();

    assert.ok(counterAfterRestart);
    assert.equal(counterAfterRestart.seq, 2);

    await closeConsumer();

    assert.equal(await committedOffset(), "3");

    console.log(
        "Consumer restart resumes from the committed offset: OK"
    );
    const faultEvent = {
        ...event,
        eventId: randomUUID(),
        oid: event.oid + 2,
    };

    const faultNextOffset = await publishEvent(faultEvent);

    assert.equal(faultNextOffset, "4");
    assert.equal(await committedOffset(), "3");

    const injectedFailure = new KafkaJSNonRetriableError(
        "Injected failure after Shipment persistence"
    );

    const faultOffsets = [];
    let persistedShipid;
    let faultCrashPayload;

    consumer = createOrderCreatedConsumer({
        kafka,
        topic,
        groupId,

        async handler(record) {
            faultOffsets.push(record.message.offset);

            const result = await handleRecord(record);

            assert.equal(result.outcome, "CREATED");
            persistedShipid = result.shipid;

            // MongoDB has persisted the Shipment.
            // The batch has not committed this offset yet.
            throw injectedFailure;
        },
    });

    removeCrashListener = consumer.onCrash((notification) => {
        faultCrashPayload = notification.payload;
    });

    await consumer.connect();
    await consumer.run();

    const crashDeadline = Date.now() + 60_000;

    while (
        !faultCrashPayload &&
        Date.now() < crashDeadline
    ) {
        await delay(100);
    }

    assert.ok(
        faultCrashPayload,
        "Expected injected consumer crash was not observed"
    );

    assert.equal(faultCrashPayload.restart, false);

    // Accept the injected error itself or an SDK wrapper with cause.
    let observedError = faultCrashPayload.error;

    for (
        let depth = 0;
        observedError &&
        observedError !== injectedFailure &&
        depth < 8;
        depth += 1
    ) {
        observedError = observedError.cause;
    }

    assert.equal(observedError, injectedFailure);
    assert.deepEqual(faultOffsets, ["3"]);

    const persistedBeforeRecovery =
        await ShipmentFixture.findOne({
            sourceEventId: faultEvent.eventId,
        }).lean();

    assert.ok(persistedBeforeRecovery);
    assert.equal(
        persistedBeforeRecovery.shipid,
        persistedShipid
    );
    assert.equal(persistedBeforeRecovery.oid, faultEvent.oid);
    assert.equal(persistedBeforeRecovery.uid, faultEvent.uid);
    assert.equal(persistedBeforeRecovery.status, "PENDING");

    assert.equal(
        await ShipmentFixture.countDocuments(),
        3
    );

    const counterBeforeRecovery = await CounterFixture
        .findById("shipment")
        .lean();

    assert.ok(counterBeforeRecovery);
    assert.equal(counterBeforeRecovery.seq, 3);

    // Persistence succeeded, but the failed message is uncommitted.
    assert.equal(await committedOffset(), "3");

    await closeConsumer();

    console.log(
        "Failure after MongoDB persistence leaves offset uncommitted: OK"
    );

    const recoveryOffsets = [];
    const recoveryOutcomes = [];

    crashError = undefined;

    consumer = createOrderCreatedConsumer({
        kafka,
        topic,
        groupId,

        async handler(record) {
            recoveryOffsets.push(record.message.offset);

            const result = await handleRecord(record);

            recoveryOutcomes.push(result.outcome);
            return result;
        },
    });

    removeCrashListener = consumer.onCrash((notification) => {
        crashError =
            notification?.payload?.error ??
            new Error("Recovery consumer crashed");
    });

    await consumer.connect();
    await consumer.run();

    await waitForCommit(faultNextOffset);

    assert.deepEqual(recoveryOffsets, ["3"]);
    assert.deepEqual(recoveryOutcomes, ["DUPLICATE"]);

    const persistedAfterRecovery =
        await ShipmentFixture.findOne({
            sourceEventId: faultEvent.eventId,
        }).lean();

    assert.deepEqual(
        persistedAfterRecovery,
        persistedBeforeRecovery
    );

    const originalAfterRecovery =
        await ShipmentFixture.findOne({
            sourceEventId: event.eventId,
        }).lean();

    assert.deepEqual(originalAfterRecovery, beforeReplay);

    assert.equal(
        await ShipmentFixture.countDocuments(),
        3
    );

    const counterAfterRecovery = await CounterFixture
        .findById("shipment")
        .lean();

    assert.ok(counterAfterRecovery);
    assert.equal(counterAfterRecovery.seq, 3);

    await closeConsumer();

    assert.equal(await committedOffset(), "4");

    console.log(
        "Restart replays persisted event without duplicate Shipment: OK"
    );
}

const cleanupErrors = [];

async function cleanupStep(label, operation) {
    try {
        await operation();
    } catch (error) {
        cleanupErrors.push(label);

        console.error(
            `${label} failed:`,
            error.code ?? error.name
        );

        process.exitCode = 1;
    }
}

async function dropFixtureCollection(model) {
    try {
        await model.collection.drop();
    } catch (error) {
        if (error.code !== 26) {
            throw error;
        }
    }
}

try {
    await main();
} catch (error) {
    console.error(
        "Shipment consumer integration check failed:",
        error.code ?? error.message
    );

    reportFixtures();
    process.exitCode = 1;
} finally {
    // Do not remove collections while a consumer may still be writing.
    try {
        await closeConsumer();
    } catch (error) {
        console.error(
            "Integration consumer shutdown failed:",
            error.code ?? error.name
        );

        reportFixtures();
        process.exit(1);
    }

    await cleanupStep(
        "Producer disconnect",
        () => producer.disconnect()
    );

    if (adminConnected) {
        await cleanupStep("Kafka group cleanup", async () => {
            const result = await admin.listGroups();

            if (
                result.groups.some(
                    (item) => item.groupId === groupId
                )
            ) {
                await admin.deleteGroups([groupId]);
            }
        });

        if (topicCreated) {
            await cleanupStep(
                "Kafka topic cleanup",
                () => admin.deleteTopics({ topics: [topic] })
            );
        }
    }

    if (shipmentCollectionCreated) {
        await cleanupStep(
            "Shipment fixture collection cleanup",
            () => dropFixtureCollection(ShipmentFixture)
        );
    }

    if (counterCollectionCreated) {
        await cleanupStep(
            "Counter fixture collection cleanup",
            () => dropFixtureCollection(CounterFixture)
        );
    }

    if (connection?.readyState === 1) {
        await cleanupStep("MongoDB cleanup verification", async () => {
            const remaining = await connection.db
                .listCollections({
                    name: {
                        $in: [
                            shipmentCollection,
                            counterCollection,
                        ],
                    },
                })
                .toArray();

            assert.equal(remaining.length, 0);
        });
    }

    if (cleanupErrors.length === 0) {
        console.log(
            "Scoped MongoDB and Kafka fixtures removed: OK"
        );
    }

    const errorsBeforeClosing = cleanupErrors.length;

    await cleanupStep(
        "Kafka admin disconnect",
        () => admin.disconnect()
    );

    await cleanupStep(
        "MongoDB disconnect",
        () => disconnectDatabase()
    );

    if (cleanupErrors.length === errorsBeforeClosing) {
        console.log("Integration connections closed: OK");
    }

    if (cleanupErrors.length > 0) {
        reportFixtures();
    }
}