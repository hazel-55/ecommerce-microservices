import {
    ShipmentModel,
} from "../database/models/shipment.model.js";
import {
    CounterModel,
} from "../database/models/counter.model.js";
import {
    createKafkaClient,
} from "../kafka/client.js";
import {
    orderCreatedTopic,
} from "../kafka/topics.js";

async function ensureCollection(model) {
    try {
        await model.createCollection();
    } catch (error) {
        // NamespaceExists: another startup may have created it.
        if (error.code !== 48) {
            throw error;
        }
    }
}

function requireIndex(
    indexes,
    name,
    field,
    unique = null
) {
    const index = indexes.find((item) => item.name === name);

    if (
        !index ||
        Object.keys(index.key).length !== 1 ||
        index.key[field] !== 1 ||
        (
            unique !== null &&
            Boolean(index.unique) !== unique
        ) ||
        (
            unique === true &&
            (
                index.sparse === true ||
                index.partialFilterExpression != null
            )
        )
    ) {
        throw new Error(`Required index is invalid: ${name}`);
    }
}

export async function prepareShipmentDatabase(connection) {
    if (
        !connection ||
        connection.readyState !== 1 ||
        !connection.db ||
        ShipmentModel.db !== connection ||
        CounterModel.db !== connection
    ) {
        throw new Error("Shipment database connection is not ready");
    }

    await connection.db.command(
        { ping: 1 },
        { maxTimeMS: 5000 }
    );

    const hello = await connection.db.command(
        { hello: 1 },
        { maxTimeMS: 5000 }
    );

    if (hello.isWritablePrimary !== true) {
        throw new Error("Shipment database is not writable");
    }

    await ensureCollection(ShipmentModel);
    await ensureCollection(CounterModel);

    await ShipmentModel.createIndexes();
    await CounterModel.createIndexes();

    const shipmentIndexes =
        await ShipmentModel.collection.listIndexes().toArray();

    const counterIndexes =
        await CounterModel.collection.listIndexes().toArray();

    requireIndex(
        shipmentIndexes,
        "shipment_shipid_unique",
        "shipid",
        true
    );

    requireIndex(
        shipmentIndexes,
        "shipment_source_event_unique",
        "sourceEventId",
        true
    );

    requireIndex(
        shipmentIndexes,
        "shipment_oid_idx",
        "oid",
        false
    );

    requireIndex(
        counterIndexes,
        "_id_",
        "_id"
    );

    return {
        databaseName: connection.name,
        collections: [
            ShipmentModel.collection.name,
            CounterModel.collection.name,
        ],
    };
}

export async function checkOrderCreatedTopic({
    kafka = createKafkaClient(),
    topic = orderCreatedTopic,
} = {}) {
    if (!kafka || typeof kafka.admin !== "function") {
        throw new TypeError("Kafka admin factory is required");
    }

    if (
        typeof topic !== "string" ||
        !/^[A-Za-z0-9][A-Za-z0-9._-]{0,248}$/.test(topic)
    ) {
        throw new TypeError("Invalid OrderCreated topic");
    }

    const admin = kafka.admin();

    try {
        await admin.connect();

        const topics = await admin.listTopics();

        if (!topics.includes(topic)) {
            throw new Error("OrderCreated topic is missing");
        }

        const metadata = await admin.fetchTopicMetadata({
            topics: [topic],
        });

        const found = metadata.topics.find(
            (item) => item.name === topic
        );

        if (!found || found.partitions.length === 0) {
            throw new Error("OrderCreated partitions are missing");
        }

        for (const partition of found.partitions) {
            if (
                partition.partitionErrorCode !== 0 ||
                !Number.isInteger(partition.leader) ||
                partition.leader < 0 ||
                !partition.replicas.includes(partition.leader) ||
                !partition.isr.includes(partition.leader)
            ) {
                throw new Error(
                    "OrderCreated partition is not ready"
                );
            }
        }

        return {
            topic,
            partitionCount: found.partitions.length,
        };
    } finally {
        await admin.disconnect();
    }
}