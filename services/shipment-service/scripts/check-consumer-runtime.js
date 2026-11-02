import assert from "node:assert/strict";
import {
    kafkaConfig,
} from "../src/infrastructure/config/env.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import {
    orderCreatedTopic,
} from "../src/infrastructure/kafka/topics.js";
import {
    prepareShipmentDatabase,
    checkOrderCreatedTopic,
} from "../src/infrastructure/runtime/consumer-readiness.js";

let databaseAttempted = false;

try {
    assert.equal(kafkaConfig.clientId, "shipment-service");
    assert.equal(kafkaConfig.groupId, "shipment-service-v1");

    assert.ok(Array.isArray(kafkaConfig.brokers));
    assert.ok(kafkaConfig.brokers.length > 0);

    for (const broker of kafkaConfig.brokers) {
        assert.equal(typeof broker, "string");
        assert.ok(broker.length > 0);
        assert.equal(/\s/.test(broker), false);
    }

    console.log("Shipment consumer runtime configuration: OK");

    databaseAttempted = true;

    const connection = await connectDatabase();

    // Verify the target before preparing collections and indexes.
    assert.equal(connection.name, "shipment_db");

    const database = await prepareShipmentDatabase(connection);

    assert.equal(database.databaseName, "shipment_db");

    assert.deepEqual(database.collections, [
        "shipments",
        "counters",
    ]);

    console.log(
        "Shipment MongoDB and required collections: OK"
    );

    console.log(
        "Shipment unique identities and counter index: OK"
    );

    const topic = await checkOrderCreatedTopic();

    assert.equal(topic.topic, orderCreatedTopic);
    assert.ok(topic.partitionCount > 0);

    console.log(
        "Shipment OrderCreated topic readiness: OK"
    );
} catch (error) {
    console.error(
        "Shipment consumer runtime check failed:",
        error.code ?? error.name
    );

    process.exitCode = 1;
} finally {
    try {
        if (databaseAttempted) {
            await disconnectDatabase();
        }

        console.log("Readiness connections closed: OK");
    } catch (error) {
        console.error(
            "Readiness connection cleanup failed:",
            error.code ?? error.name
        );

        process.exitCode = 1;
    }
}