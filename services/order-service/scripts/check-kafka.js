import assert from "node:assert/strict";
import {
    kafkaConfig,
} from "../src/infrastructure/config/env.js";
import {
    createKafkaClient,
} from "../src/infrastructure/kafka/client.js";

const kafka = createKafkaClient();
const admin = kafka.admin();

let connected = false;

async function main() {
    assert.equal(kafkaConfig.clientId, "order-service");
    assert.ok(Array.isArray(kafkaConfig.brokers));
    assert.ok(kafkaConfig.brokers.length >= 1);

    console.log("Kafka client configuration: OK");

    await admin.connect();
    connected = true;

    const cluster = await admin.describeCluster();

    assert.ok(Array.isArray(cluster.brokers));
    assert.ok(cluster.brokers.length >= 1);
    assert.equal(typeof cluster.clusterId, "string");
    assert.ok(cluster.clusterId.length > 0);
    assert.ok(Number.isInteger(cluster.controller));

    assert.ok(
        cluster.brokers.some(
            (broker) =>
                broker.nodeId === cluster.controller,
        ),
    );

    for (const broker of cluster.brokers) {
        assert.equal(typeof broker.host, "string");
        assert.ok(broker.host.length > 0);
        assert.ok(Number.isInteger(broker.port));
        assert.ok(broker.port >= 1);
        assert.ok(broker.port <= 65535);
    }

    console.log(
        "Kafka broker connection and cluster metadata: OK",
    );

    const metadata = await admin.fetchTopicMetadata();

    assert.ok(Array.isArray(metadata.topics));

    console.log("Kafka topic metadata request: OK");
    console.log("Visible topics:", metadata.topics.length);
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Kafka check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    try {
        await admin.disconnect();

        if (connected) {
            console.log("Kafka admin disconnected: OK");
        }
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Kafka disconnect failed:",
            error.code ?? error.name,
            error.message,
        );
    }
}