import assert from "node:assert/strict";
import {
    kafkaConfig,
} from "../src/infrastructure/config/env.js";
import {
    createKafkaClient,
} from "../src/infrastructure/kafka/client.js";
import {
    orderCreatedTopic,
} from "../src/infrastructure/kafka/topics.js";

const admin = createKafkaClient().admin();

try {
    assert.equal(kafkaConfig.clientId, "shipment-service");
    assert.equal(kafkaConfig.groupId, "shipment-service-v1");
    assert.ok(Array.isArray(kafkaConfig.brokers));
    assert.ok(kafkaConfig.brokers.length > 0);

    console.log("Shipment Kafka client configuration: OK");

    await admin.connect();

    const cluster = await admin.describeCluster();

    assert.ok(cluster.brokers.length > 0);
    assert.equal(typeof cluster.clusterId, "string");
    assert.ok(cluster.clusterId.length > 0);
    assert.ok(Number.isInteger(cluster.controller));

    assert.ok(
        cluster.brokers.some(
            (broker) => broker.nodeId === cluster.controller
        )
    );

    console.log("Shipment Kafka broker metadata: OK");

    const topics = await admin.listTopics();

    assert.ok(
        topics.includes(orderCreatedTopic),
        "OrderCreated topic is missing"
    );

    const metadata = await admin.fetchTopicMetadata({
        topics: [orderCreatedTopic],
    });

    const topic = metadata.topics.find(
        (item) => item.name === orderCreatedTopic
    );

    assert.ok(topic);
    assert.ok(topic.partitions.length > 0);

    for (const partition of topic.partitions) {
        assert.equal(partition.partitionErrorCode, 0);
        assert.ok(Number.isInteger(partition.leader));
        assert.ok(partition.leader >= 0);

        assert.ok(
            partition.replicas.includes(partition.leader)
        );

        assert.ok(
            partition.isr.includes(partition.leader)
        );
    }

    console.log("OrderCreated topic and leader readiness: OK");
} catch (error) {
    console.error(
        "Shipment Kafka check failed:",
        error.code ?? error.name
    );

    process.exitCode = 1;
} finally {
    try {
        await admin.disconnect();

        console.log("Shipment Kafka admin disconnected: OK");
    } catch (error) {
        console.error(
            "Shipment Kafka disconnect failed:",
            error.code ?? error.name
        );

        process.exitCode = 1;
    }
}