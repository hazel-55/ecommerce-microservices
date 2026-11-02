import assert from "node:assert/strict";
import {
    createKafkaClient,
} from "../src/infrastructure/kafka/client.js";
import {
    orderCreatedTopic,
} from "../src/infrastructure/kafka/topics.js";

const admin = createKafkaClient().admin();

async function main() {
    await admin.connect();

    const topicNames = await admin.listTopics();

    assert.ok(
        topicNames.includes(orderCreatedTopic),
        `Required Kafka topic is missing: ${orderCreatedTopic}`
    );

    console.log("OrderCreated topic exists: OK");

    const metadata = await admin.fetchTopicMetadata({
        topics: [orderCreatedTopic],
    });

    const topic = metadata.topics.find(
        (item) => item.name === orderCreatedTopic
    );

    assert.ok(topic);
    assert.equal(topic.partitions.length, 1);

    const partition = topic.partitions[0];

    assert.equal(partition.partitionErrorCode, 0);
    assert.equal(partition.partitionId, 0);
    assert.equal(partition.replicas.length, 1);

    console.log("OrderCreated partition and replication: OK");

    assert.ok(Number.isInteger(partition.leader));
    assert.ok(partition.leader >= 0);
    assert.ok(
        partition.replicas.includes(partition.leader)
    );
    assert.ok(
        partition.isr.includes(partition.leader)
    );

    console.log("OrderCreated leader and in-sync replica: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Kafka topic check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        await admin.disconnect();
        console.log("Kafka topic admin disconnected: OK");
    } catch (error) {
        console.error(
            "Kafka topic admin disconnect failed:",
            error.code ?? error.message
        );

        process.exitCode = 1;
    }
}