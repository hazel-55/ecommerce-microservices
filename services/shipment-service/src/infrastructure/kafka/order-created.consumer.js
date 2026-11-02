import { kafkaConfig } from "../config/env.js";
import { createKafkaClient } from "./client.js";
import { orderCreatedTopic } from "./topics.js";
import {
    createOrderCreatedHandler,
} from "./order-created.handler.js";
import {
    createOrderCreatedBatchHandler,
} from "./order-created.batch.js";

const NAME_PATTERN =
    /^[A-Za-z0-9][A-Za-z0-9._-]{0,248}$/;

export function createOrderCreatedConsumer({
    kafka = createKafkaClient(),
    groupId = kafkaConfig.groupId,
    topic = orderCreatedTopic,
    handler = createOrderCreatedHandler({ topic }),
} = {}) {
    if (!kafka || typeof kafka.consumer !== "function") {
        throw new TypeError("Kafka consumer factory is required");
    }

    if (
        typeof groupId !== "string" ||
        !NAME_PATTERN.test(groupId)
    ) {
        throw new TypeError("Invalid consumer group ID");
    }

    if (
        typeof topic !== "string" ||
        !NAME_PATTERN.test(topic)
    ) {
        throw new TypeError("Invalid consumer topic");
    }

    if (typeof handler !== "function") {
        throw new TypeError("Record handler must be a function");
    }

    const consumer = kafka.consumer({
        groupId,
        allowAutoTopicCreation: false,
        sessionTimeout: 60_000,
        heartbeatInterval: 3000,
    });

    const eachBatch = createOrderCreatedBatchHandler({
        handler,

        commitOffsets(offsets) {
            return consumer.commitOffsets(offsets);
        },
    });

    return {
        async connect() {
            await consumer.connect();

            await consumer.subscribe({
                topics: [topic],
                fromBeginning: true,
            });
        },

        async run() {
            await consumer.run({
                autoCommit: false,
                eachBatchAutoResolve: false,
                partitionsConsumedConcurrently: 1,
                eachBatch,
            });
        },

        async stop() {
            await consumer.stop();
        },

        async disconnect() {
            await consumer.disconnect();
        },

        onCrash(listener) {
            if (typeof listener !== "function") {
                throw new TypeError(
                    "Consumer crash listener must be a function"
                );
            }

            return consumer.on(
                consumer.events.CRASH,
                listener
            );
        },
    };
}