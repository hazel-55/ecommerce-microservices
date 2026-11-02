import { pool } from "./database/pool.js";
import { outboxWorkerConfig } from "./config/env.js";
import {
    createOutboxRepository,
} from "./repositories/outbox.repository.js";
import { createKafkaClient } from "./kafka/client.js";
import { orderCreatedTopic } from "./kafka/topics.js";

export async function checkOutboxReadiness({
    database = pool,
    kafka = createKafkaClient(),
} = {}) {
    await database.query(`
        SELECT
            e.event_id,
            e.event_type,
            e.oid,
            e.payload,
            e.created_at,
            e.published_at,
            e.attempts,
            e.last_error,
            e.next_attempt_at,
            o.operation_id,
            o.uid,
            t.status
        FROM outbox_events e
        JOIN orders o ON o.oid = e.oid
        JOIN order_creation_tasks t
            ON t.operation_id = o.operation_id
           AND t.uid = o.uid
        LIMIT 0
    `);

    const admin = kafka.admin();

    try {
        await admin.connect();

        const topics = await admin.listTopics();

        if (!topics.includes(orderCreatedTopic)) {
            throw new Error(
                "Required OrderCreated topic is missing"
            );
        }

        const metadata = await admin.fetchTopicMetadata({
            topics: [orderCreatedTopic],
        });

        const topic = metadata.topics.find(
            (item) => item.name === orderCreatedTopic
        );

        if (
            !topic ||
            topic.partitions.length === 0 ||
            topic.partitions.some(
                (partition) =>
                    partition.partitionErrorCode !== 0 ||
                    !Number.isInteger(partition.leader) ||
                    partition.leader < 0 ||
                    !partition.isr.includes(partition.leader)
            )
        ) {
            throw new Error(
                "OrderCreated topic is not ready"
            );
        }
    } finally {
        await admin.disconnect();
    }

    const candidateLimit = outboxWorkerConfig.batchSize;

    const candidates = await createOutboxRepository({
        database,
    }).listDueEventIds({
        limit: candidateLimit,
    });

    return {
        candidateCount: candidates.length,
        candidateLimit,
    };
}