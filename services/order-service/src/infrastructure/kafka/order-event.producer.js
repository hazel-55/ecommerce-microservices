import kafkaJs from "kafkajs";
import { DomainError } from "../../domain/errors.js";
import { createKafkaClient } from "./client.js";
import { orderCreatedTopic } from "./topics.js";

const { Partitioners } = kafkaJs;

const MAX_ID = 2147483647;

const UUID_PATTERN =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function isObject(value) {
    return (
        value !== null &&
        typeof value === "object" &&
        !Array.isArray(value)
    );
}

function isValidId(value) {
    return (
        Number.isInteger(value) &&
        value > 0 &&
        value <= MAX_ID
    );
}

function invalidEvent() {
    throw new DomainError(
        "INTERNAL",
        "Invalid stored OrderCreated event"
    );
}

function eventPayload(event) {
    if (
        !isObject(event) ||
        event.event_type !== "OrderCreated" ||
        typeof event.event_id !== "string" ||
        !UUID_PATTERN.test(event.event_id) ||
        !isValidId(event.oid)
    ) {
        invalidEvent();
    }

    const payload = event.payload;

    if (
        !isObject(payload) ||
        payload.eventId !== event.event_id ||
        payload.eventType !== "OrderCreated" ||
        payload.version !== 1 ||
        payload.oid !== event.oid ||
        !isValidId(payload.uid) ||
        typeof payload.discount !== "string" ||
        !/^(0|[1-9]\d{0,15})\.\d{2}$/.test(payload.discount) ||
        !Array.isArray(payload.items) ||
        payload.items.length === 0
    ) {
        invalidEvent();
    }

    const productIds = new Set();

    const items = payload.items.map((item) => {
        if (
            !isObject(item) ||
            !isValidId(item.pid) ||
            !isValidId(item.qty) ||
            productIds.has(item.pid) ||
            typeof item.unit_price !== "string" ||
            !/^(0|[1-9]\d{0,7})\.\d{2}$/.test(item.unit_price)
        ) {
            invalidEvent();
        }

        productIds.add(item.pid);

        return {
            pid: item.pid,
            qty: item.qty,
            unit_price: item.unit_price,
        };
    });

    return {
        eventId: payload.eventId,
        eventType: payload.eventType,
        version: payload.version,
        oid: payload.oid,
        uid: payload.uid,
        discount: payload.discount,
        items,
    };
}

export function createOrderEventProducer({
    kafka = createKafkaClient(),
    topic = orderCreatedTopic,
} = {}) {
    if (
        typeof topic !== "string" ||
        !/^[A-Za-z0-9][A-Za-z0-9._-]{0,248}$/.test(topic)
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid Order event topic"
        );
    }

    const producer = kafka.producer({
        createPartitioner: Partitioners.DefaultPartitioner,
        allowAutoTopicCreation: false,
        maxInFlightRequests: 1,
    });

    return {
        async connect() {
            await producer.connect();
        },

        async publish(event) {
            const payload = eventPayload(event);

            return await producer.send({
                topic,
                acks: -1,
                timeout: 10000,
                messages: [
                    {
                        key: String(payload.oid),
                        value: JSON.stringify(payload),
                    },
                ],
            });
        },

        async disconnect() {
            await producer.disconnect();
        },
    };
}