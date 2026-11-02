import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import {
    createOrderEventProducer,
} from "../src/infrastructure/kafka/order-event.producer.js";
import {
    orderCreatedTopic,
} from "../src/infrastructure/kafka/topics.js";

let producerOptions;
let connectCalls = 0;
let disconnectCalls = 0;
let sendHandler = async () => [];

const requests = [];

const transport = {
    async connect() {
        connectCalls += 1;
    },

    async send(request) {
        requests.push(structuredClone(request));
        return await sendHandler(request);
    },

    async disconnect() {
        disconnectCalls += 1;
    },
};

const producer = createOrderEventProducer({
    kafka: {
        producer(options) {
            producerOptions = options;
            return transport;
        },
    },
});

const eventId = randomUUID();

const event = {
    event_id: eventId,
    event_type: "OrderCreated",
    oid: 101,
    attempts: 2,
    payload: {
        eventId,
        eventType: "OrderCreated",
        version: 1,
        oid: 101,
        uid: 13,
        discount: "2.55",
        internalDebug: "excluded",
        items: [
            {
                pid: 22,
                qty: 2,
                unit_price: "25.50",
                reservedQuantity: 2,
            },
        ],
    },
};

const expectedPayload = {
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

async function main() {
    assert.equal(
        producerOptions.allowAutoTopicCreation,
        false
    );
    assert.equal(producerOptions.maxInFlightRequests, 1);
    assert.equal(
        typeof producerOptions.createPartitioner,
        "function"
    );

    await producer.connect();
    assert.equal(connectCalls, 1);

    console.log("Producer configuration and connection: OK");

    const original = structuredClone(event);

    await producer.publish(event);

    const request = requests[0];

    assert.equal(request.topic, orderCreatedTopic);
    assert.equal(request.acks, -1);
    assert.equal(request.timeout, 10000);
    assert.equal(request.messages.length, 1);
    assert.equal(request.messages[0].key, "101");

    assert.deepEqual(
        JSON.parse(request.messages[0].value),
        expectedPayload
    );

    assert.deepEqual(event, original);

    console.log(
        "OrderCreated key, identity and decimal serialization: OK"
    );

    const mutations = [
        (value) => { value.event_type = "OtherEvent"; },
        (value) => { value.event_id = randomUUID(); },
        (value) => { value.oid = 999; },
        (value) => { value.payload.version = 2; },
        (value) => { value.payload.uid = 0; },
        (value) => { value.payload.discount = 2.55; },
        (value) => { value.payload.items = []; },
        (value) => { value.payload.items[0].qty = 0; },
        (value) => {
            value.payload.items[0].unit_price = "25.500";
        },
        (value) => {
            value.payload.items.push(
                structuredClone(value.payload.items[0])
            );
        },
    ];

    const invalidEvents = [null, {}];

    for (const mutate of mutations) {
        const invalid = structuredClone(event);
        mutate(invalid);
        invalidEvents.push(invalid);
    }

    const sendsBeforeInvalid = requests.length;

    for (const invalid of invalidEvents) {
        await assert.rejects(
            () => producer.publish(invalid),
            (error) => {
                assert.ok(error instanceof DomainError);
                assert.equal(error.code, "INTERNAL");
                return true;
            }
        );
    }

    assert.equal(requests.length, sendsBeforeInvalid);

    console.log("Invalid stored events rejected before send: OK");

    const acknowledgment = Promise.withResolvers();

    const metadata = [
        {
            topicName: orderCreatedTopic,
            partition: 0,
            errorCode: 0,
            baseOffset: "7",
        },
    ];

    sendHandler = () => acknowledgment.promise;

    let settled = false;

    const pending = producer.publish(event);

    void pending.then(
        () => { settled = true; },
        () => { settled = true; }
    );

    await Promise.resolve();
    assert.equal(settled, false);

    acknowledgment.resolve(metadata);

    assert.deepEqual(await pending, metadata);

    console.log("Producer waits for Kafka acknowledgment: OK");

    const transportError = new Error("Simulated send failure");

    sendHandler = async () => {
        throw transportError;
    };

    await assert.rejects(
        () => producer.publish(event),
        (error) => error === transportError
    );

    sendHandler = async () => metadata;

    await producer.publish(event);

    const replay = requests.at(-1);

    assert.equal(replay.messages[0].key, "101");
    assert.equal(
        replay.messages[0].value,
        request.messages[0].value
    );

    assert.deepEqual(event, original);

    console.log(
        "Send failure propagated and replay identity preserved: OK"
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Order event producer check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        await producer.disconnect();
        assert.equal(disconnectCalls, 1);

        console.log("Producer disconnected: OK");
    } catch (error) {
        console.error(
            "Producer cleanup failed:",
            error.code ?? error.message
        );

        process.exitCode = 1;
    }
}