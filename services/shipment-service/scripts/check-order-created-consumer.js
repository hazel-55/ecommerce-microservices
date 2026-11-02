import assert from "node:assert/strict";
import { kafkaConfig } from "../src/infrastructure/config/env.js";
import {
    orderCreatedTopic,
} from "../src/infrastructure/kafka/topics.js";
import {
    createOrderCreatedConsumer,
} from "../src/infrastructure/kafka/order-created.consumer.js";

function fixture({
    handler = async () => ({ outcome: "CREATED" }),
    connect = async () => { },
    subscribe = async () => { },
    commit = async () => { },
    stop = async () => { },
    disconnect = async () => { },
} = {}) {
    const state = {
        calls: [],
        commits: [],
        subscription: null,
        runOptions: null,
        consumerOptions: null,
        listener: null,
        unsubscribed: false,
    };

    const sdk = {
        events: {
            CRASH: "consumer.crash",
        },

        async connect() {
            state.calls.push("connect");
            await connect();
        },

        async subscribe(options) {
            state.calls.push("subscribe");
            state.subscription = options;
            await subscribe(options);
        },

        async run(options) {
            state.calls.push("run");
            state.runOptions = options;
        },

        async commitOffsets(offsets) {
            // Verify the SDK method retains its receiver.
            assert.equal(this, sdk);

            await commit(offsets);
            state.commits.push(structuredClone(offsets));
        },

        async stop() {
            state.calls.push("stop");
            await stop();
        },

        async disconnect() {
            state.calls.push("disconnect");
            await disconnect();
        },

        on(event, listener) {
            assert.equal(event, this.events.CRASH);
            state.listener = listener;

            return () => {
                state.unsubscribed = true;
                state.listener = null;
            };
        },
    };

    const kafka = {
        consumer(options) {
            state.calls.push("create");
            state.consumerOptions = options;
            return sdk;
        },
    };

    const adapter = createOrderCreatedConsumer({
        kafka,
        handler,
    });

    return { adapter, state, kafka };
}

function batchPayload() {
    const resolved = [];

    return {
        resolved,

        payload: {
            batch: {
                topic: orderCreatedTopic,
                partition: 0,
                messages: [
                    {
                        offset: "5",
                        key: Buffer.from("1001"),
                        value: Buffer.from("{}"),
                    },
                ],
            },

            resolveOffset(offset) {
                resolved.push(offset);
            },

            async heartbeat() { },

            isRunning: () => true,
            isStale: () => false,
        },
    };
}

async function main() {
    const configured = fixture();

    assert.deepEqual(configured.state.consumerOptions, {
        groupId: kafkaConfig.groupId,
        allowAutoTopicCreation: false,
        sessionTimeout: 60_000,
        heartbeatInterval: 3000,
    });

    assert.equal(
        configured.state.consumerOptions.groupId,
        "shipment-service-v1"
    );

    assert.deepEqual(configured.state.calls, ["create"]);

    console.log(
        "Stable consumer group and topic creation settings: OK"
    );

    const connectGate = Promise.withResolvers();

    const connecting = fixture({
        connect: () => connectGate.promise,
    });

    const pendingConnect = connecting.adapter.connect();

    assert.deepEqual(
        connecting.state.calls,
        ["create", "connect"]
    );

    connectGate.resolve();
    await pendingConnect;

    assert.deepEqual(
        connecting.state.calls,
        ["create", "connect", "subscribe"]
    );

    assert.deepEqual(connecting.state.subscription, {
        topics: [orderCreatedTopic],
        fromBeginning: true,
    });

    const connectionError = new Error("Connection failed");

    const failedConnection = fixture({
        async connect() {
            throw connectionError;
        },
    });

    await assert.rejects(
        () => failedConnection.adapter.connect(),
        (error) => error === connectionError
    );

    assert.equal(
        failedConnection.state.calls.includes("subscribe"),
        false
    );

    const subscriptionError = new Error("Subscription failed");

    const failedSubscription = fixture({
        async subscribe() {
            throw subscriptionError;
        },
    });

    await assert.rejects(
        () => failedSubscription.adapter.connect(),
        (error) => error === subscriptionError
    );

    console.log(
        "Connection completes before subscription and errors propagate: OK"
    );

    let handledRecord;

    const running = fixture({
        async handler(record) {
            handledRecord = record;
            return { outcome: "CREATED" };
        },
    });

    await running.adapter.connect();
    await running.adapter.run();

    const options = running.state.runOptions;

    assert.equal(options.autoCommit, false);
    assert.equal(options.eachBatchAutoResolve, false);
    assert.equal(options.partitionsConsumedConcurrently, 1);
    assert.equal(typeof options.eachBatch, "function");

    const batch = batchPayload();

    await options.eachBatch(batch.payload);

    assert.equal(handledRecord.topic, orderCreatedTopic);
    assert.equal(handledRecord.partition, 0);
    assert.equal(handledRecord.message.offset, "5");

    assert.deepEqual(running.state.commits, [
        [
            {
                topic: orderCreatedTopic,
                partition: 0,
                offset: "6",
            },
        ],
    ]);

    assert.deepEqual(batch.resolved, ["5"]);

    console.log(
        "Manual commit settings and batch offset wiring: OK"
    );

    const handlerError = new Error("Persistence failed");

    const failedHandler = fixture({
        async handler() {
            throw handlerError;
        },
    });

    await failedHandler.adapter.run();

    const failedHandlerBatch = batchPayload();

    await assert.rejects(
        () => failedHandler.state.runOptions.eachBatch(
            failedHandlerBatch.payload
        ),
        (error) => error === handlerError
    );

    assert.deepEqual(failedHandler.state.commits, []);
    assert.deepEqual(failedHandlerBatch.resolved, []);

    const commitError = new Error("Commit failed");

    const failedCommit = fixture({
        async commit() {
            throw commitError;
        },
    });

    await failedCommit.adapter.run();

    const failedCommitBatch = batchPayload();

    await assert.rejects(
        () => failedCommit.state.runOptions.eachBatch(
            failedCommitBatch.payload
        ),
        (error) => error === commitError
    );

    assert.deepEqual(failedCommitBatch.resolved, []);

    console.log(
        "Handler and commit failures propagate through consumer callback: OK"
    );

    const stopGate = Promise.withResolvers();
    const disconnectGate = Promise.withResolvers();

    const lifecycle = fixture({
        stop: () => stopGate.promise,
        disconnect: () => disconnectGate.promise,
    });

    let stopped = false;

    const pendingStop = lifecycle.adapter.stop().then(() => {
        stopped = true;
    });

    assert.equal(stopped, false);
    assert.equal(lifecycle.state.calls.at(-1), "stop");

    stopGate.resolve();
    await pendingStop;
    assert.equal(stopped, true);

    let disconnected = false;

    const pendingDisconnect =
        lifecycle.adapter.disconnect().then(() => {
            disconnected = true;
        });

    assert.equal(disconnected, false);

    disconnectGate.resolve();
    await pendingDisconnect;
    assert.equal(disconnected, true);

    let observedCrash;

    const unsubscribe = lifecycle.adapter.onCrash((event) => {
        observedCrash = event;
    });

    const crashEvent = {
        payload: {
            error: new Error("Consumer crashed"),
            restart: false,
        },
    };

    lifecycle.state.listener(crashEvent);
    assert.equal(observedCrash, crashEvent);

    unsubscribe();
    assert.equal(lifecycle.state.unsubscribed, true);

    console.log(
        "Stop, disconnect and crash listener delegation: OK"
    );

    let allocations = 0;

    const unusedKafka = {
        consumer() {
            allocations += 1;
            throw new Error("Must not allocate");
        },
    };

    for (const override of [
        { groupId: "" },
        { groupId: "invalid group" },
        { topic: "" },
        { topic: "invalid topic" },
        { handler: null },
    ]) {
        assert.throws(
            () => createOrderCreatedConsumer({
                kafka: unusedKafka,
                handler: async () => ({ outcome: "CREATED" }),
                ...override,
            }),
            TypeError
        );
    }

    assert.equal(allocations, 0);

    assert.throws(
        () => createOrderCreatedConsumer({
            kafka: null,
            handler: async () => ({ outcome: "CREATED" }),
        }),
        TypeError
    );

    assert.throws(
        () => lifecycle.adapter.onCrash(null),
        TypeError
    );

    console.log(
        "Invalid consumer options and crash listeners rejected: OK"
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "OrderCreated consumer check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
}