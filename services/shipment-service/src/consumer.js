import pino from "pino";
import {
    kafkaConfig,
} from "./infrastructure/config/env.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "./infrastructure/database/mongoose.js";
import {
    prepareShipmentDatabase,
    checkOrderCreatedTopic,
} from "./infrastructure/runtime/consumer-readiness.js";
import {
    createOrderCreatedConsumer,
} from "./infrastructure/kafka/order-created.consumer.js";
import {
    orderCreatedTopic,
} from "./infrastructure/kafka/topics.js";

const logger = pino();

let consumer;
let removeCrashListener;

let stopping = false;
let startupPromise = Promise.resolve();
let shutdownPromise;

function errorFields(error) {
    return {
        errorName: error?.name ?? "Error",
        code: error?.code ?? "INTERNAL",
    };
}

function shutdown(signal) {
    if (shutdownPromise) {
        return shutdownPromise;
    }

    stopping = true;

    logger.info(
        { signal },
        "Shipment consumer stopping"
    );

    shutdownPromise = (async () => {
        const timer = setTimeout(() => {
            logger.error("Shipment consumer shutdown timed out");
            process.exit(1);
        }, 120_000);

        try {
            // Startup must settle before its resources are closed.
            await startupPromise.catch(() => { });

            if (consumer) {
                // Keep MongoDB connected while active work drains.
                await consumer.stop();

                removeCrashListener?.();
                removeCrashListener = undefined;

                await consumer.disconnect();
            }

            await disconnectDatabase();

            logger.info("Shipment consumer stopped");
        } catch (error) {
            logger.error(
                errorFields(error),
                "Shipment consumer shutdown failed"
            );

            // Terminate if orderly cleanup cannot be completed.
            process.exit(1);
        } finally {
            clearTimeout(timer);

            process.off("SIGINT", onSigint);
            process.off("SIGTERM", onSigterm);
        }
    })();

    return shutdownPromise;
}

function onSigint() {
    void shutdown("SIGINT");
}

function onSigterm() {
    void shutdown("SIGTERM");
}

function onConsumerCrash(event) {
    if (stopping) {
        return;
    }

    const restarting = event?.payload?.restart === true;
    const fields = {
        ...errorFields(event?.payload?.error),
        restart: restarting,
    };

    if (restarting) {
        logger.warn(
            fields,
            "Shipment consumer crashed; KafkaJS is restarting"
        );

        return;
    }

    logger.error(
        fields,
        "Shipment consumer crashed without automatic restart"
    );

    process.exitCode = 1;
    void shutdown("CONSUMER_CRASH");
}

async function main() {
    const connection = await connectDatabase();

    if (stopping) {
        return;
    }

    await prepareShipmentDatabase(connection);

    if (stopping) {
        return;
    }

    await checkOrderCreatedTopic();

    if (stopping) {
        return;
    }

    consumer = createOrderCreatedConsumer();
    removeCrashListener = consumer.onCrash(onConsumerCrash);

    await consumer.connect();

    if (stopping) {
        return;
    }

    await consumer.run();

    if (stopping) {
        return;
    }

    logger.info(
        {
            groupId: kafkaConfig.groupId,
            topic: orderCreatedTopic,
        },
        "Shipment consumer started"
    );
}

process.once("SIGINT", onSigint);
process.once("SIGTERM", onSigterm);

startupPromise = main();

void startupPromise.catch((error) => {
    logger.error(
        errorFields(error),
        "Shipment consumer startup failed"
    );

    process.exitCode = 1;
    void shutdown("STARTUP_FAILURE");
});