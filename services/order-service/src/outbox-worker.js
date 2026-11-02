import pino from "pino";
import { pool } from "./infrastructure/database/pool.js";
import {
    outboxWorkerConfig,
} from "./infrastructure/config/env.js";
import {
    checkOutboxReadiness,
} from "./infrastructure/outbox-readiness.js";
import {
    createOrderEventProducer,
} from "./infrastructure/kafka/order-event.producer.js";
import {
    createOutboxPublisher,
} from "./application/outbox.publisher.js";
import {
    createOutboxWorker,
} from "./application/outbox.worker.js";
import {
    createOutboxLoop,
} from "./application/outbox.loop.js";

const logger = pino();

const producer = createOrderEventProducer();

const publisher = createOutboxPublisher({
    producer,
});

const worker = createOutboxWorker({
    publisher,
    logger,
    batchSize: outboxWorkerConfig.batchSize,
});

const loop = createOutboxLoop({
    worker,
    logger,
    intervalMs: outboxWorkerConfig.intervalMs,
});

let stopping = false;
let shutdownPromise;
let startupPromise = Promise.resolve();

function errorCode(error) {
    return typeof error?.code === "string"
        ? error.code
        : "INTERNAL";
}

function shutdown(signal) {
    if (shutdownPromise) {
        return shutdownPromise;
    }

    stopping = true;

    shutdownPromise = (async () => {
        logger.info({ signal }, "Order Outbox worker stopping");

        const timer = setTimeout(() => {
            logger.error("Order Outbox shutdown timed out");
            process.exit(1);
        }, 120_000);

        timer.unref();

        try {
            // Let startup settle before closing its resources.
            await startupPromise.catch(() => { });

            try {
                await loop.stop();
            } finally {
                try {
                    await producer.disconnect();
                } finally {
                    await pool.end();
                }
            }

            logger.info("Order Outbox worker stopped");
        } catch (error) {
            process.exitCode = 1;

            logger.error(
                { code: errorCode(error) },
                "Order Outbox shutdown failed"
            );
        } finally {
            clearTimeout(timer);
        }
    })();

    return shutdownPromise;
}

async function main() {
    const readiness = await checkOutboxReadiness();

    if (stopping) {
        return;
    }

    await producer.connect();

    if (stopping) {
        return;
    }

    loop.start();

    logger.info(
        {
            batchSize: outboxWorkerConfig.batchSize,
            intervalMs: outboxWorkerConfig.intervalMs,
            candidateCount: readiness.candidateCount,
        },
        "Order Outbox worker started"
    );
}

process.once("SIGINT", () => void shutdown("SIGINT"));
process.once("SIGTERM", () => void shutdown("SIGTERM"));

startupPromise = main();

void startupPromise.catch((error) => {
    if (!stopping) {
        process.exitCode = 1;

        logger.error(
            { code: errorCode(error) },
            "Order Outbox startup failed"
        );
    }

    void shutdown("STARTUP_FAILURE");
});