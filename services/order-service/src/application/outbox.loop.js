import pino from "pino";
import {
    createOrderCreationLoop,
} from "./order-creation.loop.js";

export function createOutboxLoop({
    worker,
    intervalMs = 1000,
    logger = pino(),
} = {}) {
    if (typeof worker?.runOnce !== "function") {
        throw new TypeError(
            "Outbox worker must provide runOnce"
        );
    }

    if (typeof logger?.error !== "function") {
        throw new TypeError(
            "Outbox loop requires an error logger"
        );
    }

    return createOrderCreationLoop({
        worker,
        intervalMs,
        logger: {
            error(fields) {
                logger.error(
                    fields,
                    "Outbox worker batch failed"
                );
            },
        },
    });
}