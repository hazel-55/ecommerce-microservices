import { DomainError } from "../../domain/errors.js";
import {
    createOrderCreatedHandler,
} from "./order-created.handler.js";

const OUTCOMES = new Set([
    "CREATED",
    "DUPLICATE",
    "REJECTED",
]);

function invalidBatch() {
    return new DomainError(
        "INTERNAL",
        "Invalid OrderCreated batch"
    );
}

export function createOrderCreatedBatchHandler({
    handler = createOrderCreatedHandler(),
    commitOffsets,
} = {}) {
    if (typeof handler !== "function") {
        throw new TypeError("Record handler must be a function");
    }

    if (typeof commitOffsets !== "function") {
        throw new TypeError("Offset commit must be a function");
    }

    return async function handleOrderCreatedBatch({
        batch,
        resolveOffset,
        heartbeat,
        isRunning,
        isStale,
    } = {}) {
        if (
            !batch ||
            typeof batch.topic !== "string" ||
            batch.topic.length === 0 ||
            !Number.isInteger(batch.partition) ||
            batch.partition < 0 ||
            !Array.isArray(batch.messages) ||
            typeof resolveOffset !== "function" ||
            typeof heartbeat !== "function" ||
            typeof isRunning !== "function" ||
            typeof isStale !== "function"
        ) {
            throw invalidBatch();
        }

        for (const message of batch.messages) {
            if (!isRunning() || isStale()) {
                return;
            }

            if (
                typeof message?.offset !== "string" ||
                !/^(0|[1-9]\d*)$/.test(message.offset)
            ) {
                throw invalidBatch();
            }

            await heartbeat();

            if (!isRunning() || isStale()) {
                return;
            }

            const result = await handler({
                topic: batch.topic,
                partition: batch.partition,
                message,
            });

            if (!OUTCOMES.has(result?.outcome)) {
                throw new DomainError(
                    "INTERNAL",
                    "Invalid OrderCreated handler outcome"
                );
            }

            if (isStale()) {
                return;
            }

            await heartbeat();

            if (isStale()) {
                return;
            }

            await commitOffsets([
                {
                    topic: batch.topic,
                    partition: batch.partition,
                    offset: (
                        BigInt(message.offset) + 1n
                    ).toString(),
                },
            ]);

            if (isStale()) {
                return;
            }

            resolveOffset(message.offset);
        }
    };
}