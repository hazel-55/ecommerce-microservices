import pino from "pino";
import { DomainError } from "../domain/errors.js";
import {
    outboxRepository,
} from "../infrastructure/repositories/outbox.repository.js";

export function createOutboxWorker({
    repository = outboxRepository,
    publisher,
    logger = pino(),
    batchSize = 20,
} = {}) {
    if (
        !Number.isInteger(batchSize) ||
        batchSize < 1 ||
        batchSize > 100
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Outbox batch size must be between 1 and 100"
        );
    }

    if (
        typeof repository?.listDueEventIds !== "function" ||
        typeof publisher?.publishEvent !== "function" ||
        typeof logger?.error !== "function"
    ) {
        throw new TypeError(
            "Invalid Outbox worker dependencies"
        );
    }

    let running = false;

    return {
        async runOnce() {
            if (running) {
                return null;
            }

            running = true;

            try {
                const eventIds = await repository.listDueEventIds({
                    limit: batchSize,
                });

                if (
                    !Array.isArray(eventIds) ||
                    eventIds.length > batchSize
                ) {
                    throw new Error(
                        "Invalid Outbox candidate batch"
                    );
                }

                const stats = {
                    scanned: eventIds.length,
                    published: 0,
                    retried: 0,
                    skipped: 0,
                    failed: 0,
                };

                for (const eventId of eventIds) {
                    try {
                        const result = await publisher.publishEvent(
                            eventId
                        );

                        if (result?.handled === false) {
                            stats.skipped += 1;
                            continue;
                        }

                        if (
                            result?.handled !== true ||
                            result.result?.eventId !== eventId
                        ) {
                            throw new Error(
                                "Invalid Outbox publisher result"
                            );
                        }

                        switch (result.result.outcome) {
                            case "PUBLISHED":
                                stats.published += 1;
                                break;

                            case "RETRY_SCHEDULED":
                                stats.retried += 1;
                                break;

                            default:
                                throw new Error(
                                    "Unknown Outbox publisher outcome"
                                );
                        }
                    } catch (error) {
                        stats.failed += 1;

                        logger.error(
                            {
                                eventId,
                                errorName:
                                    typeof error?.name === "string"
                                        ? error.name
                                        : "Error",
                                code:
                                    typeof error?.code === "string"
                                        ? error.code
                                        : "INTERNAL",
                            },
                            "Outbox event processing failed"
                        );
                    }
                }

                return stats;
            } finally {
                running = false;
            }
        },
    };
}