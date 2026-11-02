import {
    outboxTransaction,
} from "../infrastructure/database/outbox.transaction.js";

function publicationErrorCode(error) {
    const name = typeof error?.name === "string"
        ? error.name
        : "";

    return name.startsWith("KafkaJS")
        ? "SERVICE_UNAVAILABLE"
        : "INTERNAL";
}

export function createOutboxPublisher({
    transactions = outboxTransaction,
    producer,
} = {}) {
    if (typeof producer?.publish !== "function") {
        throw new TypeError(
            "Outbox publisher requires an event producer"
        );
    }

    return {
        async publishEvent(eventId) {
            return await transactions.withLockedEvent(
                eventId,
                async (event, context) => {
                    try {
                        await producer.publish(event);
                    } catch (error) {
                        const errorCode = publicationErrorCode(
                            error
                        );

                        await context.scheduleRetry(errorCode);

                        return {
                            eventId,
                            outcome: "RETRY_SCHEDULED",
                            errorCode,
                        };
                    }

                    // Kafka acknowledged the event before this step.
                    // SQL failures must propagate to transaction rollback.
                    await context.markPublished();

                    return {
                        eventId,
                        outcome: "PUBLISHED",
                    };
                }
            );
        },
    };
}