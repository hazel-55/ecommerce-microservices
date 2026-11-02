import { pool } from "./pool.js";
import { DomainError } from "../../domain/errors.js";
import {
    buildOutboxRetryPlan,
} from "../../domain/outbox.retry.js";

const UUID_PATTERN =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export function createOutboxTransaction({
    database = pool,
} = {}) {
    return {
        async withLockedEvent(eventId, operation) {
            if (
                typeof eventId !== "string" ||
                !UUID_PATTERN.test(eventId)
            ) {
                throw new DomainError(
                    "INVALID_ARGUMENT",
                    "Invalid Outbox event ID"
                );
            }

            if (typeof operation !== "function") {
                throw new TypeError(
                    "Outbox operation must be a function"
                );
            }

            const client = await database.connect();

            let transactionStarted = false;
            let scopeActive = false;
            let releaseError;

            try {
                await client.query("BEGIN");
                transactionStarted = true;

                const selected = await client.query(
                    `
                    SELECT e.*
                    FROM outbox_events e
                    JOIN orders o
                        ON o.oid = e.oid
                    JOIN order_creation_tasks t
                        ON t.operation_id = o.operation_id
                       AND t.uid = o.uid
                    WHERE e.event_id = $1::uuid
                      AND e.published_at IS NULL
                      AND e.event_type = 'OrderCreated'
                      AND e.next_attempt_at <= CURRENT_TIMESTAMP
                      AND t.status = 'COMPLETED'
                      AND e.event_id = t.operation_id
                    FOR UPDATE OF e SKIP LOCKED
                    `,
                    [eventId]
                );

                if (selected.rowCount === 0) {
                    await client.query("COMMIT");
                    transactionStarted = false;

                    return { handled: false };
                }

                let lockedEvent = selected.rows[0];
                scopeActive = true;

                let retryScheduled = false;

                function assertScopeActive() {
                    if (!scopeActive) {
                        throw new DomainError(
                            "INTERNAL",
                            "Outbox transaction is closed"
                        );
                    }
                }

                const context = {
                    async markPublished() {
                        assertScopeActive();

                        if (retryScheduled) {
                            throw new DomainError(
                                "CONFLICT",
                                "Cannot publish after scheduling retry"
                            );
                        }

                        if (lockedEvent.published_at !== null) {
                            return structuredClone(lockedEvent);
                        }

                        const updated = await client.query(
                            `
                            UPDATE outbox_events
                            SET published_at = clock_timestamp(),
                                last_error = NULL
                            WHERE event_id = $1::uuid
                              AND published_at IS NULL
                            RETURNING *
                            `,
                            [eventId]
                        );

                        if (updated.rowCount !== 1) {
                            throw new DomainError(
                                "CONFLICT",
                                "Outbox publication state changed"
                            );
                        }

                        lockedEvent = updated.rows[0];

                        return structuredClone(lockedEvent);
                    },

                    async scheduleRetry(errorCode) {
                        assertScopeActive();

                        if (lockedEvent.published_at !== null) {
                            throw new DomainError(
                                "CONFLICT",
                                "Cannot retry a published event"
                            );
                        }

                        const plan = buildOutboxRetryPlan(
                            lockedEvent.attempts,
                            errorCode
                        );

                        if (retryScheduled) {
                            return structuredClone(lockedEvent);
                        }

                        const updated = await client.query(
                            `
                            UPDATE outbox_events
                            SET attempts = $2::integer,
                                last_error = $3::text,
                                next_attempt_at =
                                    clock_timestamp() +
                                    (
                                        $4::integer *
                                        INTERVAL '1 millisecond'
                                    )
                            WHERE event_id = $1::uuid
                              AND published_at IS NULL
                            RETURNING *
                            `,
                            [
                                eventId,
                                plan.attempts,
                                plan.errorCode,
                                plan.delayMs,
                            ]
                        );

                        if (updated.rowCount !== 1) {
                            throw new DomainError(
                                "CONFLICT",
                                "Outbox retry state changed"
                            );
                        }

                        lockedEvent = updated.rows[0];
                        retryScheduled = true;

                        return structuredClone(lockedEvent);
                    },
                };

                const result = await operation(
                    structuredClone(lockedEvent),
                    context
                );

                scopeActive = false;

                await client.query("COMMIT");
                transactionStarted = false;

                return {
                    handled: true,
                    result,
                };
            } catch (error) {
                scopeActive = false;

                if (transactionStarted) {
                    try {
                        await client.query("ROLLBACK");
                    } catch (rollbackError) {
                        releaseError = rollbackError;
                    }
                }

                throw error;
            } finally {
                scopeActive = false;
                client.release(releaseError);
            }
        },
    };
}

export const outboxTransaction = createOutboxTransaction();