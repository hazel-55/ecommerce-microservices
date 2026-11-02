import {
    scheduleCreationRetry,
} from "./order-creation.retry.js";
import {
    persistReservedOrder,
    findStoredOrder,
} from "./order-creation.persistence.js";
import { pool } from "./pool.js";
import { DomainError } from "../../domain/errors.js";
import {
    validateCreationOperationId,
} from "../../domain/order-creation.rules.js";
import {
    discountRateForScore,
} from "../../domain/order.pricing.js";
import {
    completeConfirmedOrder,
    requestStockRelease,
    finishStockRelease,
} from "./order-creation.lifecycle.js";

export function createOrderCreationTransaction({
    database = pool,
    lockTimeoutMs = 2000,
    statementTimeoutMs = 5000,
} = {}) {
    if (
        !Number.isInteger(lockTimeoutMs) ||
        lockTimeoutMs < 1 ||
        lockTimeoutMs > 60_000 ||
        !Number.isInteger(statementTimeoutMs) ||
        statementTimeoutMs < 1 ||
        statementTimeoutMs > 60_000 ||
        lockTimeoutMs >= statementTimeoutMs
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid creation transaction timeout options",
        );
    }

    return {
        async withLockedTask(operationId, operation) {
            validateCreationOperationId(operationId);

            if (typeof operation !== "function") {
                throw new TypeError(
                    "Task operation must be a function"
                );
            }

            const client = await database.connect();

            let transactionStarted = false;
            let scopeActive = false;
            let releaseError;

            try {
                await client.query("BEGIN");
                transactionStarted = true;
                await client.query(
                    `
    SELECT
        set_config('lock_timeout', $1, true),
        set_config('statement_timeout', $2, true)
    `,
                    [
                        `${lockTimeoutMs}ms`,
                        `${statementTimeoutMs}ms`,
                    ],
                );

                const selected = await client.query(
                    `
                    SELECT *
                    FROM order_creation_tasks
                    WHERE operation_id = $1
                    FOR UPDATE
                    `,
                    [operationId]
                );

                if (selected.rowCount === 0) {
                    throw new DomainError(
                        "NOT_FOUND",
                        "Order creation task not found"
                    );
                }

                let lockedTask = selected.rows[0];

                const scoreWasCommitted = lockedTask.score !== null;
                const taskStatusAtStart = lockedTask.status;

                scopeActive = true;

                const context = {
                    async assertNoStoredOrder() {
                        if (!scopeActive) {
                            throw new DomainError(
                                "INTERNAL",
                                "Task transaction is closed",
                            );
                        }

                        const order = await findStoredOrder(
                            client,
                            operationId,
                        );

                        if (order) {
                            throw new DomainError(
                                "CONFLICT",
                                "Stored order must not release stock",
                            );
                        }
                    },
                    async isDue() {
                        if (!scopeActive) {
                            throw new DomainError(
                                "INTERNAL",
                                "Task transaction is closed",
                            );
                        }

                        const result = await client.query(
                            `
                            SELECT
                                next_attempt_at <= clock_timestamp()
                                    AS due
                            FROM order_creation_tasks
                            WHERE operation_id = $1::uuid
                            `,
                            [operationId],
                        );

                        if (result.rowCount !== 1) {
                            throw new DomainError(
                                "NOT_FOUND",
                                "Order creation task not found",
                            );
                        }

                        return result.rows[0].due;
                    },

                    async freezeScore(score) {
                        if (!scopeActive) {
                            throw new DomainError(
                                "INTERNAL",
                                "Task transaction is closed"
                            );
                        }

                        // Reuse score validation from the domain policy.
                        discountRateForScore(score);

                        if (lockedTask.score !== null) {
                            return lockedTask.score;
                        }

                        if (lockedTask.status !== "PROCESSING") {
                            throw new DomainError(
                                "CONFLICT",
                                "Cannot initialize score in this task state"
                            );
                        }

                        const updated = await client.query(
                            `
                            UPDATE order_creation_tasks
                            SET score = $2,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE operation_id = $1
                              AND status = 'PROCESSING'
                              AND score IS NULL
                            RETURNING *
                            `,
                            [operationId, score]
                        );

                        if (updated.rowCount !== 1) {
                            throw new DomainError(
                                "CONFLICT",
                                "Task score could not be initialized"
                            );
                        }

                        lockedTask = updated.rows[0];

                        return lockedTask.score;
                    },
                    async commitReservedOrder(reservation, options = {}) {
                        if (!scopeActive) {
                            throw new DomainError(
                                "INTERNAL",
                                "Task transaction is closed"
                            );
                        }

                        if (!scoreWasCommitted) {
                            throw new DomainError(
                                "CONFLICT",
                                "Score must be committed before creating an order"
                            );
                        }

                        const persisted = await persistReservedOrder(
                            client,
                            lockedTask,
                            reservation,
                            options
                        );

                        lockedTask = persisted.task;

                        return persisted.order;
                    },
                    async completeOrder(reservation) {
                        if (!scopeActive) {
                            throw new DomainError(
                                "INTERNAL",
                                "Task transaction is closed"
                            );
                        }

                        if (
                            !["CONFIRM_PENDING", "COMPLETED"]
                                .includes(taskStatusAtStart)
                        ) {
                            throw new DomainError(
                                "CONFLICT",
                                "Order must be committed before task completion"
                            );
                        }

                        const completed = await completeConfirmedOrder(
                            client,
                            lockedTask,
                            reservation
                        );

                        lockedTask = completed.task;

                        return completed.order;
                    },

                    async requestRelease(failureCode, options = {}) {
                        if (!scopeActive) {
                            throw new DomainError(
                                "INTERNAL",
                                "Task transaction is closed"
                            );
                        }

                        lockedTask = await requestStockRelease(
                            client,
                            lockedTask,
                            failureCode,
                            options
                        );

                        return structuredClone(lockedTask);
                    },

                    async finishRelease(reservation) {
                        if (!scopeActive) {
                            throw new DomainError(
                                "INTERNAL",
                                "Task transaction is closed"
                            );
                        }

                        if (
                            !["RELEASE_PENDING", "FAILED"]
                                .includes(taskStatusAtStart)
                        ) {
                            throw new DomainError(
                                "CONFLICT",
                                "Release decision must be committed before finalization"
                            );
                        }

                        lockedTask = await finishStockRelease(
                            client,
                            lockedTask,
                            reservation
                        );

                        return structuredClone(lockedTask);
                    },
                    async scheduleRetry(errorCode, options = {}) {
                        if (!scopeActive) {
                            throw new DomainError(
                                "INTERNAL",
                                "Task transaction is closed",
                            );
                        }

                        lockedTask = await scheduleCreationRetry(
                            client,
                            lockedTask,
                            errorCode,
                            options,
                        );

                        return structuredClone(lockedTask);
                    },
                };

                const result = await operation(
                    structuredClone(lockedTask),
                    context
                );

                scopeActive = false;

                await client.query("COMMIT");
                transactionStarted = false;

                return result;
            } catch (error) {
                scopeActive = false;

                if (transactionStarted) {
                    try {
                        await client.query("ROLLBACK");
                    } catch (rollbackError) {
                        releaseError = rollbackError;
                    }
                }

                if (
                    !(error instanceof DomainError) &&
                    (
                        error?.code === "55P03" ||
                        error?.code === "57014"
                    )
                ) {
                    throw new DomainError(
                        "SERVICE_UNAVAILABLE",
                        "Order database operation timed out; retry the same operation",
                    );
                }

                throw error;
            } finally {
                scopeActive = false;
                client.release(releaseError);
            }
        },
    };
}

export const orderCreationTransaction =
    createOrderCreationTransaction();