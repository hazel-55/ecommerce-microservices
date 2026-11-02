import pino from "pino";
import { DomainError } from "../domain/errors.js";
import {
    orderCreationRepository,
} from "../infrastructure/repositories/order-creation.repository.js";
import {
    orderCreationTransaction,
} from "../infrastructure/database/order-creation.transaction.js";
import {
    orderCreationDispatcher,
} from "./order-creation.dispatcher.js";

const PENDING_STATES = new Set([
    "PROCESSING",
    "CONFIRM_PENDING",
    "RELEASE_PENDING",
]);

const SKIPPED_OUTCOMES = new Set([
    "NOT_DUE",
    "ALREADY_COMPLETED",
    "ALREADY_FAILED",
]);

function retryCode(error) {
    return error instanceof DomainError &&
        error.code === "SERVICE_UNAVAILABLE"
        ? "SERVICE_UNAVAILABLE"
        : "INTERNAL";
}

export function createOrderCreationWorker({
    repository = orderCreationRepository,
    dispatcher = orderCreationDispatcher,
    transactions = orderCreationTransaction,
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
            "Worker batch size must be between 1 and 100",
        );
    }

    let running = false;

    async function recoverFailure(operationId, code) {
        await transactions.withLockedTask(
            operationId,
            async (task, context) => {
                if (!PENDING_STATES.has(task.status)) {
                    return;
                }

                if (!(await context.isDue())) {
                    return;
                }

                await context.scheduleRetry(code);
            },
        );
    }

    return {
        async runOnce() {
            if (running) {
                return null;
            }

            running = true;

            try {
                const operationIds =
                    await repository.listDueOperationIds({
                        limit: batchSize,
                    });

                const stats = {
                    scanned: operationIds.length,
                    handled: 0,
                    skipped: 0,
                    failed: 0,
                };

                for (const operationId of operationIds) {
                    try {
                        const result =
                            await dispatcher.processDueTask(
                                operationId,
                            );

                        if (
                            SKIPPED_OUTCOMES.has(result.outcome)
                        ) {
                            stats.skipped++;
                        } else {
                            stats.handled++;
                        }
                    } catch (error) {
                        stats.failed++;

                        const code = retryCode(error);

                        logger.error(
                            { operationId, code },
                            "Order creation task processing failed",
                        );

                        try {
                            await recoverFailure(
                                operationId,
                                code,
                            );
                        } catch (recoveryError) {
                            logger.error(
                                {
                                    operationId,
                                    code: retryCode(recoveryError),
                                },
                                "Order creation retry scheduling failed",
                            );
                        }
                    }
                }

                return stats;
            } finally {
                running = false;
            }
        },
    };
}