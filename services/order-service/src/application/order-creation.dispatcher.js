import { DomainError } from "../domain/errors.js";
import {
    orderCreationTransaction,
} from "../infrastructure/database/order-creation.transaction.js";
import {
    customerClient,
} from "../infrastructure/grpc/customer.client.js";
import {
    productClient,
} from "../infrastructure/grpc/product.client.js";
import {
    createOrderCreationProcessor,
} from "./order-creation.processor.js";

const PENDING_STATES = new Set([
    "PROCESSING",
    "CONFIRM_PENDING",
    "RELEASE_PENDING",
]);

function outcome(task, name) {
    return {
        operationId: task.operation_id,
        status: task.status,
        outcome: name,
    };
}

export function createOrderCreationDispatcher({
    transactions = orderCreationTransaction,
    customers = customerClient,
    products = productClient,
} = {}) {
    return {
        async processDueTask(operationId, rpcOptions = {}) {
            return transactions.withLockedTask(
                operationId,
                async (task, context) => {
                    if (task.status === "COMPLETED") {
                        return outcome(
                            task,
                            "ALREADY_COMPLETED",
                        );
                    }

                    if (task.status === "FAILED") {
                        return outcome(
                            task,
                            "ALREADY_FAILED",
                        );
                    }

                    if (!PENDING_STATES.has(task.status)) {
                        throw new DomainError(
                            "INTERNAL",
                            "Unsupported creation task state",
                        );
                    }

                    if (!(await context.isDue())) {
                        return outcome(
                            task,
                            "NOT_DUE",
                        );
                    }

                    // Processor dùng giao dịch và khóa hiện tại.
                    // Không mở thêm transaction bên trong.
                    const processor = createOrderCreationProcessor({
                        customers,
                        products,
                        rpcOptions,
                        transactions: {
                            async withLockedTask(
                                requestedId,
                                operation,
                            ) {
                                if (
                                    requestedId !== task.operation_id
                                ) {
                                    throw new DomainError(
                                        "INTERNAL",
                                        "Creation task scope mismatch",
                                    );
                                }

                                return operation(
                                    structuredClone(task),
                                    context,
                                );
                            },
                        },
                    });

                    if (task.status === "PROCESSING") {
                        if (task.score === null) {
                            return processor.freezeCustomerScore(
                                operationId,
                            );
                        }

                        return processor.reserveAndStoreOrder(
                            operationId,
                        );
                    }

                    if (task.status === "CONFIRM_PENDING") {
                        return processor.confirmStoredOrder(
                            operationId,
                        );
                    }

                    return processor.releaseFailedCreation(
                        operationId,
                    );
                },
            );
        },
    };
}

export const orderCreationDispatcher =
    createOrderCreationDispatcher();