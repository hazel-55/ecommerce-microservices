import { DomainError } from "../domain/errors.js";
import {
    assertOrderActor,
    normalizeOrderInput,
} from "../domain/order.rules.js";
import {
    orderCreationTransaction,
} from "../infrastructure/database/order-creation.transaction.js";
import {
    customerClient,
} from "../infrastructure/grpc/customer.client.js";
import {
    calculateOrderPricing,
} from "../domain/order.pricing.js";
import {
    productClient,
} from "../infrastructure/grpc/product.client.js";
const PERMANENT_FAILURES = new Set([
    "INVALID_ARGUMENT",
    "NOT_FOUND",
    "FORBIDDEN",
    "CONFLICT",
]);

function outcome(task, name) {
    return {
        operationId: task.operation_id,
        status: task.status,
        outcome: name,
    };
}

function validateCustomerSnapshot(customer, task) {
    if (
        !customer ||
        customer.uid !== task.uid ||
        !Number.isInteger(customer.score) ||
        customer.score < 0 ||
        customer.score > 2147483647
    ) {
        throw new DomainError(
            "INTERNAL",
            "Invalid customer score response",
        );
    }
}
function validateStockSnapshot(reservation, task) {
    try {
        if (
            !reservation ||
            reservation.operationId !== task.operation_id ||
            reservation.uid !== task.uid ||
            !["RESERVED", "CONFIRMED", "RELEASED"]
                .includes(reservation.status) ||
            !Array.isArray(reservation.items)
        ) {
            throw new Error("Invalid reservation identity");
        }

        if (reservation.status === "RELEASED") {
            return;
        }

        const actual = normalizeOrderInput({
            items: reservation.items,
        }).items.sort((a, b) => a.pid - b.pid);

        const expected = normalizeOrderInput({
            items: task.items,
        }).items.sort((a, b) => a.pid - b.pid);

        if (
            actual.length !== expected.length ||
            actual.some((item, index) =>
                item.pid !== expected[index].pid ||
                item.qty !== expected[index].qty
            )
        ) {
            throw new Error("Reservation items do not match");
        }

        for (const item of reservation.items) {
            if (
                typeof item.unit_price !== "string" ||
                !/^(0|[1-9]\d{0,7})\.\d{2}$/
                    .test(item.unit_price)
            ) {
                throw new Error("Invalid reservation price");
            }
        }
    } catch {
        throw new DomainError(
            "INTERNAL",
            "Invalid stock reservation response",
        );
    }
}
export function createOrderCreationProcessor({
    transactions = orderCreationTransaction,
    customers = customerClient,
    products = productClient,
    rpcOptions = {},
} = {}) {
    return {
        async freezeCustomerScore(operationId) {
            return transactions.withLockedTask(
                operationId,
                async (task, context) => {
                    if (task.status !== "PROCESSING") {
                        return outcome(
                            task,
                            "NOT_PROCESSING",
                        );
                    }

                    if (task.score !== null) {
                        return outcome(
                            task,
                            "ALREADY_FROZEN",
                        );
                    }

                    const actor = {
                        uid: task.uid,
                        role: task.actor_role,
                    };

                    assertOrderActor(actor);

                    let customer;

                    // Chỉ phân loại lỗi từ Customer và dữ liệu trả về.
                    // Lỗi SQL bên dưới được truyền ra transaction.
                    try {
                        customer = await customers.getCustomer(
                            task.uid,
                            actor,
                            rpcOptions,
                        );

                        validateCustomerSnapshot(
                            customer,
                            task,
                        );
                    } catch (error) {
                        if (
                            error instanceof DomainError &&
                            PERMANENT_FAILURES.has(error.code)
                        ) {
                            const updated =
                                await context.requestRelease(
                                    error.code,
                                );

                            return outcome(
                                updated,
                                "RELEASE_PENDING",
                            );
                        }

                        const retryCode =
                            error instanceof DomainError &&
                                error.code === "SERVICE_UNAVAILABLE"
                                ? "SERVICE_UNAVAILABLE"
                                : "INTERNAL";

                        const updated =
                            await context.scheduleRetry(
                                retryCode,
                            );

                        return outcome(
                            updated,
                            "RETRY_SCHEDULED",
                        );
                    }

                    await context.freezeScore(
                        customer.score,
                    );

                    return outcome(
                        task,
                        "SCORE_FROZEN",
                    );
                },
            );
        },
        async reserveAndStoreOrder(
            operationId,
            { nextAttemptAt = null } = {},
        ) {
            if (
                nextAttemptAt !== null &&
                (
                    !(nextAttemptAt instanceof Date) ||
                    !Number.isFinite(nextAttemptAt.getTime())
                )
            ) {
                throw new DomainError(
                    "INVALID_ARGUMENT",
                    "Invalid next attempt time",
                );
            }

            return transactions.withLockedTask(
                operationId,
                async (task, context) => {
                    if (task.status !== "PROCESSING") {
                        return outcome(task, "NOT_PROCESSING");
                    }

                    if (task.score === null) {
                        return outcome(task, "SCORE_REQUIRED");
                    }

                    const actor = {
                        uid: task.uid,
                        role: task.actor_role,
                    };

                    assertOrderActor(actor);

                    let reservation;

                    try {
                        reservation = await products.reserveStock(
                            task.operation_id,
                            task.items,
                            actor,
                            rpcOptions,
                        );

                        validateStockSnapshot(
                            reservation,
                            task,
                        );
                    } catch (error) {
                        if (
                            error instanceof DomainError &&
                            PERMANENT_FAILURES.has(error.code)
                        ) {
                            const updated =
                                await context.requestRelease(
                                    error.code,
                                );

                            return outcome(
                                updated,
                                "RELEASE_PENDING",
                            );
                        }

                        const retryCode =
                            error instanceof DomainError &&
                                error.code === "SERVICE_UNAVAILABLE"
                                ? "SERVICE_UNAVAILABLE"
                                : "INTERNAL";

                        const updated =
                            await context.scheduleRetry(
                                retryCode,
                            );

                        return outcome(
                            updated,
                            "RETRY_SCHEDULED",
                        );
                    }

                    if (reservation.status === "RELEASED") {
                        const updated =
                            await context.requestRelease(
                                "CONFLICT",
                            );

                        return outcome(
                            updated,
                            "RELEASE_PENDING",
                        );
                    }

                    if (reservation.status === "CONFIRMED") {
                        const updated =
                            await context.scheduleRetry(
                                "INTERNAL",
                            );

                        return outcome(
                            updated,
                            "RETRY_SCHEDULED",
                        );
                    }

                    // Kiểm tra giới hạn tiền trước khi ghi SQL.
                    try {
                        calculateOrderPricing(
                            reservation.items,
                            task.score,
                        );
                    } catch (error) {
                        if (
                            error instanceof DomainError &&
                            error.code === "INVALID_ARGUMENT"
                        ) {
                            const updated =
                                await context.requestRelease(
                                    "INVALID_ARGUMENT",
                                );

                            return outcome(
                                updated,
                                "RELEASE_PENDING",
                            );
                        }

                        throw error;
                    }

                    // Lỗi ghi SQL hoặc COMMIT được truyền ra ngoài.
                    const order =
                        await context.commitReservedOrder(
                            reservation,
                            { nextAttemptAt },
                        );

                    return {
                        ...outcome(
                            {
                                ...task,
                                status: "CONFIRM_PENDING",
                            },
                            "ORDER_STORED",
                        ),
                        order,
                    };
                },
            );
        },
        async confirmStoredOrder(operationId) {
            return transactions.withLockedTask(
                operationId,
                async (task, context) => {
                    if (task.status === "COMPLETED") {
                        return outcome(
                            task,
                            "ALREADY_COMPLETED",
                        );
                    }

                    if (task.status !== "CONFIRM_PENDING") {
                        return outcome(
                            task,
                            "NOT_CONFIRM_PENDING",
                        );
                    }

                    const actor = {
                        uid: task.uid,
                        role: task.actor_role,
                    };

                    assertOrderActor(actor);

                    let reservation;

                    try {
                        reservation = await products.confirmStock(
                            task.operation_id,
                            actor,
                            rpcOptions,
                        );

                        validateStockSnapshot(
                            reservation,
                            task,
                        );

                        if (reservation.status !== "CONFIRMED") {
                            throw new DomainError(
                                "INTERNAL",
                                "Stock confirmation was not acknowledged",
                            );
                        }
                    } catch (error) {
                        const retryCode =
                            error instanceof DomainError &&
                                error.code === "SERVICE_UNAVAILABLE"
                                ? "SERVICE_UNAVAILABLE"
                                : "INTERNAL";

                        const updated =
                            await context.scheduleRetry(
                                retryCode,
                            );

                        return outcome(
                            updated,
                            "RETRY_SCHEDULED",
                        );
                    }

                    // Kiểm tra snapshot giá và hoàn tất task
                    // trong giao dịch SQL hiện tại.
                    const order = await context.completeOrder(
                        reservation,
                    );

                    return {
                        ...outcome(
                            {
                                ...task,
                                status: "COMPLETED",
                            },
                            "ORDER_COMPLETED",
                        ),
                        order,
                    };
                },
            );
        },
        async releaseFailedCreation(operationId) {
            return transactions.withLockedTask(
                operationId,
                async (task, context) => {
                    if (task.status === "FAILED") {
                        return outcome(
                            task,
                            "ALREADY_FAILED",
                        );
                    }

                    if (task.status !== "RELEASE_PENDING") {
                        return outcome(
                            task,
                            "NOT_RELEASE_PENDING",
                        );
                    }

                    if (!PERMANENT_FAILURES.has(task.failure_code)) {
                        throw new DomainError(
                            "INTERNAL",
                            "Invalid stored release decision",
                        );
                    }

                    const actor = {
                        uid: task.uid,
                        role: task.actor_role,
                    };

                    assertOrderActor(actor);

                    await context.assertNoStoredOrder();

                    let reservation;

                    try {
                        reservation = await products.releaseStock(
                            task.operation_id,
                            actor,
                            rpcOptions,
                        );

                        validateStockSnapshot(
                            reservation,
                            task,
                        );

                        if (reservation.status !== "RELEASED") {
                            throw new DomainError(
                                "INTERNAL",
                                "Stock release was not acknowledged",
                            );
                        }
                    } catch (error) {
                        const retryCode =
                            error instanceof DomainError &&
                                error.code === "SERVICE_UNAVAILABLE"
                                ? "SERVICE_UNAVAILABLE"
                                : "INTERNAL";

                        const updated =
                            await context.scheduleRetry(
                                retryCode,
                            );

                        return outcome(
                            updated,
                            "RETRY_SCHEDULED",
                        );
                    }

                    await context.finishRelease(
                        reservation,
                    );

                    return {
                        ...outcome(
                            {
                                ...task,
                                status: "FAILED",
                            },
                            "ORDER_FAILED",
                        ),
                        failureCode: task.failure_code,
                    };
                },
            );
        },

    };
}

export const orderCreationProcessor =
    createOrderCreationProcessor();