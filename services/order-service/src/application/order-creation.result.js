import { DomainError } from "../domain/errors.js";
import { assertOrderActor } from "../domain/order.rules.js";
import {
    validateCreationOperationId,
} from "../domain/order-creation.rules.js";
import {
    orderCreationRepository,
} from "../infrastructure/repositories/order-creation.repository.js";
import {
    orderRepository,
} from "../infrastructure/repositories/order.repository.js";
import {
    shipmentClient,
} from "../infrastructure/grpc/shipment.client.js";
import {
    assertCreationRpcActive,
} from "./order-creation.request.js";

const PENDING_STATES = new Set([
    "PROCESSING",
    "CONFIRM_PENDING",
    "RELEASE_PENDING",
]);

const FAILURE_MESSAGES = new Map([
    ["INVALID_ARGUMENT", "Invalid order request"],
    ["NOT_FOUND", "Customer or product not found"],
    ["CONFLICT", "Order could not be created"],
    ["FORBIDDEN", "Order creation forbidden"],
]);

const SHIPMENT_STATUSES = new Set([
    "PENDING",
    "SHIPPING",
    "DELIVERED",
    "CANCELLED",
]);

function validId(value) {
    return Number.isInteger(value) &&
        value > 0 &&
        value <= 2147483647;
}

function inconsistentResult() {
    throw new DomainError(
        "INTERNAL",
        "Stored order creation result is inconsistent",
    );
}

export function createOrderCreationResultReader({
    tasks = orderCreationRepository,
    orders = orderRepository,
    shipments = shipmentClient,
} = {}) {
    if (
        typeof tasks?.findByOperationId !== "function" ||
        typeof orders?.findByOperationId !== "function" ||
        typeof shipments?.getShipmentBySourceEvent !== "function"
    ) {
        throw new TypeError(
            "Invalid order creation result dependencies",
        );
    }

    return {
        async readResult(
            operationId,
            actor,
            rpcOptions = {},
        ) {
            assertOrderActor(actor);
            validateCreationOperationId(operationId);
            assertCreationRpcActive(rpcOptions);

            const task = await tasks.findByOperationId(
                operationId,
            );

            assertCreationRpcActive(rpcOptions);

            if (!task) {
                throw new DomainError(
                    "NOT_FOUND",
                    "Order creation task not found",
                );
            }

            if (task.operation_id !== operationId) {
                inconsistentResult();
            }

            if (task.uid !== actor.uid) {
                throw new DomainError(
                    "FORBIDDEN",
                    "Order operation belongs to another user",
                );
            }

            if (PENDING_STATES.has(task.status)) {
                return null;
            }

            if (task.status === "FAILED") {
                const message = FAILURE_MESSAGES.get(
                    task.failure_code,
                );

                if (!message) {
                    inconsistentResult();
                }

                throw new DomainError(
                    task.failure_code,
                    message,
                );
            }

            if (task.status !== "COMPLETED") {
                inconsistentResult();
            }

            const order = await orders.findByOperationId(
                operationId,
            );

            assertCreationRpcActive(rpcOptions);

            if (
                !order ||
                !validId(order.oid) ||
                order.uid !== task.uid ||
                !Array.isArray(order.items) ||
                order.items.length === 0
            ) {
                inconsistentResult();
            }

            let shipment;

            try {
                shipment =
                    await shipments.getShipmentBySourceEvent(
                        operationId,
                        order.oid,
                        actor,
                        rpcOptions,
                    );
            } catch (error) {
                assertCreationRpcActive(rpcOptions);

                if (
                    error instanceof DomainError &&
                    error.code === "NOT_FOUND"
                ) {
                    return null;
                }

                throw error;
            }

            assertCreationRpcActive(rpcOptions);

            if (
                !shipment ||
                !validId(shipment.shipid) ||
                shipment.oid !== order.oid ||
                !SHIPMENT_STATUSES.has(shipment.status)
            ) {
                inconsistentResult();
            }

            return {
                oid: order.oid,
                uid: order.uid,
                discount: order.discount,
                items: order.items.map((item) => ({
                    pid: item.pid,
                    qty: item.qty,
                    unit_price: item.unit_price,
                })),
                shipment: {
                    shipid: shipment.shipid,
                    oid: shipment.oid,
                    status: shipment.status,
                },
            };
        },
    };
}

export const orderCreationResultReader =
    createOrderCreationResultReader();