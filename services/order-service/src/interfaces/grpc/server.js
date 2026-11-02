import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { DomainError } from "../../domain/errors.js";
import {
    normalizeOrderInput,
} from "../../domain/order.rules.js";
import {
    orderApplication,
} from "../../application/order.application.js";
import {
    orderCreationWaiter,
} from "../../application/order-creation.wait.js";
import {
    assertCreationRpcActive,
} from "../../application/order-creation.request.js";
import { readGatewayActor } from "./caller.js";
import {
    createCreationCallScope,
} from "./creation-call.js";

const protoPath = fileURLToPath(
    new URL(
        "../../../../../proto/order.proto",
        import.meta.url,
    ),
);

const definition = loadSync(protoPath, {
    includeDirs: [dirname(protoPath)],
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.order.v1;

const errorCodes = {
    INVALID_ARGUMENT: grpc.status.INVALID_ARGUMENT,
    UNAUTHENTICATED: grpc.status.UNAUTHENTICATED,
    FORBIDDEN: grpc.status.PERMISSION_DENIED,
    NOT_FOUND: grpc.status.NOT_FOUND,
    CONFLICT: grpc.status.ABORTED,
    SERVICE_UNAVAILABLE: grpc.status.UNAVAILABLE,
};

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

function creationResponse(order, input, actor) {
    try {
        if (
            !order ||
            !validId(order.oid) ||
            order.uid !== actor.uid ||
            typeof order.discount !== "string" ||
            !/^(0|[1-9]\d{0,15})\.\d{2}$/.test(order.discount)
        ) {
            throw new Error("Invalid order identity or discount");
        }

        const actual = normalizeOrderInput({
            items: order.items,
        }).items.sort((a, b) => a.pid - b.pid);

        const expected = [...input.items].sort(
            (a, b) => a.pid - b.pid,
        );

        if (
            actual.length !== expected.length ||
            actual.some((item, index) =>
                item.pid !== expected[index].pid ||
                item.qty !== expected[index].qty
            )
        ) {
            throw new Error("Order items do not match request");
        }

        for (const item of order.items) {
            if (
                typeof item.unit_price !== "string" ||
                !/^(0|[1-9]\d{0,7})\.\d{2}$/.test(item.unit_price)
            ) {
                throw new Error("Invalid price snapshot");
            }
        }

        const shipment = order.shipment;

        if (
            !shipment ||
            !validId(shipment.shipid) ||
            shipment.oid !== order.oid ||
            !SHIPMENT_STATUSES.has(shipment.status)
        ) {
            throw new Error("Invalid shipment");
        }

        return {
            oid: order.oid,
            uid: order.uid,
            discount: order.discount,
            items: order.items.map((item) => ({
                pid: item.pid,
                qty: item.qty,
                unitPrice: item.unit_price,
            })),
            shipment: {
                shipid: shipment.shipid,
                oid: shipment.oid,
                status: shipment.status,
            },
        };
    } catch {
        throw new DomainError(
            "INTERNAL",
            "Invalid order creation response",
        );
    }
}

export function createGrpcServer(
    logger,
    {
        application = orderApplication,
        creation = orderCreationWaiter,
    } = {},
) {
    if (
        typeof logger?.error !== "function" ||
        typeof application?.listOrders !== "function" ||
        typeof application?.getOrder !== "function" ||
        typeof creation?.waitForOrder !== "function"
    ) {
        throw new TypeError("Invalid Order gRPC dependencies");
    }

    const server = new grpc.Server();
    const activeHandlers = new Set();
    const idleWaiters = new Set();

    server.waitForIdle = () => {
        if (activeHandlers.size === 0) {
            return Promise.resolve();
        }

        return new Promise((resolve) => {
            idleWaiters.add(resolve);
        });
    };

    function handler(operation) {
        return async (call, callback) => {
            const ticket = {};
            activeHandlers.add(ticket);

            try {
                const actor = readGatewayActor(
                    call.metadata,
                );

                const result = await operation(
                    call.request,
                    actor,
                    call,
                );

                callback(null, result);
            } catch (error) {
                const code = error instanceof DomainError
                    ? errorCodes[error.code]
                    : undefined;

                if (code !== undefined) {
                    callback({
                        code,
                        details: error.message,
                    });

                    return;
                }

                if (
                    error?.code === "55P03" ||
                    error?.code === "57014"
                ) {
                    callback({
                        code: grpc.status.UNAVAILABLE,
                        details:
                            "Order database temporarily unavailable",
                    });

                    return;
                }

                logger.error(
                    {
                        errorName: error?.name ?? "Error",
                        code: error?.code ?? "UNKNOWN",
                    },
                    "Order operation failed",
                );

                callback({
                    code: grpc.status.INTERNAL,
                    details: "Order operation failed",
                });
            } finally {
                activeHandlers.delete(ticket);

                if (activeHandlers.size === 0) {
                    for (const resolve of idleWaiters) {
                        resolve();
                    }

                    idleWaiters.clear();
                }
            }
        };
    }

    server.addService(proto.OrderService.service, {
        ListOrders: handler((request, actor) =>
            application.listOrders(actor)),

        GetOrder: handler(async (request, actor) => {
            const order = await application.getOrder(
                request.oid,
                actor,
            );

            return {
                oid: order.oid,
                uid: order.uid,
                discount: order.discount,
                items: order.items.map((item) => ({
                    pid: item.pid,
                    qty: item.qty,
                    unitPrice: item.unit_price,
                })),
            };
        }),

        CreateOrder: handler(async (request, actor, call) => {
            const scope = createCreationCallScope(call);

            try {
                const input = normalizeOrderInput(request);

                assertCreationRpcActive(scope.waitOptions);

                const order = await creation.waitForOrder(
                    scope.operationId,
                    input,
                    actor,
                    scope.waitOptions,
                );

                assertCreationRpcActive(scope.waitOptions);

                return creationResponse(order, input, actor);
            } finally {
                scope.dispose();
            }
        }),
    });

    return server;
}