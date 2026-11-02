import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { gatewayConfig } from "../../config/env.js";
import { HttpError } from "../../errors/http-error.js";

const protoPath = fileURLToPath(
    new URL(
        "../../../../../proto/order.proto",
        import.meta.url,
    ),
);

const definition = protoLoader.loadSync(protoPath, {
    includeDirs: [dirname(protoPath)],
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.order.v1;

const MAX_ID = 2147483647;
const ROLES = new Set([
    "CUSTOMER",
    "ADMIN",
    "SHIPMENT_STAFF",
]);
const OPERATION_ID_PATTERN =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function validId(value) {
    return Number.isInteger(value) &&
        value > 0 &&
        value <= MAX_ID;
}

function orderInput(input) {
    if (
        !input ||
        typeof input !== "object" ||
        Array.isArray(input) ||
        !Array.isArray(input.items) ||
        input.items.length === 0
    ) {
        throw new HttpError(400, "Invalid order data");
    }

    const seen = new Set();

    const items = input.items.map((item) => {
        if (
            !item ||
            typeof item !== "object" ||
            Array.isArray(item) ||
            !validId(item.pid) ||
            !validId(item.qty) ||
            seen.has(item.pid)
        ) {
            throw new HttpError(400, "Invalid order item");
        }

        seen.add(item.pid);

        return {
            pid: item.pid,
            qty: item.qty,
        };
    });

    return { items };
}

function canonicalItems(items) {
    return items.map((item) => ({
        pid: item.pid,
        qty: item.qty,
    })).sort((a, b) => a.pid - b.pid);
}

function createdOrderResponse(response, request, actor) {
    try {
        if (
            !response ||
            !validId(response.oid) ||
            response.uid !== actor.uid ||
            typeof response.discount !== "string" ||
            !/^(0|[1-9]\d{0,15})\.\d{2}$/.test(response.discount)
        ) {
            throw new Error("Invalid order response");
        }

        const normalized = orderInput(response);

        if (
            JSON.stringify(canonicalItems(normalized.items)) !==
            JSON.stringify(canonicalItems(request.items))
        ) {
            throw new Error("Order items do not match");
        }

        for (const item of response.items) {
            if (
                typeof item.unit_price !== "string" ||
                !/^(0|[1-9]\d{0,7})\.\d{2}$/.test(item.unit_price)
            ) {
                throw new Error("Invalid price snapshot");
            }
        }

        const shipment = response.shipment;

        if (
            !shipment ||
            !validId(shipment.shipid) ||
            shipment.oid !== response.oid ||
            ![
                "PENDING",
                "SHIPPING",
                "DELIVERED",
                "CANCELLED",
            ].includes(shipment.status)
        ) {
            throw new Error("Invalid shipment response");
        }

        return {
            oid: response.oid,
            uid: response.uid,
            discount: response.discount,
            items: response.items.map((item) => ({
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
    } catch {
        throw new HttpError(
            502,
            "Invalid order service response",
        );
    }
}

function validateOid(oid) {
    if (
        !Number.isInteger(oid) ||
        oid < 1 ||
        oid > MAX_ID
    ) {
        throw new HttpError(400, "Invalid order ID");
    }
}

function actorMetadata(actor, serviceToken) {
    if (
        !actor ||
        !Number.isInteger(actor.uid) ||
        actor.uid < 1 ||
        actor.uid > MAX_ID ||
        !ROLES.has(actor.role)
    ) {
        throw new HttpError(
            401,
            "Authenticated user required",
        );
    }

    const metadata = new grpc.Metadata();

    metadata.set("x-service-name", "api-gateway");
    metadata.set("x-service-token", serviceToken);
    metadata.set("x-user-id", String(actor.uid));
    metadata.set("x-user-role", actor.role);

    return metadata;
}

function mapOrderRpcError(error) {
    switch (error.code) {
        case grpc.status.INVALID_ARGUMENT:
            return new HttpError(
                400,
                "Invalid order request",
            );

        case grpc.status.UNAUTHENTICATED:
            return new HttpError(
                502,
                "Order service authentication failed",
            );

        case grpc.status.PERMISSION_DENIED:
            return new HttpError(
                403,
                "Insufficient permission",
            );

        case grpc.status.NOT_FOUND:
            return new HttpError(
                404,
                "Order not found",
            );

        case grpc.status.CANCELLED:
        case grpc.status.UNAVAILABLE:
        case grpc.status.DEADLINE_EXCEEDED:
            return new HttpError(
                503,
                "Order service unavailable",
            );
        case grpc.status.ABORTED:
            return new HttpError(
                409,
                "Order operation conflict",
            );

        default:
            return new HttpError(
                500,
                "Order operation failed",
            );
    }
}

export function createOrderClient({
    address = gatewayConfig.orderGrpcAddress,
    serviceToken = gatewayConfig.orderGatewayToken,
    creationTimeoutMs = 10_000,
} = {}) {
    if (
        !Number.isInteger(creationTimeoutMs) ||
        creationTimeoutMs < 1 ||
        creationTimeoutMs > 60_000
    ) {
        throw new TypeError(
            "Invalid Order creation timeout",
        );
    }

    const client = new proto.OrderService(
        address,
        grpc.credentials.createInsecure(),
    );

    function invoke(method, request, actor) {
        const metadata = actorMetadata(
            actor,
            serviceToken,
        );

        return new Promise((resolve, reject) => {
            client[method](
                request,
                metadata,
                {
                    deadline: new Date(Date.now() + 10_000),
                },
                (error, response) => {
                    if (error) {
                        reject(mapOrderRpcError(error));
                        return;
                    }

                    resolve(response);
                },
            );
        });
    }

    // Hàm này nằm cùng cấp với invoke, bên trong factory.
    function invokeCreate(request, metadata, signal) {
        if (
            signal !== undefined &&
            !(signal instanceof AbortSignal)
        ) {
            throw new TypeError(
                "Invalid Order cancellation signal",
            );
        }

        return new Promise((resolve, reject) => {
            let call;
            let settled = false;
            let cancelRequested = false;

            function finish(error, response) {
                if (settled) {
                    return;
                }

                settled = true;

                signal?.removeEventListener(
                    "abort",
                    onAbort,
                );

                if (error) {
                    reject(mapOrderRpcError(error));
                    return;
                }

                resolve(response);
            }

            function onAbort() {
                cancelRequested = true;

                if (call && !settled) {
                    call.cancel();
                }
            }

            signal?.addEventListener(
                "abort",
                onAbort,
                { once: true },
            );

            if (signal?.aborted) {
                finish({
                    code: grpc.status.CANCELLED,
                });

                return;
            }

            try {
                call = client.createOrder(
                    request,
                    metadata,
                    {
                        deadline: new Date(
                            Date.now() + creationTimeoutMs,
                        ),
                    },
                    finish,
                );

                if (cancelRequested && !settled) {
                    call.cancel();
                }
            } catch (error) {
                finish(error);
            }
        });
    }

    return {
        async createOrder(
            operationId,
            input,
            actor,
            { signal } = {},
        ) {
            const metadata = actorMetadata(
                actor,
                serviceToken,
            );

            if (
                typeof operationId !== "string" ||
                !OPERATION_ID_PATTERN.test(operationId)
            ) {
                throw new HttpError(
                    400,
                    "Invalid order operation ID",
                );
            }

            const request = orderInput(input);

            metadata.set(
                "x-order-operation-id",
                operationId,
            );

            const response = await invokeCreate(
                request,
                metadata,
                signal,
            );

            return createdOrderResponse(
                response,
                request,
                actor,
            );
        },

        async listOrders(actor) {
            return invoke(
                "listOrders",
                {},
                actor,
            );
        },

        async getOrder(oid, actor) {
            validateOid(oid);

            return invoke(
                "getOrder",
                { oid },
                actor,
            );
        },

        close() {
            client.close();
        },
    };
}

export const orderClient = createOrderClient();