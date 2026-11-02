import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { DomainError } from "../../domain/errors.js";
import {
    assertOrderActor,
    normalizeOrderInput,
} from "../../domain/order.rules.js";
import { productClientConfig } from "../config/env.js";
import { invokeUnary } from "./unary.js";

const UUID_PATTERN =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

const definition = loadSync(
    fileURLToPath(
        new URL("../../../../../proto/product.proto", import.meta.url)
    ),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    }
);

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.product.v1;

function stockCommand(operationId, actor) {
    assertOrderActor(actor);

    if (
        typeof operationId !== "string" ||
        !UUID_PATTERN.test(operationId)
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid stock operation ID"
        );
    }

    return {
        operationId,
        uid: actor.uid,
    };
}

function mapProductError(error) {
    switch (error.code) {
        case grpc.status.INVALID_ARGUMENT:
            return new DomainError(
                "INVALID_ARGUMENT",
                "Invalid stock request"
            );

        case grpc.status.PERMISSION_DENIED:
            return new DomainError(
                "FORBIDDEN",
                "Stock operation forbidden"
            );

        case grpc.status.NOT_FOUND:
            return new DomainError(
                "NOT_FOUND",
                "Product or reservation not found"
            );

        case grpc.status.ABORTED:
            return new DomainError(
                "CONFLICT",
                "Stock operation conflict"
            );

        case grpc.status.UNAUTHENTICATED:
            return new DomainError(
                "INTERNAL",
                "Product service authentication failed"
            );

        case grpc.status.CANCELLED:
        case grpc.status.UNAVAILABLE:
        case grpc.status.DEADLINE_EXCEEDED:
            return new DomainError(
                "SERVICE_UNAVAILABLE",
                "Product service unavailable"
            );

        default:
            return new DomainError(
                "INTERNAL",
                "Product stock operation failed"
            );
    }
}

function reservationResponse(response, request) {
    if (
        !response ||
        response.operationId !== request.operationId ||
        response.uid !== request.uid ||
        !["RESERVED", "CONFIRMED", "RELEASED"]
            .includes(response.status) ||
        !Array.isArray(response.items) ||
        (
            response.status !== "RELEASED" &&
            response.items.length === 0
        )
    ) {
        throw new DomainError(
            "INTERNAL",
            "Invalid stock service response"
        );
    }

    for (const item of response.items) {
        if (
            !Number.isInteger(item.pid) ||
            item.pid < 1 ||
            item.pid > 2147483647 ||
            !Number.isInteger(item.qty) ||
            item.qty < 1 ||
            item.qty > 2147483647 ||
            typeof item.unitPrice !== "string" ||
            !/^(0|[1-9]\d{0,7})\.\d{2}$/.test(item.unitPrice)
        ) {
            throw new DomainError(
                "INTERNAL",
                "Invalid stock item response"
            );
        }
    }

    return {
        operationId: response.operationId,
        uid: response.uid,
        status: response.status,
        items: response.items.map((item) => ({
            pid: item.pid,
            qty: item.qty,
            unit_price: item.unitPrice,
        })),
    };
}

export function createProductClient({
    address = productClientConfig.grpcAddress,
    serviceToken = productClientConfig.orderToken,
} = {}) {
    const client = new proto.ProductService(
        address,
        grpc.credentials.createInsecure()
    );

    async function invoke(
        method,
        request,
        actor,
        { timeoutMs, deadline, signal } = {},
    ) {
        const metadata = new grpc.Metadata();

        metadata.set("x-service-name", "order-service");
        metadata.set("x-service-token", serviceToken);
        metadata.set("x-user-id", String(actor.uid));
        metadata.set("x-user-role", actor.role);

        let response;

        try {
            response = await invokeUnary({
                client,
                method,
                request,
                metadata,
                timeoutMs,
                deadline,
                signal,
            });
        } catch (error) {
            throw mapProductError(error);
        }

        return reservationResponse(response, request);
    }

    return {
        async reserveStock(
            operationId,
            items,
            actor,
            rpcOptions = {},
        ) {
            const command = stockCommand(operationId, actor);
            const normalized = normalizeOrderInput({ items });

            return invoke(
                "reserveStock",
                { ...command, items: normalized.items },
                actor,
                rpcOptions,
            );
        },

        async confirmStock(
            operationId,
            actor,
            rpcOptions = {},
        ) {
            return invoke(
                "confirmStock",
                stockCommand(operationId, actor),
                actor,
                rpcOptions,
            );
        },

        async releaseStock(
            operationId,
            actor,
            rpcOptions = {},
        ) {
            return invoke(
                "releaseStock",
                stockCommand(operationId, actor),
                actor,
                rpcOptions,
            );
        },

        async getStockReservation(
            operationId,
            actor,
            rpcOptions = {},
        ) {
            return invoke(
                "getStockReservation",
                stockCommand(operationId, actor),
                actor,
                rpcOptions,
            );
        },

        close() {
            client.close();
        },
    };
}

export const productClient = createProductClient();