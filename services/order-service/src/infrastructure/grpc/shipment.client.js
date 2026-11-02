import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";

import { DomainError } from "../../domain/errors.js";
import {
    assertOrderActor,
    validateId,
} from "../../domain/order.rules.js";
import {
    validateCreationOperationId,
} from "../../domain/order-creation.rules.js";
import {
    shipmentClientConfig,
} from "../config/env.js";
import { invokeUnary } from "./unary.js";

const definition = loadSync(
    fileURLToPath(
        new URL(
            "../../../../../proto/shipment.proto",
            import.meta.url
        )
    ),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    }
);

const shipmentPackage = grpc.loadPackageDefinition(definition)
    .ecommerce.shipment.v1;

const STATUSES = new Set([
    "PENDING",
    "SHIPPING",
    "DELIVERED",
    "CANCELLED",
]);

function mapShipmentError(error) {
    switch (error.code) {
        case grpc.status.INVALID_ARGUMENT:
            return new DomainError(
                "INVALID_ARGUMENT",
                "Invalid shipment lookup request"
            );

        case grpc.status.PERMISSION_DENIED:
            return new DomainError(
                "FORBIDDEN",
                "Shipment access forbidden"
            );

        case grpc.status.NOT_FOUND:
            return new DomainError(
                "NOT_FOUND",
                "Shipment not found"
            );

        case grpc.status.UNAUTHENTICATED:
            return new DomainError(
                "INTERNAL",
                "Shipment service authentication failed"
            );

        case grpc.status.CANCELLED:
        case grpc.status.UNAVAILABLE:
        case grpc.status.DEADLINE_EXCEEDED:
            return new DomainError(
                "SERVICE_UNAVAILABLE",
                "Shipment service unavailable"
            );

        case grpc.status.ABORTED:
            return new DomainError(
                "CONFLICT",
                "Shipment lookup conflict"
            );

        default:
            return new DomainError(
                "INTERNAL",
                "Shipment lookup failed"
            );
    }
}

function shipmentResponse(response, expectedOid) {
    if (
        !response ||
        !Number.isInteger(response.shipid) ||
        response.shipid < 1 ||
        response.shipid > 2147483647 ||
        response.oid !== expectedOid ||
        !STATUSES.has(response.status)
    ) {
        throw new DomainError(
            "INTERNAL",
            "Invalid shipment service response"
        );
    }

    return {
        shipid: response.shipid,
        oid: response.oid,
        status: response.status,
    };
}

export function createShipmentClient({
    address = shipmentClientConfig.grpcAddress,
    serviceToken = shipmentClientConfig.orderToken,
} = {}) {
    const client = new shipmentPackage.ShipmentService(
        address,
        grpc.credentials.createInsecure()
    );

    return {
        async getShipmentBySourceEvent(
            sourceEventId,
            expectedOid,
            actor,
            { timeoutMs, deadline, signal } = {},
        ) {
            assertOrderActor(actor);
            validateCreationOperationId(sourceEventId);
            validateId(expectedOid, "order ID");

            const metadata = new grpc.Metadata();

            metadata.set("x-service-name", "order-service");
            metadata.set("x-service-token", serviceToken);
            metadata.set("x-user-id", String(actor.uid));
            metadata.set("x-user-role", actor.role);

            let response;

            try {
                response = await invokeUnary({
                    client,
                    method: "getShipmentBySourceEvent",
                    request: { sourceEventId },
                    metadata,
                    timeoutMs,
                    deadline,
                    signal,
                });
            } catch (error) {
                throw mapShipmentError(error);
            }

            return shipmentResponse(response, expectedOid);
        },

        close() {
            client.close();
        },
    };
}

export const shipmentClient = createShipmentClient();