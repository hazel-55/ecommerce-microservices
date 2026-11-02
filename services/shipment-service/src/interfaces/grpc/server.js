import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";

import { DomainError } from "../../domain/errors.js";
import {
    shipmentApplication,
} from "../../application/shipment.application.js";
import {
    readGatewayActor,
    readOrderActor,
} from "./caller.js";


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

const errorCodes = new Map([
    ["INVALID_ARGUMENT", grpc.status.INVALID_ARGUMENT],
    ["UNAUTHENTICATED", grpc.status.UNAUTHENTICATED],
    ["FORBIDDEN", grpc.status.PERMISSION_DENIED],
    ["NOT_FOUND", grpc.status.NOT_FOUND],
    ["SERVICE_UNAVAILABLE", grpc.status.UNAVAILABLE],
    ["CONFLICT", grpc.status.ABORTED],
]);

export function createGrpcServer(
    logger,
    { application = shipmentApplication } = {}
) {
    if (typeof logger?.error !== "function") {
        throw new TypeError("Shipment logger is required");
    }

    if (
        typeof application?.getShipment !== "function" ||
        typeof application?.updateShipmentStatus !== "function" ||
        typeof application?.getShipmentBySourceEvent !== "function"
    ) {
        throw new TypeError("Shipment application is required");
    }

    const server = new grpc.Server();

    function handler(operation, readActor = readGatewayActor) {
        return async (call, callback) => {
            try {
                const actor = readActor(call.metadata);

                const result = await operation(
                    call.request,
                    actor
                );

                callback(null, result);
            } catch (error) {
                const code = error instanceof DomainError
                    ? errorCodes.get(error.code)
                    : undefined;

                if (code !== undefined) {
                    callback({
                        code,
                        details: error.message,
                    });

                    return;
                }

                logger.error(
                    {
                        errorName: error?.name ?? "Error",
                        code: error?.code ?? "UNKNOWN",
                    },
                    "Shipment operation failed"
                );

                callback({
                    code: grpc.status.INTERNAL,
                    details: "Shipment operation failed",
                });
            }
        };
    }

    server.addService(shipmentPackage.ShipmentService.service, {
        GetShipment: handler((request, actor) =>
            application.getShipment(
                request.shipid,
                actor
            )
        ),

        UpdateShipmentStatus: handler((request, actor) =>
            application.updateShipmentStatus(
                request.shipid,
                { status: request.status },
                actor
            )
        ),

        GetShipmentBySourceEvent: handler(
            (request, actor) =>
                application.getShipmentBySourceEvent(
                    request.sourceEventId,
                    actor
                ),
            readOrderActor
        ),
    });
    return server;
}