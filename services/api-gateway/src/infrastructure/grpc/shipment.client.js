import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";

import { gatewayConfig } from "../../config/env.js";
import { HttpError } from "../../errors/http-error.js";

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

const MAX_ID = 2147483647;
const ROLES = new Set(["CUSTOMER", "ADMIN", "SHIPMENT_STAFF"]);

const STATUSES = new Set([
    "PENDING",
    "SHIPPING",
    "DELIVERED",
    "CANCELLED",
]);

function validId(value) {
    return Number.isInteger(value) &&
        value > 0 &&
        value <= MAX_ID;
}

function validateShipid(shipid) {
    if (!validId(shipid)) {
        throw new HttpError(400, "Invalid shipment ID");
    }
}

function actorMetadata(actor, serviceToken) {
    if (
        !actor ||
        !validId(actor.uid) ||
        !ROLES.has(actor.role)
    ) {
        throw new HttpError(401, "Authenticated user required");
    }

    const metadata = new grpc.Metadata();

    metadata.set("x-service-name", "api-gateway");
    metadata.set("x-service-token", serviceToken);
    metadata.set("x-user-id", String(actor.uid));
    metadata.set("x-user-role", actor.role);

    return metadata;
}

function shipmentResponse(value, expectedShipid) {
    if (
        !value ||
        !validId(value.shipid) ||
        value.shipid !== expectedShipid ||
        !validId(value.oid) ||
        !STATUSES.has(value.status)
    ) {
        throw new HttpError(
            502,
            "Invalid shipment service response"
        );
    }

    return {
        shipid: value.shipid,
        oid: value.oid,
        status: value.status,
    };
}

function mapRpcError(error) {
    switch (error.code) {
        case grpc.status.INVALID_ARGUMENT:
            return new HttpError(400, "Invalid shipment request");

        case grpc.status.UNAUTHENTICATED:
            return new HttpError(
                502,
                "Shipment service authentication failed"
            );

        case grpc.status.PERMISSION_DENIED:
            return new HttpError(403, "Insufficient permission");

        case grpc.status.NOT_FOUND:
            return new HttpError(404, "Shipment not found");

        case grpc.status.UNAVAILABLE:
        case grpc.status.DEADLINE_EXCEEDED:
            return new HttpError(
                503,
                "Shipment service unavailable"
            );

        case grpc.status.ABORTED:
            return new HttpError(
                409,
                "Shipment status changed; retry request"
            );

        default:
            return new HttpError(500, "Shipment operation failed");
    }
}

export function createShipmentClient({
    address = gatewayConfig.shipmentGrpcAddress,
    serviceToken = gatewayConfig.shipmentGatewayToken,
} = {}) {
    const client = new shipmentPackage.ShipmentService(
        address,
        grpc.credentials.createInsecure()
    );

    function invoke(method, request, actor) {
        const metadata = actorMetadata(actor, serviceToken);

        return new Promise((resolve, reject) => {
            client[method](
                request,
                metadata,
                { deadline: new Date(Date.now() + 10_000) },
                (error, response) => {
                    if (error) {
                        reject(mapRpcError(error));
                        return;
                    }

                    try {
                        resolve(
                            shipmentResponse(
                                response,
                                request.shipid
                            )
                        );
                    } catch (responseError) {
                        reject(responseError);
                    }
                }
            );
        });
    }

    return {
        async getShipment(shipid, actor) {
            validateShipid(shipid);

            return invoke("getShipment", { shipid }, actor);
        },

        async updateShipmentStatus(shipid, input, actor) {
            validateShipid(shipid);

            if (
                !input ||
                typeof input !== "object" ||
                Array.isArray(input) ||
                !STATUSES.has(input.status)
            ) {
                throw new HttpError(400, "Invalid shipment status");
            }

            return invoke(
                "updateShipmentStatus",
                {
                    shipid,
                    status: input.status,
                },
                actor
            );
        },

        close() {
            client.close();
        },
    };
}

export const shipmentClient = createShipmentClient();