import dotenv from "dotenv";
import { fileURLToPath } from "node:url";

dotenv.config({
    path: fileURLToPath(
        new URL("../../../.env", import.meta.url)
    ),
    quiet: true,
});

function required(name) {
    const value = process.env[name];

    if (
        typeof value !== "string" ||
        value.trim().length === 0
    ) {
        throw new Error(
            `Missing environment variable: ${name}`
        );
    }

    return value;
}

export const shipmentConfig = {
    mongodbUri: required("MONGODB_URI"),
};
const kafkaBrokers = (
    process.env.KAFKA_BROKERS ?? "127.0.0.1:19092"
)
    .split(",")
    .map((broker) => broker.trim());

if (
    kafkaBrokers.some(
        (broker) =>
            broker.length === 0 ||
            /\s/.test(broker)
    )
) {
    throw new Error(
        "KAFKA_BROKERS must contain non-empty broker addresses"
    );
}

export const kafkaConfig = {
    clientId: "shipment-service",
    groupId: "shipment-service-v1",
    brokers: kafkaBrokers,
};
function requiredSecret(name) {
    const value = required(name);

    if (value.length < 32) {
        throw new Error(
            `${name} must contain at least 32 characters`
        );
    }

    return value;
}

export const shipmentTransportConfig = {
    gatewayToken: requiredSecret("SHIPMENT_GATEWAY_TOKEN"),
    orderToken: requiredSecret("SHIPMENT_ORDER_TOKEN"),
};

if (
    shipmentTransportConfig.gatewayToken ===
    shipmentTransportConfig.orderToken
) {
    throw new Error(
        "Shipment Gateway and Order tokens must be different"
    );
}
export const shipmentServerConfig = {
    grpcAddress: required("SHIPMENT_GRPC_ADDRESS"),
};