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

    if (typeof value !== "string" || value.length === 0) {
        throw new Error(`Missing environment variable: ${name}`);
    }

    return value;
}

const port = Number(required("PGPORT"));

if (
    !Number.isInteger(port) ||
    port < 1 ||
    port > 65535
) {
    throw new Error("PGPORT must be between 1 and 65535");
}

export const dbConfig = {
    host: required("PGHOST"),
    port,
    database: required("PGDATABASE"),
    user: required("PGUSER"),
    password: required("PGPASSWORD"),
};
function requiredToken(name) {
    const value = required(name);

    if (value.length < 32) {
        throw new Error(
            `${name} must contain at least 32 characters`
        );
    }

    return value;
}

export const customerClientConfig = {
    grpcAddress: required("CUSTOMER_GRPC_ADDRESS"),
    orderToken: requiredToken("CUSTOMER_ORDER_TOKEN"),
};
export const productClientConfig = {
    grpcAddress: required("PRODUCT_GRPC_ADDRESS"),
    orderToken: requiredToken("PRODUCT_ORDER_TOKEN"),
};
function optionalBoundedInteger(name, fallback, min, max) {
    const raw = process.env[name];
    const value = raw === undefined ? fallback : Number(raw);

    if (
        !Number.isInteger(value) ||
        value < min ||
        value > max
    ) {
        throw new Error(
            `${name} must be an integer between ${min} and ${max}`,
        );
    }

    return value;
}

export const creationWorkerConfig = {
    intervalMs: optionalBoundedInteger(
        "ORDER_CREATION_WORKER_INTERVAL_MS",
        1000,
        1,
        60_000,
    ),
    batchSize: optionalBoundedInteger(
        "ORDER_CREATION_WORKER_BATCH_SIZE",
        5,
        1,
        100,
    ),
};
export const orderTransportConfig = {
    gatewayToken: requiredToken("ORDER_GATEWAY_TOKEN"),
};

if (
    [
        customerClientConfig.orderToken,
        productClientConfig.orderToken,
    ].includes(orderTransportConfig.gatewayToken)
) {
    throw new Error(
        "Order Gateway token must differ from outbound service tokens",
    );
}
export const orderServerConfig = {
    grpcAddress: required("ORDER_GRPC_ADDRESS"),
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
            /\s/.test(broker),
    )
) {
    throw new Error(
        "KAFKA_BROKERS must contain non-empty broker addresses",
    );
}

export const kafkaConfig = {
    clientId: "order-service",
    brokers: kafkaBrokers,
};
export const outboxWorkerConfig = {
    intervalMs: optionalBoundedInteger(
        "ORDER_OUTBOX_WORKER_INTERVAL_MS",
        1000,
        1,
        60_000,
    ),
    batchSize: optionalBoundedInteger(
        "ORDER_OUTBOX_WORKER_BATCH_SIZE",
        5,
        1,
        100,
    ),
};
export const shipmentClientConfig = {
    grpcAddress: required("SHIPMENT_GRPC_ADDRESS"),
    orderToken: requiredToken("SHIPMENT_ORDER_TOKEN"),
};

if (
    [
        customerClientConfig.orderToken,
        productClientConfig.orderToken,
        orderTransportConfig.gatewayToken,
    ].includes(shipmentClientConfig.orderToken)
) {
    throw new Error(
        "Order Shipment token must differ from existing service tokens"
    );
}