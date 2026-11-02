import dotenv from "dotenv";
import { fileURLToPath } from "node:url";

dotenv.config({
    path: fileURLToPath(new URL("../../.env", import.meta.url)),
    quiet: true,
});

function required(name) {
    const value = process.env[name];

    if (typeof value !== "string" || value.length === 0) {
        throw new Error(`Missing environment variable: ${name}`);
    }

    return value;
}

function requiredSecret(name) {
    const value = required(name);

    if (value.length < 32) {
        throw new Error(`${name} must contain at least 32 characters`);
    }

    return value;
}

const port = Number(required("GATEWAY_PORT"));

if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("GATEWAY_PORT must be between 1 and 65535");
}

export const gatewayConfig = {
    host: required("GATEWAY_HOST"),
    port,
    redisUrl: required("REDIS_URL"),
    authGrpcAddress: required("AUTH_GRPC_ADDRESS"),
    customerGrpcAddress: required("CUSTOMER_GRPC_ADDRESS"),
    jwtSecret: requiredSecret("JWT_SECRET"),
    jwtIssuer: "ecommerce-auth",
    jwtAudience: "ecommerce-api",
    customerGatewayToken: requiredSecret("CUSTOMER_GATEWAY_TOKEN"),
    productGrpcAddress: required("PRODUCT_GRPC_ADDRESS"),
    productGatewayToken: requiredSecret("PRODUCT_GATEWAY_TOKEN"),
    orderGatewayToken: requiredSecret("ORDER_GATEWAY_TOKEN"),
    orderGrpcAddress: required("ORDER_GRPC_ADDRESS"),
    shipmentGrpcAddress: required("SHIPMENT_GRPC_ADDRESS"),
    shipmentGatewayToken: requiredSecret("SHIPMENT_GATEWAY_TOKEN"),
};

function requiredPositiveInteger(name) {
    const value = Number(required(name));

    if (!Number.isSafeInteger(value) || value < 1) {
        throw new Error(`${name} must be a positive integer`);
    }

    return value;
}

export const rateLimitConfig = {
    windowSeconds: requiredPositiveInteger("RATE_LIMIT_WINDOW_SECONDS"),
    maxRequests: requiredPositiveInteger("RATE_LIMIT_MAX_REQUESTS"),
    authMaxRequests: requiredPositiveInteger("AUTH_RATE_LIMIT_MAX_REQUESTS"),
};