import dotenv from "dotenv";
import { fileURLToPath } from "node:url";

dotenv.config({
    path: fileURLToPath(new URL("../../../.env", import.meta.url)),
    quiet: true,
});

function required(name) {
    const value = process.env[name];

    if (typeof value !== "string" || value.trim().length === 0) {
        throw new Error(`Missing environment variable: ${name}`);
    }

    return value;
}

export const productConfig = {
    mongodbUri: required("MONGODB_URI"),
    grpcAddress: required("PRODUCT_GRPC_ADDRESS"),
};

function requiredSecret(name) {
    const value = required(name);

    if (value.length < 32) {
        throw new Error(`${name} must contain at least 32 characters`);
    }

    return value;
}

export const productTransportConfig = {
    gatewayToken: requiredSecret("PRODUCT_GATEWAY_TOKEN"),
    orderToken: requiredSecret("PRODUCT_ORDER_TOKEN"),
};

if (
    productTransportConfig.gatewayToken ===
    productTransportConfig.orderToken
) {
    throw new Error(
        "Gateway and Order service tokens must be different"
    );
}