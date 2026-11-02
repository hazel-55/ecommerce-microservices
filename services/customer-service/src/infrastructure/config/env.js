import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

dotenv.config({
    path: fileURLToPath(new URL("../../../.env", import.meta.url)),
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

if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("PGPORT must be an integer between 1 and 65535");
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
        throw new Error(`${name} must contain at least 32 characters`);
    }

    return value;
}

export const customerTransportConfig = {
    gatewayToken: requiredToken("CUSTOMER_GATEWAY_TOKEN"),
    authToken: requiredToken("CUSTOMER_AUTH_TOKEN"),
    orderToken: requiredToken("CUSTOMER_ORDER_TOKEN"),
};

const distinctTokens = new Set([
    customerTransportConfig.gatewayToken,
    customerTransportConfig.authToken,
    customerTransportConfig.orderToken,
]);

if (distinctTokens.size !== 3) {
    throw new Error(
        "Gateway, Auth and Order service tokens must be different"
    );
}