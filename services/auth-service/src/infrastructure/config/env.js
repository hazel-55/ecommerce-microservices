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

export const jwtConfig = {
    secret: required("JWT_SECRET"),
    expiresIn: 3600,
    issuer: "ecommerce-auth",
    audience: "ecommerce-api",
};

if (jwtConfig.secret.length < 32) {
    throw new Error("JWT_SECRET must contain at least 32 characters");
}
export const customerClientConfig = {
    address: required("CUSTOMER_GRPC_ADDRESS"),
    token: required("CUSTOMER_AUTH_TOKEN"),
};

if (customerClientConfig.token.length < 32) {
    throw new Error("CUSTOMER_AUTH_TOKEN must contain at least 32 characters");
}