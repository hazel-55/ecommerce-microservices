import { timingSafeEqual } from "node:crypto";
import { DomainError } from "../../domain/errors.js";
import {
    customerTransportConfig,
} from "../../infrastructure/config/env.js";

const serviceTokens = new Map([
    ["api-gateway", customerTransportConfig.gatewayToken],
    ["auth-service", customerTransportConfig.authToken],
    ["order-service", customerTransportConfig.orderToken],
]);

function reject() {
    throw new DomainError(
        "UNAUTHENTICATED",
        "Invalid caller credentials"
    );
}

function readSingle(metadata, name) {
    const values = metadata.get(name);

    if (
        values.length !== 1 ||
        typeof values[0] !== "string"
    ) {
        reject();
    }

    return values[0];
}

function authenticateCaller(metadata, expectedService) {
    const service = readSingle(metadata, "x-service-name");
    const token = readSingle(metadata, "x-service-token");
    const expectedToken = serviceTokens.get(expectedService);

    if (service !== expectedService || !expectedToken) {
        reject();
    }

    const actual = Buffer.from(token);
    const expected = Buffer.from(expectedToken);

    if (
        actual.length !== expected.length ||
        !timingSafeEqual(actual, expected)
    ) {
        reject();
    }
}

function readActor(metadata, expectedService) {
    authenticateCaller(metadata, expectedService);

    const uidText = readSingle(metadata, "x-user-id");
    const role = readSingle(metadata, "x-user-role");

    if (!/^[1-9]\d*$/.test(uidText)) {
        reject();
    }

    const uid = Number(uidText);

    if (
        !Number.isInteger(uid) ||
        uid > 2147483647 ||
        !["CUSTOMER", "ADMIN", "SHIPMENT_STAFF"].includes(role)
    ) {
        reject();
    }

    return { uid, role };
}

export function readGatewayActor(metadata) {
    return readActor(metadata, "api-gateway");
}

export function readOrderActor(metadata) {
    return readActor(metadata, "order-service");
}

export function readCustomerReader(metadata) {
    const service = readSingle(metadata, "x-service-name");

    if (service === "api-gateway") {
        return readGatewayActor(metadata);
    }

    if (service === "order-service") {
        return readOrderActor(metadata);
    }

    reject();
}

export function readAuthCaller(metadata) {
    authenticateCaller(metadata, "auth-service");

    return { service: "auth-service" };
}