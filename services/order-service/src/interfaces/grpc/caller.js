import { timingSafeEqual } from "node:crypto";
import { DomainError } from "../../domain/errors.js";
import {
    orderTransportConfig,
} from "../../infrastructure/config/env.js";

const ROLES = new Set([
    "CUSTOMER",
    "ADMIN",
    "SHIPMENT_STAFF",
]);

function reject() {
    throw new DomainError(
        "UNAUTHENTICATED",
        "Invalid caller credentials",
    );
}

function readSingle(metadata, name) {
    if (
        !metadata ||
        typeof metadata.get !== "function"
    ) {
        reject();
    }

    const values = metadata.get(name);

    if (
        !Array.isArray(values) ||
        values.length !== 1 ||
        typeof values[0] !== "string"
    ) {
        reject();
    }

    return values[0];
}

export function readGatewayActor(metadata) {
    const service = readSingle(metadata, "x-service-name");
    const token = readSingle(metadata, "x-service-token");

    if (service !== "api-gateway") {
        reject();
    }

    const actual = Buffer.from(token);
    const expected = Buffer.from(
        orderTransportConfig.gatewayToken,
    );

    if (
        actual.length !== expected.length ||
        !timingSafeEqual(actual, expected)
    ) {
        reject();
    }

    const uidText = readSingle(metadata, "x-user-id");
    const role = readSingle(metadata, "x-user-role");

    if (!/^[1-9]\d*$/.test(uidText)) {
        reject();
    }

    const uid = Number(uidText);

    if (
        !Number.isInteger(uid) ||
        uid > 2147483647 ||
        !ROLES.has(role)
    ) {
        reject();
    }

    return { uid, role };
}
