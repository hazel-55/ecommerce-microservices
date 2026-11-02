import { DomainError } from "./errors.js";

const MAX_ID = 2147483647;

const ROLES = new Set([
    "CUSTOMER",
    "ADMIN",
    "SHIPMENT_STAFF",
]);

const STATUSES = new Set([
    "PENDING",
    "SHIPPING",
    "DELIVERED",
    "CANCELLED",
]);

const NEXT_STATUSES = {
    PENDING: new Set(["SHIPPING", "CANCELLED"]),
    SHIPPING: new Set(["DELIVERED", "CANCELLED"]),
    DELIVERED: new Set(),
    CANCELLED: new Set(),
};

function isValidId(value) {
    return (
        Number.isInteger(value) &&
        value > 0 &&
        value <= MAX_ID
    );
}

export function validateId(value, label = "ID") {
    if (!isValidId(value)) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            `Invalid ${label}`
        );
    }

    return value;
}

export function validateShipmentStatus(status) {
    if (!STATUSES.has(status)) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid shipment status"
        );
    }

    return status;
}

export function normalizeShipmentStatusInput(input) {
    if (
        !input ||
        typeof input !== "object" ||
        Array.isArray(input)
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid shipment status data"
        );
    }

    return {
        status: validateShipmentStatus(input.status),
    };
}

export function assertShipmentActor(actor) {
    if (
        !actor ||
        typeof actor !== "object" ||
        Array.isArray(actor) ||
        !isValidId(actor.uid)
    ) {
        throw new DomainError(
            "UNAUTHENTICATED",
            "Authenticated user required"
        );
    }

    if (!ROLES.has(actor.role)) {
        throw new DomainError(
            "FORBIDDEN",
            "Unsupported role"
        );
    }
}

export function assertShipmentAccess(actor, ownerUid) {
    assertShipmentActor(actor);
    validateId(ownerUid, "shipment owner ID");

    if (
        actor.role === "CUSTOMER" &&
        actor.uid !== ownerUid
    ) {
        throw new DomainError(
            "FORBIDDEN",
            "Cannot access another customer's shipment"
        );
    }
}

export function assertShipmentMutationActor(actor) {
    assertShipmentActor(actor);

    if (
        actor.role !== "ADMIN" &&
        actor.role !== "SHIPMENT_STAFF"
    ) {
        throw new DomainError(
            "FORBIDDEN",
            "Shipment staff permission required"
        );
    }
}

export function assertShipmentStatusTransition(
    currentStatus,
    nextStatus
) {
    if (!STATUSES.has(currentStatus)) {
        throw new DomainError(
            "INTERNAL",
            "Invalid stored shipment status"
        );
    }

    validateShipmentStatus(nextStatus);

    if (currentStatus === nextStatus) {
        return nextStatus;
    }

    if (!NEXT_STATUSES[currentStatus].has(nextStatus)) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid shipment status transition"
        );
    }

    return nextStatus;
}

export function assertShipmentStatusChange(
    actor,
    currentStatus,
    nextStatus
) {
    assertShipmentMutationActor(actor);

    return assertShipmentStatusTransition(
        currentStatus,
        nextStatus
    );
}