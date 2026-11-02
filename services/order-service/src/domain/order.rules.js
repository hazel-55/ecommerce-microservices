import { DomainError } from "./errors.js";

const MAX_ID = 2147483647;
const ROLES = new Set(["CUSTOMER", "ADMIN", "SHIPMENT_STAFF"]);

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
}

export function normalizeOrderInput(input) {
    if (
        !input ||
        typeof input !== "object" ||
        Array.isArray(input)
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid order data"
        );
    }

    if (
        !Array.isArray(input.items) ||
        input.items.length === 0
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Order must contain at least one item"
        );
    }

    const productIds = new Set();

    const items = input.items.map((item) => {
        if (
            !item ||
            typeof item !== "object" ||
            Array.isArray(item)
        ) {
            throw new DomainError(
                "INVALID_ARGUMENT",
                "Invalid order item"
            );
        }

        validateId(item.pid, "product ID");
        validateId(item.qty, "item quantity");

        if (productIds.has(item.pid)) {
            throw new DomainError(
                "INVALID_ARGUMENT",
                "Duplicate product ID in order"
            );
        }

        productIds.add(item.pid);

        return {
            pid: item.pid,
            qty: item.qty,
        };
    });

    return { items };
}

export function assertOrderActor(actor) {
    if (!actor || !isValidId(actor.uid)) {
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

export function assertOrderAccess(actor, ownerUid) {
    assertOrderActor(actor);
    validateId(ownerUid, "order owner ID");

    if (
        actor.role !== "ADMIN" &&
        actor.uid !== ownerUid
    ) {
        throw new DomainError(
            "FORBIDDEN",
            "Cannot access another customer's order"
        );
    }
}