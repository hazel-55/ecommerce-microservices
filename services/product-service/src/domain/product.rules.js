import { DomainError } from "./errors.js";

const MAX_ID = 2147483647;
const ROLES = new Set(["CUSTOMER", "ADMIN", "SHIPMENT_STAFF"]);

function isValidId(value) {
    return Number.isInteger(value) && value >= 1 && value <= MAX_ID;
}

function invalid(message) {
    throw new DomainError("INVALID_ARGUMENT", message);
}

export function validateId(pid) {
    if (!isValidId(pid)) {
        invalid("Invalid product ID");
    }
}

export function normalizePrice(value) {
    if (typeof value !== "string") {
        invalid("Price must be a decimal string");
    }

    const text = value.trim();
    const match = /^(0|[1-9]\d{0,7})(?:\.(\d{1,2}))?$/.exec(text);

    if (!match) {
        invalid("Invalid product price");
    }

    const whole = match[1];
    const fraction = (match[2] ?? "").padEnd(2, "0");

    return `${whole}.${fraction}`;
}

export function normalizeProductInput(input) {
    if (
        input === null ||
        typeof input !== "object" ||
        Array.isArray(input)
    ) {
        invalid("Invalid product data");
    }

    if (typeof input.pname !== "string") {
        invalid("Product name must be a string");
    }

    const pname = input.pname.trim();

    if (pname.length < 1 || pname.length > 100) {
        invalid("Product name must contain 1 to 100 characters");
    }

    const price = normalizePrice(input.price);

    if (
        !Number.isInteger(input.quantity) ||
        input.quantity < 0 ||
        input.quantity > MAX_ID
    ) {
        invalid("Invalid product quantity");
    }

    return {
        pname,
        price,
        quantity: input.quantity,
    };
}

export function assertProductRead(actor) {
    if (!actor || !isValidId(actor.uid)) {
        throw new DomainError(
            "UNAUTHENTICATED",
            "Authenticated user required",
        );
    }

    if (!ROLES.has(actor.role)) {
        throw new DomainError("FORBIDDEN", "Unsupported role");
    }
}

export function assertProductAdmin(actor) {
    assertProductRead(actor);

    if (actor.role !== "ADMIN") {
        throw new DomainError(
            "FORBIDDEN",
            "Admin permission required",
        );
    }
}