import { DomainError } from "./errors.js";

const MAX_ID = 2147483647;
const ROLES = new Set(["CUSTOMER", "ADMIN", "SHIPMENT_STAFF"]);

function isValidId(value) {
    return Number.isInteger(value) && value > 0 && value <= MAX_ID;
}

export function validateId(value, field = "uid") {
    if (!isValidId(value)) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            `${field} must be a positive PostgreSQL integer`
        );
    }

    return value;
}

export function normalizeFullname(value) {
    if (typeof value !== "string") {
        throw new DomainError("INVALID_ARGUMENT", "fullname must be a string");
    }

    const fullname = value.trim();

    if (fullname.length === 0 || fullname.length > 255) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "fullname must contain between 1 and 255 characters"
        );
    }

    return fullname;
}

export function assertCustomerAccess(actor, uid) {
    validateId(uid);

    if (!actor || !isValidId(actor.uid)) {
        throw new DomainError("UNAUTHENTICATED", "Authenticated user required");
    }

    if (!ROLES.has(actor.role)) {
        throw new DomainError("FORBIDDEN", "Unsupported role");
    }

    if (actor.role !== "ADMIN" && actor.uid !== uid) {
        throw new DomainError("FORBIDDEN", "Cannot access another customer's profile");
    }
}

export function assertMembershipChange(actor, currentMid, nextMid) {
    if (nextMid === undefined) {
        return;
    }

    validateId(nextMid, "mid");

    if (nextMid !== currentMid && actor?.role !== "ADMIN") {
        throw new DomainError("FORBIDDEN", "Only Admin can change membership");
    }
}