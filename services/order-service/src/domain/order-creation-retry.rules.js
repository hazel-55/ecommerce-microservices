import { DomainError } from "./errors.js";

const MAX_ATTEMPTS = 2147483647;

const RETRY_CODES = new Set([
    "SERVICE_UNAVAILABLE",
    "INTERNAL",
]);

export function validateCreationRetryCode(code) {
    if (!RETRY_CODES.has(code)) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid creation retry code",
        );
    }
}

export function creationRetryDelaySeconds(attempt) {
    if (
        !Number.isInteger(attempt) ||
        attempt < 1 ||
        attempt > MAX_ATTEMPTS
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid creation retry attempt",
        );
    }

    return Math.min(
        60,
        2 ** Math.min(attempt - 1, 6),
    );
}