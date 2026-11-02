import { DomainError } from "./errors.js";

const MAX_ATTEMPTS = 2147483647;
const BASE_DELAY_MS = 1000;
const MAX_DELAY_MS = 60_000;

const RETRY_ERROR_CODES = new Set([
    "SERVICE_UNAVAILABLE",
    "INTERNAL",
]);

export function buildOutboxRetryPlan(attempts, errorCode) {
    if (
        !Number.isInteger(attempts) ||
        attempts < 0 ||
        attempts > MAX_ATTEMPTS
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid Outbox retry attempts"
        );
    }

    if (!RETRY_ERROR_CODES.has(errorCode)) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid Outbox retry error code"
        );
    }

    const delayMs = Math.min(
        BASE_DELAY_MS * (2 ** Math.min(attempts, 6)),
        MAX_DELAY_MS
    );

    return {
        attempts: Math.min(attempts + 1, MAX_ATTEMPTS),
        delayMs,
        errorCode,
    };
}