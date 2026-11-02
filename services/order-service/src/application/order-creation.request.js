import { DomainError } from "../domain/errors.js";

export function assertCreationRpcActive({
    deadline = null,
    signal,
    timeoutMs,
} = {}) {
    if (
        (
            deadline !== null &&
            (
                !(deadline instanceof Date) ||
                !Number.isFinite(deadline.getTime())
            )
        ) ||
        (
            signal !== undefined &&
            !(signal instanceof AbortSignal)
        ) ||
        (
            timeoutMs !== undefined &&
            (
                !Number.isInteger(timeoutMs) ||
                timeoutMs < 1 ||
                timeoutMs > 60_000
            )
        )
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid creation RPC options",
        );
    }

    if (
        signal?.aborted ||
        (
            deadline !== null &&
            deadline.getTime() <= Date.now()
        )
    ) {
        throw new DomainError(
            "SERVICE_UNAVAILABLE",
            "Order request ended; retry the same operation",
        );
    }
}