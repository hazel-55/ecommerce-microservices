import { DomainError } from "../../domain/errors.js";
import {
    validateCreationOperationId,
} from "../../domain/order-creation.rules.js";

export function readCreationOperationId(metadata) {
    if (typeof metadata?.get !== "function") {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Order operation ID is required",
        );
    }

    const values = metadata.get("x-order-operation-id");

    if (
        !Array.isArray(values) ||
        values.length !== 1 ||
        typeof values[0] !== "string"
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Exactly one order operation ID is required",
        );
    }

    validateCreationOperationId(values[0]);

    return values[0];
}

export function createCreationCallScope(
    call,
    { timeoutMs = 5000 } = {},
) {
    if (
        !Number.isInteger(timeoutMs) ||
        timeoutMs < 1 ||
        timeoutMs > 60_000
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid creation call timeout",
        );
    }

    if (
        !call ||
        typeof call.getDeadline !== "function" ||
        typeof call.on !== "function" ||
        typeof call.removeListener !== "function"
    ) {
        throw new DomainError(
            "INTERNAL",
            "Invalid creation RPC call",
        );
    }

    const operationId = readCreationOperationId(
        call.metadata,
    );

    const rawDeadline = call.getDeadline();

    const receivedDeadline = rawDeadline instanceof Date
        ? rawDeadline.getTime()
        : rawDeadline;

    if (
        typeof receivedDeadline !== "number" ||
        (
            !Number.isFinite(receivedDeadline) &&
            receivedDeadline !== Infinity
        )
    ) {
        throw new DomainError(
            "INTERNAL",
            "Invalid creation RPC deadline",
        );
    }

    const startedAt = Date.now();

    const deadline = new Date(Math.min(
        startedAt + timeoutMs,
        receivedDeadline,
    ));

    const controller = new AbortController();
    let disposed = false;

    function onCancelled() {
        if (!disposed) {
            controller.abort();
        }
    }

    call.on("cancelled", onCancelled);

    if (
        call.cancelled ||
        deadline.getTime() <= startedAt
    ) {
        onCancelled();
    }

    return {
        operationId,

        waitOptions: {
            timeoutMs,
            deadline,
            signal: controller.signal,
        },

        dispose() {
            if (disposed) {
                return;
            }

            disposed = true;

            call.removeListener(
                "cancelled",
                onCancelled,
            );
        },
    };
}