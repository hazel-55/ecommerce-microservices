import { DomainError } from "./errors.js";
import {
    assertOrderActor,
    normalizeOrderInput,
} from "./order.rules.js";

const UUID_PATTERN =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export function validateCreationOperationId(operationId) {
    if (
        typeof operationId !== "string" ||
        !UUID_PATTERN.test(operationId)
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid order operation ID"
        );
    }
}

function canonicalItems(items) {
    const normalized = normalizeOrderInput({ items });

    return normalized.items.sort(
        (first, second) => first.pid - second.pid
    );
}

export function normalizeCreationTaskInput(input) {
    if (
        !input ||
        typeof input !== "object" ||
        Array.isArray(input)
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid order creation task"
        );
    }

    validateCreationOperationId(input.operationId);

    assertOrderActor({
        uid: input.uid,
        role: input.actorRole,
    });

    return {
        operationId: input.operationId,
        uid: input.uid,
        actorRole: input.actorRole,
        items: canonicalItems(input.items),
    };
}

export function assertSameCreationTask(task, request) {
    if (task.uid !== request.uid) {
        throw new DomainError(
            "FORBIDDEN",
            "Order operation belongs to another user"
        );
    }

    if (
        task.actor_role !== request.actorRole ||
        JSON.stringify(canonicalItems(task.items)) !==
        JSON.stringify(request.items)
    ) {
        throw new DomainError(
            "CONFLICT",
            "Operation ID already used with different order data"
        );
    }
}