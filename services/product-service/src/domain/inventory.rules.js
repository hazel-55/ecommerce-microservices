import { DomainError } from "./errors.js";
import { validateId } from "./product.rules.js";

const UUID_PATTERN =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function reject(message) {
    throw new DomainError("INVALID_ARGUMENT", message);
}

export function validateOperationId(operationId) {
    if (
        typeof operationId !== "string" ||
        !UUID_PATTERN.test(operationId)
    ) {
        reject("Invalid stock operation ID");
    }
}

export function normalizeStockCommand(input) {
    if (
        !input ||
        typeof input !== "object" ||
        Array.isArray(input)
    ) {
        reject("Invalid stock command");
    }

    validateOperationId(input.operationId);
    validateId(input.uid);

    return {
        operationId: input.operationId,
        uid: input.uid,
    };
}

export function normalizeReservationRequest(input) {
    const command = normalizeStockCommand(input);

    if (
        !Array.isArray(input.items) ||
        input.items.length === 0
    ) {
        reject("Reservation must contain at least one item");
    }

    const productIds = new Set();

    const items = input.items.map((item) => {
        if (
            !item ||
            typeof item !== "object" ||
            Array.isArray(item)
        ) {
            reject("Invalid reservation item");
        }

        validateId(item.pid);
        validateId(item.qty);

        if (productIds.has(item.pid)) {
            reject("Duplicate product ID in reservation");
        }

        productIds.add(item.pid);

        return {
            pid: item.pid,
            qty: item.qty,
        };
    });

    items.sort((first, second) => first.pid - second.pid);

    return {
        ...command,
        items,
    };
}