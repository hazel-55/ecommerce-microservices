import { DomainError } from "./errors.js";
import { validateId } from "./shipment.rules.js";

const UUID_PATTERN =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function isObject(value) {
    return (
        value !== null &&
        typeof value === "object" &&
        !Array.isArray(value)
    );
}

function reject(message) {
    throw new DomainError("INVALID_ARGUMENT", message);
}

export function validateOrderEventId(eventId) {
    if (
        typeof eventId !== "string" ||
        !UUID_PATTERN.test(eventId)
    ) {
        reject("Invalid OrderCreated event ID");
    }

    return eventId;
}

export function normalizeOrderCreatedEvent(input) {
    if (!isObject(input)) {
        reject("Invalid OrderCreated event");
    }

    const eventId = validateOrderEventId(input.eventId);

    if (
        input.eventType !== "OrderCreated" ||
        input.version !== 1
    ) {
        reject("Unsupported OrderCreated event contract");
    }

    const oid = validateId(input.oid, "order ID");
    const uid = validateId(input.uid, "customer ID");

    if (
        typeof input.discount !== "string" ||
        !/^(0|[1-9]\d{0,15})\.\d{2}$/.test(input.discount)
    ) {
        reject("Invalid OrderCreated discount");
    }

    if (
        !Array.isArray(input.items) ||
        input.items.length === 0
    ) {
        reject("OrderCreated event must contain items");
    }

    const productIds = new Set();

    const items = input.items.map((item) => {
        if (!isObject(item)) {
            reject("Invalid OrderCreated item");
        }

        const pid = validateId(item.pid, "product ID");
        const qty = validateId(item.qty, "item quantity");

        if (productIds.has(pid)) {
            reject("Duplicate product ID in OrderCreated event");
        }

        productIds.add(pid);

        if (
            typeof item.unit_price !== "string" ||
            !/^(0|[1-9]\d{0,7})\.\d{2}$/.test(item.unit_price)
        ) {
            reject("Invalid OrderCreated unit price");
        }

        return {
            pid,
            qty,
            unit_price: item.unit_price,
        };
    });

    items.sort((first, second) => first.pid - second.pid);

    return {
        eventId,
        eventType: "OrderCreated",
        version: 1,
        oid,
        uid,
        discount: input.discount,
        items,
    };
}