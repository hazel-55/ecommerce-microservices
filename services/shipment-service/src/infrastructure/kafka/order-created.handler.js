import pino from "pino";
import { TextDecoder } from "node:util";
import { DomainError } from "../../domain/errors.js";
import {
    validateId,
    validateShipmentStatus,
} from "../../domain/shipment.rules.js";
import {
    normalizeOrderCreatedEvent,
} from "../../domain/order-created.rules.js";
import {
    shipmentApplication,
} from "../../application/shipment.application.js";
import { orderCreatedTopic } from "./topics.js";

const ERROR_CODES = new Set([
    "INVALID_ARGUMENT",
    "CONFLICT",
    "SERVICE_UNAVAILABLE",
    "INTERNAL",
]);

function validApplicationResult(result, event) {
    const shipment = result?.shipment;

    if (
        typeof result?.created !== "boolean" ||
        !shipment ||
        shipment.oid !== event.oid ||
        shipment.uid !== event.uid ||
        shipment.sourceEventId !== event.eventId
    ) {
        return false;
    }

    try {
        validateId(shipment.shipid, "shipment ID");
        validateShipmentStatus(shipment.status);
    } catch {
        return false;
    }

    return true;
}

export function createOrderCreatedHandler({
    application = shipmentApplication,
    logger = pino(),
    topic = orderCreatedTopic,
} = {}) {
    if (
        typeof application?.handleOrderCreated !== "function" ||
        typeof logger?.warn !== "function" ||
        typeof logger?.error !== "function" ||
        typeof topic !== "string" ||
        !/^[A-Za-z0-9][A-Za-z0-9._-]{0,248}$/.test(topic)
    ) {
        throw new TypeError(
            "Invalid OrderCreated handler dependencies"
        );
    }

    const decoder = new TextDecoder("utf-8", {
        fatal: true,
    });

    return async function handleOrderCreatedRecord(record) {
        const {
            topic: receivedTopic,
            partition,
            message,
        } = record ?? {};

        if (
            receivedTopic !== topic ||
            !Number.isInteger(partition) ||
            partition < 0 ||
            typeof message?.offset !== "string" ||
            !/^(0|[1-9]\d*)$/.test(message.offset)
        ) {
            throw new DomainError(
                "INTERNAL",
                "Unexpected OrderCreated record metadata"
            );
        }

        const metadata = {
            topic: receivedTopic,
            partition,
            offset: message.offset,
        };

        function rejectRecord() {
            logger.warn(
                {
                    ...metadata,
                    code: "INVALID_ARGUMENT",
                },
                "OrderCreated message rejected"
            );

            return {
                outcome: "REJECTED",
                code: "INVALID_ARGUMENT",
            };
        }

        if (
            !Buffer.isBuffer(message.key) ||
            !Buffer.isBuffer(message.value)
        ) {
            return rejectRecord();
        }

        let payload;
        let key;

        try {
            key = decoder.decode(message.key);
            payload = JSON.parse(
                decoder.decode(message.value)
            );
        } catch {
            return rejectRecord();
        }

        let event;

        try {
            event = normalizeOrderCreatedEvent(payload);

            if (key !== String(event.oid)) {
                throw new DomainError(
                    "INVALID_ARGUMENT",
                    "OrderCreated key does not match order ID"
                );
            }
        } catch (error) {
            if (
                error instanceof DomainError &&
                error.code === "INVALID_ARGUMENT"
            ) {
                return rejectRecord();
            }

            throw error;
        }

        try {
            const result = await application.handleOrderCreated(
                event
            );

            if (!validApplicationResult(result, event)) {
                throw new DomainError(
                    "INTERNAL",
                    "Invalid Shipment application result"
                );
            }

            return {
                outcome: result.created
                    ? "CREATED"
                    : "DUPLICATE",
                eventId: event.eventId,
                shipid: result.shipment.shipid,
            };
        } catch (error) {
            const code =
                error instanceof DomainError &&
                    ERROR_CODES.has(error.code)
                    ? error.code
                    : "INTERNAL";

            logger.error(
                {
                    ...metadata,
                    eventId: event.eventId,
                    code,
                },
                "Shipment persistence failed"
            );

            throw error;
        }
    };
}