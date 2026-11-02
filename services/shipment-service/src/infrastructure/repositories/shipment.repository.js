import {
    ShipmentModel,
} from "../database/models/shipment.model.js";
import {
    nextShipmentId,
} from "../database/shipment-id.js";
import { DomainError } from "../../domain/errors.js";
import {
    validateId,
    assertShipmentStatusTransition,
} from "../../domain/shipment.rules.js";
import {
    normalizeOrderCreatedEvent,
    validateOrderEventId,
} from "../../domain/order-created.rules.js";

function toShipment(document) {
    if (!document) {
        return null;
    }

    return {
        shipid: document.shipid,
        oid: document.oid,
        uid: document.uid,
        sourceEventId: document.sourceEventId,
        status: document.status,
    };
}

function assertSameSource(shipment, event) {
    if (
        shipment.sourceEventId !== event.eventId ||
        shipment.oid !== event.oid ||
        shipment.uid !== event.uid
    ) {
        throw new DomainError(
            "CONFLICT",
            "OrderCreated identity does not match stored shipment"
        );
    }
}

export function createShipmentRepository({
    model = ShipmentModel,
    allocateId = nextShipmentId,
} = {}) {
    async function findBySourceEventId(eventId) {
        validateOrderEventId(eventId);

        const document = await model.findOne({
            sourceEventId: eventId,
        }).lean();

        return toShipment(document);
    }

    return {
        findBySourceEventId,

        async findById(shipid) {
            validateId(shipid, "shipment ID");

            const document = await model.findOne({
                shipid,
            }).lean();

            return toShipment(document);
        },
        async updateStatus(
            shipid,
            expectedStatus,
            nextStatus
        ) {
            validateId(shipid, "shipment ID");

            assertShipmentStatusTransition(
                expectedStatus,
                nextStatus
            );

            if (expectedStatus === nextStatus) {
                const document = await model.findOne({
                    shipid,
                    status: expectedStatus,
                }).lean();

                return toShipment(document);
            }

            const document = await model.findOneAndUpdate(
                {
                    shipid,
                    status: expectedStatus,
                },
                {
                    $set: {
                        status: nextStatus,
                    },
                },
                {
                    returnDocument: "after",
                    runValidators: true,
                    upsert: false,
                }
            );

            return toShipment(document);
        },

        async createFromOrderEvent(input) {
            const event = normalizeOrderCreatedEvent(input);

            const existing = await findBySourceEventId(
                event.eventId
            );

            if (existing) {
                assertSameSource(existing, event);

                return {
                    created: false,
                    shipment: existing,
                };
            }

            const shipid = await allocateId();

            try {
                const document = await model.create({
                    shipid,
                    oid: event.oid,
                    uid: event.uid,
                    sourceEventId: event.eventId,
                    status: "PENDING",
                });

                return {
                    created: true,
                    shipment: toShipment(document),
                };
            } catch (error) {
                if (error?.code !== 11000) {
                    throw error;
                }

                const concurrent = await findBySourceEventId(
                    event.eventId
                );

                if (!concurrent) {
                    throw error;
                }

                assertSameSource(concurrent, event);

                return {
                    created: false,
                    shipment: concurrent,
                };
            }
        },
    };
}

export const shipmentRepository = createShipmentRepository();