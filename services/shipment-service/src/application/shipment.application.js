import { DomainError } from "../domain/errors.js";
import {
    validateId,
    assertShipmentActor,
    assertShipmentAccess,
    assertShipmentMutationActor,
    assertShipmentStatusChange,
    normalizeShipmentStatusInput,
} from "../domain/shipment.rules.js";
import {
    normalizeOrderCreatedEvent,
    validateOrderEventId,
} from "../domain/order-created.rules.js";
import {
    shipmentRepository,
} from "../infrastructure/repositories/shipment.repository.js";

function shipmentResponse(shipment) {
    return {
        shipid: shipment.shipid,
        oid: shipment.oid,
        status: shipment.status,
    };
}

export function createShipmentApplication({
    repository = shipmentRepository,
} = {}) {
    async function findExisting(shipid) {
        const shipment = await repository.findById(shipid);

        if (!shipment) {
            throw new DomainError(
                "NOT_FOUND",
                "Shipment not found"
            );
        }

        return shipment;
    }

    return {
        async handleOrderCreated(input) {
            const event = normalizeOrderCreatedEvent(input);

            return repository.createFromOrderEvent(event);
        },
        async getShipmentBySourceEvent(sourceEventId, actor) {
            assertShipmentActor(actor);
            validateOrderEventId(sourceEventId);

            const shipment =
                await repository.findBySourceEventId(sourceEventId);

            if (!shipment) {
                throw new DomainError(
                    "NOT_FOUND",
                    "Shipment not found"
                );
            }

            assertShipmentAccess(actor, shipment.uid);

            return shipmentResponse(shipment);
        },

        async getShipment(shipid, actor) {
            assertShipmentActor(actor);
            validateId(shipid, "shipment ID");

            const shipment = await findExisting(shipid);

            assertShipmentAccess(actor, shipment.uid);

            return shipmentResponse(shipment);
        },

        async updateShipmentStatus(shipid, input, actor) {
            assertShipmentMutationActor(actor);
            validateId(shipid, "shipment ID");

            const { status } = normalizeShipmentStatusInput(input);

            for (let attempt = 0; attempt < 3; attempt += 1) {
                const current = await findExisting(shipid);

                assertShipmentStatusChange(
                    actor,
                    current.status,
                    status
                );

                const updated = await repository.updateStatus(
                    shipid,
                    current.status,
                    status
                );

                if (updated) {
                    return shipmentResponse(updated);
                }
            }

            const latest = await findExisting(shipid);

            assertShipmentStatusChange(
                actor,
                latest.status,
                status
            );

            if (latest.status === status) {
                return shipmentResponse(latest);
            }

            throw new DomainError(
                "SERVICE_UNAVAILABLE",
                "Shipment status changed; retry request"
            );
        },
    };
}

export const shipmentApplication = createShipmentApplication();