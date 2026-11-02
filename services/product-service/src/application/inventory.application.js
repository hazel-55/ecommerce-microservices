import { DomainError } from "../domain/errors.js";
import { assertProductRead } from "../domain/product.rules.js";
import {
    normalizeReservationRequest,
    normalizeStockCommand,
} from "../domain/inventory.rules.js";
import {
    inventoryRepository,
} from "../infrastructure/repositories/inventory.repository.js";

function inputForActor(input, actor, normalize) {
    assertProductRead(actor);

    const normalized = normalize(input);

    if (normalized.uid !== actor.uid) {
        throw new DomainError(
            "FORBIDDEN",
            "Stock request must belong to the authenticated actor"
        );
    }

    return normalized;
}

export function createInventoryApplication({
    repository = inventoryRepository,
} = {}) {
    return {
        async reserveStock(input, actor) {
            const request = inputForActor(
                input,
                actor,
                normalizeReservationRequest
            );

            return repository.reserve(request);
        },

        async confirmStock(input, actor) {
            const command = inputForActor(
                input,
                actor,
                normalizeStockCommand
            );

            return repository.confirm(command);
        },

        async releaseStock(input, actor) {
            const command = inputForActor(
                input,
                actor,
                normalizeStockCommand
            );

            return repository.release(command);
        },

        async getStockReservation(input, actor) {
            const command = inputForActor(
                input,
                actor,
                normalizeStockCommand
            );

            const reservation =
                await repository.findByOperationId(command);

            if (!reservation) {
                throw new DomainError(
                    "NOT_FOUND",
                    "Reservation not found"
                );
            }

            return reservation;
        },
    };
}

export const inventoryApplication = createInventoryApplication();