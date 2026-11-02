import { DomainError } from "../domain/errors.js";
import {
    assertOrderActor,
} from "../domain/order.rules.js";
import {
    normalizeCreationTaskInput,
} from "../domain/order-creation.rules.js";
import {
    orderCreationRepository,
} from "../infrastructure/repositories/order-creation.repository.js";

export function createOrderCreationApplication({
    tasks = orderCreationRepository,
} = {}) {
    if (typeof tasks?.createTask !== "function") {
        throw new TypeError(
            "Creation task repository must provide createTask"
        );
    }

    return {
        async submitOrder(operationId, input, actor) {
            assertOrderActor(actor);

            if (
                !input ||
                typeof input !== "object" ||
                Array.isArray(input)
            ) {
                throw new DomainError(
                    "INVALID_ARGUMENT",
                    "Invalid order data"
                );
            }

            const request = normalizeCreationTaskInput({
                operationId,
                uid: actor.uid,
                actorRole: actor.role,
                items: input.items,
            });

            return tasks.createTask(request);
        },
    };
}

export const orderCreationApplication =
    createOrderCreationApplication();