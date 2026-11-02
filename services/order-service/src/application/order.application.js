import { DomainError } from "../domain/errors.js";
import {
    assertOrderActor,
    assertOrderAccess,
    validateId,
} from "../domain/order.rules.js";
import {
    orderRepository,
} from "../infrastructure/repositories/order.repository.js";

export function createOrderApplication({
    repository = orderRepository,
} = {}) {
    return {
        async listOrders(actor) {
            assertOrderActor(actor);

            const items = await repository.listByUid(
                actor.uid,
            );

            return { items };
        },

        async getOrder(oid, actor) {
            assertOrderActor(actor);
            validateId(oid, "order ID");

            const order = await repository.findById(oid);

            if (!order) {
                throw new DomainError(
                    "NOT_FOUND",
                    "Order not found",
                );
            }

            assertOrderAccess(actor, order.uid);

            return order;
        },
    };
}

export const orderApplication =
    createOrderApplication();