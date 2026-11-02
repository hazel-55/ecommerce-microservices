import {
    orderCreationApplication,
} from "./order-creation.application.js";
import {
    orderCreationResultReader,
} from "./order-creation.result.js";
import {
    orderCreationDispatcher,
} from "./order-creation.dispatcher.js";
import { assertOrderActor } from "../domain/order.rules.js";
import {
    assertCreationRpcActive,
} from "./order-creation.request.js";

export function createOrderCreationProgress({
    submission = orderCreationApplication,
    results = orderCreationResultReader,
    dispatcher = orderCreationDispatcher,
} = {}) {
    if (
        typeof submission?.submitOrder !== "function" ||
        typeof results?.readResult !== "function" ||
        typeof dispatcher?.processDueTask !== "function"
    ) {
        throw new TypeError(
            "Invalid order creation progress dependencies",
        );
    }

    return {
        async advanceOrder(
            operationId,
            input,
            actor,
            rpcOptions = {},
        ) {
            assertOrderActor(actor);
            assertCreationRpcActive(rpcOptions);

            await submission.submitOrder(
                operationId,
                input,
                actor,
            );

            assertCreationRpcActive(rpcOptions);

            const existing = await results.readResult(
                operationId,
                actor,
                rpcOptions,
            );

            assertCreationRpcActive(rpcOptions);

            if (existing !== null) {
                return existing;
            }

            await dispatcher.processDueTask(
                operationId,
                rpcOptions,
            );

            assertCreationRpcActive(rpcOptions);

            const result = await results.readResult(
                operationId,
                actor,
                rpcOptions,
            );

            assertCreationRpcActive(rpcOptions);

            return result;
        },
    };
}

export const orderCreationProgress =
    createOrderCreationProgress();