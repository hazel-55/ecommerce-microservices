import { DomainError } from "../domain/errors.js";
import { assertOrderActor } from "../domain/order.rules.js";
import {
    normalizeCreationTaskInput,
} from "../domain/order-creation.rules.js";
import {
    orderCreationProgress,
} from "./order-creation.progress.js";
import {
    assertCreationRpcActive,
} from "./order-creation.request.js";

function defaultSleep(milliseconds, { signal } = {}) {
    return new Promise((resolve) => {
        let timer;

        function finish() {
            clearTimeout(timer);
            signal?.removeEventListener("abort", finish);
            resolve();
        }

        signal?.addEventListener(
            "abort",
            finish,
            { once: true },
        );

        if (signal?.aborted) {
            finish();
            return;
        }

        timer = setTimeout(finish, milliseconds);
    });
}

export function createOrderCreationWaiter({
    progress = orderCreationProgress,
    now = () => performance.now(),
    sleep = defaultSleep,
} = {}) {
    if (
        typeof progress?.advanceOrder !== "function" ||
        typeof now !== "function" ||
        typeof sleep !== "function"
    ) {
        throw new TypeError(
            "Invalid order creation wait dependencies",
        );
    }

    return {
        async waitForOrder(
            operationId,
            input,
            actor,
            {
                timeoutMs = 5000,
                intervalMs = 100,
                deadline = null,
                signal,
            } = {},
        ) {
            if (
                !Number.isInteger(timeoutMs) ||
                timeoutMs < 1 ||
                timeoutMs > 60_000 ||
                !Number.isInteger(intervalMs) ||
                intervalMs < 1 ||
                intervalMs > 1000
            ) {
                throw new DomainError(
                    "INVALID_ARGUMENT",
                    "Invalid order creation wait options",
                );
            }

            assertOrderActor(actor);

            if (
                !input ||
                typeof input !== "object" ||
                Array.isArray(input)
            ) {
                throw new DomainError(
                    "INVALID_ARGUMENT",
                    "Invalid order data",
                );
            }

            const request = normalizeCreationTaskInput({
                operationId,
                uid: actor.uid,
                actorRole: actor.role,
                items: input.items,
            });

            assertCreationRpcActive({ deadline, signal });

            const wallStartedAt = Date.now();

            const budgetMs = Math.min(
                timeoutMs,
                deadline === null
                    ? Infinity
                    : deadline.getTime() - wallStartedAt,
            );

            const finishAt = now() + budgetMs;
            const controller = new AbortController();

            const rpcOptions = {
                deadline: new Date(wallStartedAt + budgetMs),
                signal: controller.signal,
            };

            let budgetTimer;

            function forwardAbort() {
                controller.abort();
            }

            function assertActive() {
                if (now() >= finishAt) {
                    controller.abort();
                }

                assertCreationRpcActive(rpcOptions);
            }

            signal?.addEventListener(
                "abort",
                forwardAbort,
                { once: true },
            );

            try {
                if (signal?.aborted) {
                    forwardAbort();
                }

                assertActive();

                budgetTimer = setTimeout(
                    () => controller.abort(),
                    Math.max(0, budgetMs),
                );

                while (true) {
                    assertActive();

                    const result = await progress.advanceOrder(
                        request.operationId,
                        {
                            items: structuredClone(request.items),
                        },
                        {
                            uid: request.uid,
                            role: request.actorRole,
                        },
                        rpcOptions,
                    );

                    assertActive();

                    if (result !== null) {
                        return result;
                    }

                    const remainingMs = finishAt - now();

                    if (remainingMs <= 0) {
                        controller.abort();
                        assertActive();
                    }

                    try {
                        await sleep(
                            Math.min(intervalMs, remainingMs),
                            { signal: controller.signal },
                        );
                    } catch (error) {
                        assertActive();
                        throw error;
                    }
                }
            } finally {
                clearTimeout(budgetTimer);

                signal?.removeEventListener(
                    "abort",
                    forwardAbort,
                );
            }
        },
    };
}

export const orderCreationWaiter =
    createOrderCreationWaiter();