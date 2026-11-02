import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import {
    createOrderCreationDispatcher,
} from "../src/application/order-creation.dispatcher.js";
import {
    customerClient,
} from "../src/infrastructure/grpc/customer.client.js";
import {
    productClient,
} from "../src/infrastructure/grpc/product.client.js";

function newTask({
    uid = 13,
    status = "PROCESSING",
    score = null,
    failureCode = null,
} = {}) {
    return {
        operation_id: randomUUID(),
        uid,
        actor_role: "CUSTOMER",
        items: [{ pid: 7, qty: 2 }],
        status,
        score,
        failure_code: failureCode,
    };
}

function newScope() {
    return {
        deadline: new Date(Date.now() + 60_000),
        signal: new AbortController().signal,
        timeoutMs: 1000,
    };
}

function assertScope(actual, expected) {
    assert.ok(actual);

    assert.equal(
        actual.deadline.getTime(),
        expected.deadline.getTime(),
    );

    assert.equal(actual.signal, expected.signal);
    assert.equal(actual.timeoutMs, expected.timeoutMs);
}

function harness(
    seeds,
    { due = true, failures = {} } = {},
) {
    const rows = new Map(
        seeds.map((task) => [
            task.operation_id,
            structuredClone(task),
        ]),
    );

    const calls = [];
    const locks = [];
    const actions = [];

    function storedOrder(task) {
        return {
            oid: 41,
            uid: task.uid,
            discount: "2.55",
            items: [{
                pid: 7,
                qty: 2,
                unit_price: "25.50",
            }],
        };
    }

    async function record(
        method,
        uid,
        actor,
        options,
        response,
    ) {
        assert.equal(uid, actor.uid);
        assert.equal(actor.role, "CUSTOMER");

        calls.push({
            method,
            uid,
            options,
        });

        await Promise.resolve();

        if (failures[method]) {
            throw failures[method];
        }

        return response;
    }

    function stock(method, status) {
        return async (id, actor, options) => {
            const task = rows.get(id);

            assert.ok(task);

            return record(
                method,
                task.uid,
                actor,
                options,
                {
                    operationId: id,
                    uid: task.uid,
                    status,
                    items: [{
                        pid: 7,
                        qty: 2,
                        unit_price: "25.50",
                    }],
                },
            );
        };
    }

    const reserve = stock("reserveStock", "RESERVED");

    const dispatcher = createOrderCreationDispatcher({
        transactions: {
            async withLockedTask(id, operation) {
                locks.push(id);

                const existing = rows.get(id);
                assert.ok(existing);

                const draft = structuredClone(existing);

                const context = {
                    async isDue() {
                        return due;
                    },

                    async freezeScore(score) {
                        actions.push("freeze");
                        draft.score = score;
                        return score;
                    },

                    async commitReservedOrder() {
                        actions.push("store");
                        draft.status = "CONFIRM_PENDING";
                        return storedOrder(draft);
                    },

                    async completeOrder() {
                        actions.push("complete");
                        draft.status = "COMPLETED";
                        return storedOrder(draft);
                    },

                    async assertNoStoredOrder() {
                        actions.push("check-no-order");
                    },

                    async requestRelease(code) {
                        actions.push(`release:${code}`);
                        draft.status = "RELEASE_PENDING";
                        draft.failure_code ??= code;
                        return structuredClone(draft);
                    },

                    async scheduleRetry(code) {
                        actions.push(`retry:${code}`);
                        return structuredClone(draft);
                    },

                    async finishRelease() {
                        actions.push("finish-release");
                        draft.status = "FAILED";
                        return structuredClone(draft);
                    },
                };

                const result = await operation(
                    structuredClone(draft),
                    context,
                );

                rows.set(id, draft);

                return result;
            },
        },

        customers: {
            async getCustomer(uid, actor, options) {
                return record(
                    "getCustomer",
                    uid,
                    actor,
                    options,
                    { uid, score: 100 },
                );
            },
        },

        products: {
            async reserveStock(id, items, actor, options) {
                assert.deepEqual(items, [{ pid: 7, qty: 2 }]);

                return reserve(id, actor, options);
            },

            confirmStock: stock(
                "confirmStock",
                "CONFIRMED",
            ),

            releaseStock: stock(
                "releaseStock",
                "RELEASED",
            ),
        },
    });

    return {
        dispatcher,
        rows,
        calls,
        locks,
        actions,
    };
}

async function main() {
    const phases = [
        {
            task: newTask(),
            method: "getCustomer",
            finalStatus: "PROCESSING",
        },
        {
            task: newTask({ score: 100 }),
            method: "reserveStock",
            finalStatus: "CONFIRM_PENDING",
        },
        {
            task: newTask({
                status: "CONFIRM_PENDING",
                score: 100,
            }),
            method: "confirmStock",
            finalStatus: "COMPLETED",
        },
        {
            task: newTask({
                status: "RELEASE_PENDING",
                score: 100,
                failureCode: "CONFLICT",
            }),
            method: "releaseStock",
            finalStatus: "FAILED",
        },
    ];

    const successful = [];

    for (const phase of phases) {
        const test = harness([phase.task]);
        const scope = newScope();

        await test.dispatcher.processDueTask(
            phase.task.operation_id,
            scope,
        );

        assert.equal(test.calls.length, 1);
        assert.equal(test.calls[0].method, phase.method);

        assertScope(test.calls[0].options, scope);

        assert.equal(
            test.rows.get(phase.task.operation_id).status,
            phase.finalStatus,
        );

        successful.push(test);
    }

    console.log("RPC options reach all four creation phases: OK");

    for (const test of successful) {
        // Processor phải dùng scope transaction của dispatcher.
        assert.equal(test.locks.length, 1);
    }

    console.log("Processor reuses the dispatcher transaction scope: OK");

    for (const phase of phases) {
        const test = harness([phase.task], {
            failures: {
                [phase.method]: new DomainError(
                    "SERVICE_UNAVAILABLE",
                    "Cancelled or expired fixture RPC",
                ),
            },
        });

        const scope = newScope();

        await test.dispatcher.processDueTask(
            phase.task.operation_id,
            scope,
        );

        assertScope(test.calls[0].options, scope);

        assert.ok(
            test.actions.includes(
                "retry:SERVICE_UNAVAILABLE",
            ),
        );

        assert.equal(
            test.actions.some(
                (action) => action.startsWith("release:"),
            ),
            false,
        );

        assert.equal(
            test.actions.includes("finish-release"),
            false,
        );

        const remaining = test.rows.get(
            phase.task.operation_id,
        );

        assert.equal(
            remaining.status,
            phase.task.status,
        );

        assert.equal(
            remaining.failure_code,
            phase.task.failure_code,
        );
    }

    console.log("Temporary RPC failures retry without new release decisions: OK");

    const missingTask = newTask();

    const missing = harness([missingTask], {
        failures: {
            getCustomer: new DomainError(
                "NOT_FOUND",
                "Customer not found",
            ),
        },
    });

    await missing.dispatcher.processDueTask(
        missingTask.operation_id,
        newScope(),
    );

    assert.equal(
        missing.rows.get(missingTask.operation_id).status,
        "RELEASE_PENDING",
    );

    assert.equal(
        missing.rows.get(missingTask.operation_id).failure_code,
        "NOT_FOUND",
    );

    for (const phase of phases.slice(2)) {
        const test = harness([phase.task], {
            failures: {
                [phase.method]: new DomainError(
                    "FORBIDDEN",
                    "Fixture permission error",
                ),
            },
        });

        await test.dispatcher.processDueTask(
            phase.task.operation_id,
            newScope(),
        );

        assert.ok(test.actions.includes("retry:INTERNAL"));

        assert.equal(
            test.actions.some(
                (action) => action.startsWith("release:"),
            ),
            false,
        );

        assert.equal(
            test.rows.get(phase.task.operation_id).status,
            phase.task.status,
        );

        assert.equal(
            test.rows.get(phase.task.operation_id).failure_code,
            phase.task.failure_code,
        );
    }

    console.log("Permanent failure and finalization policies preserved: OK");

    const futureTask = newTask();
    const future = harness([futureTask], { due: false });

    const deferred = await future.dispatcher.processDueTask(
        futureTask.operation_id,
        newScope(),
    );

    assert.equal(deferred.outcome, "NOT_DUE");
    assert.equal(future.calls.length, 0);

    assert.deepEqual(
        future.rows.get(futureTask.operation_id),
        futureTask,
    );

    for (const status of ["COMPLETED", "FAILED"]) {
        const task = newTask({
            status,
            score: 100,
            failureCode: status === "FAILED"
                ? "CONFLICT"
                : null,
        });

        const test = harness([task]);

        await test.dispatcher.processDueTask(
            task.operation_id,
            newScope(),
        );

        assert.equal(test.calls.length, 0);
        assert.deepEqual(
            test.rows.get(task.operation_id),
            task,
        );
    }

    console.log("Future and closed tasks still skip RPC calls: OK");

    const first = newTask({ uid: 13 });
    const second = newTask({ uid: 14 });

    const concurrent = harness([first, second]);

    const firstScope = newScope();
    const secondScope = newScope();

    await Promise.all([
        concurrent.dispatcher.processDueTask(
            first.operation_id,
            firstScope,
        ),
        concurrent.dispatcher.processDueTask(
            second.operation_id,
            secondScope,
        ),
    ]);

    assert.equal(concurrent.calls.length, 2);

    assertScope(
        concurrent.calls.find((call) => call.uid === 13).options,
        firstScope,
    );

    assertScope(
        concurrent.calls.find((call) => call.uid === 14).options,
        secondScope,
    );

    const workerTask = newTask();
    const worker = harness([workerTask]);

    await worker.dispatcher.processDueTask(
        workerTask.operation_id,
    );

    const defaults = worker.calls[0].options;

    assert.ok(defaults);
    assert.equal(defaults.deadline, undefined);
    assert.equal(defaults.signal, undefined);
    assert.equal(defaults.timeoutMs, undefined);

    console.log("Concurrent request scopes and worker defaults remain isolated: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Creation RPC scope check failed:",
        error.code ?? error.name,
    );

    process.exitCode = 1;
} finally {
    customerClient.close();
    productClient.close();

    console.log("Creation RPC scope test connections closed: OK");
}