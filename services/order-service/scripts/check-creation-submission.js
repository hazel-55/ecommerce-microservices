import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

import { DomainError } from "../src/domain/errors.js";
import {
    assertSameCreationTask,
} from "../src/domain/order-creation.rules.js";
import {
    createOrderCreationApplication,
} from "../src/application/order-creation.application.js";

const records = new Map();
const calls = [];

let inserts = 0;
let failBefore;
let loseReply = false;

const lostReplyError = new Error(
    "Simulated result loss after persistence"
);

const tasks = {
    async createTask(request, ...options) {
        calls.push({
            request: structuredClone(request),
            options: structuredClone(options),
        });

        if (failBefore) {
            throw failBefore;
        }

        let row = records.get(request.operationId);

        if (row) {
            assertSameCreationTask(row, request);
        } else {
            row = {
                operation_id: request.operationId,
                uid: request.uid,
                actor_role: request.actorRole,
                items: structuredClone(request.items),
                status: "PROCESSING",
                score: null,
            };

            records.set(request.operationId, row);
            inserts += 1;
        }

        if (loseReply) {
            loseReply = false;
            throw lostReplyError;
        }

        return structuredClone(row);
    },
};

async function expectDomain(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    const application = createOrderCreationApplication({
        tasks,
    });

    const actor = { uid: 13, role: "CUSTOMER" };
    const operationId = randomUUID();

    const input = {
        uid: 999,
        actorRole: "ADMIN",
        operationId: randomUUID(),
        score: 999,
        status: "COMPLETED",
        nextAttemptAt: "2099-01-01T00:00:00.000Z",
        items: [
            {
                pid: 2,
                qty: 1,
                unit_price: "0.01",
            },
            {
                pid: 1,
                qty: 2,
            },
        ],
    };

    const originalInput = structuredClone(input);

    const first = await application.submitOrder(
        operationId,
        input,
        actor
    );

    assert.deepEqual(calls[0], {
        request: {
            operationId,
            uid: 13,
            actorRole: "CUSTOMER",
            items: [
                { pid: 1, qty: 2 },
                { pid: 2, qty: 1 },
            ],
        },
        options: [],
    });

    assert.equal(first.operation_id, operationId);
    assert.equal(first.uid, 13);
    assert.equal(first.actor_role, "CUSTOMER");
    assert.equal(first.status, "PROCESSING");
    assert.equal(first.score, null);
    assert.deepEqual(input, originalInput);

    console.log(
        "Trusted actor and normalized creation task: OK"
    );

    const callsBeforeInvalid = calls.length;

    for (const [invalidActor, code] of [
        [null, "UNAUTHENTICATED"],
        [{ uid: 0, role: "CUSTOMER" }, "UNAUTHENTICATED"],
        [{ uid: 13, role: "ROOT" }, "FORBIDDEN"],
    ]) {
        await expectDomain(
            () => application.submitOrder(
                randomUUID(),
                input,
                invalidActor
            ),
            code
        );
    }

    for (const body of [
        null,
        [],
        {},
        { items: [] },
        {
            items: [
                { pid: 1, qty: 1 },
                { pid: 1, qty: 2 },
            ],
        },
    ]) {
        await expectDomain(
            () => application.submitOrder(
                randomUUID(),
                body,
                actor
            ),
            "INVALID_ARGUMENT"
        );
    }

    await expectDomain(
        () => application.submitOrder("invalid", input, actor),
        "INVALID_ARGUMENT"
    );

    assert.equal(calls.length, callsBeforeInvalid);

    assert.throws(
        () => createOrderCreationApplication({ tasks: {} }),
        TypeError
    );

    console.log(
        "Invalid submissions rejected before task repository: OK"
    );

    // Simulate a task that has progressed after submission.
    const stored = records.get(operationId);
    stored.status = "COMPLETED";
    stored.score = 100;

    const progressed = structuredClone(stored);

    const replay = await application.submitOrder(
        operationId,
        {
            ...input,
            items: [...input.items].reverse(),
        },
        actor
    );

    assert.deepEqual(replay, progressed);
    assert.equal(inserts, 1);

    await expectDomain(
        () => application.submitOrder(
            operationId,
            input,
            { uid: 14, role: "CUSTOMER" }
        ),
        "FORBIDDEN"
    );

    await expectDomain(
        () => application.submitOrder(
            operationId,
            {
                items: [
                    { pid: 1, qty: 3 },
                    { pid: 2, qty: 1 },
                ],
            },
            actor
        ),
        "CONFLICT"
    );

    await expectDomain(
        () => application.submitOrder(
            operationId,
            input,
            { uid: 13, role: "ADMIN" }
        ),
        "CONFLICT"
    );

    assert.deepEqual(records.get(operationId), progressed);
    assert.equal(inserts, 1);

    console.log(
        "Replay preserves stored task state and request binding: OK"
    );

    const failedOperation = randomUUID();
    const repositoryError = new Error("Simulated repository failure");

    failBefore = repositoryError;

    await assert.rejects(
        () => application.submitOrder(
            failedOperation,
            input,
            actor
        ),
        (error) => error === repositoryError
    );

    assert.equal(records.has(failedOperation), false);
    failBefore = undefined;

    const lostOperation = randomUUID();
    loseReply = true;

    await assert.rejects(
        () => application.submitOrder(
            lostOperation,
            input,
            actor
        ),
        (error) => error === lostReplyError
    );

    assert.ok(records.has(lostOperation));

    const insertsAfterLostReply = inserts;

    const recreatedApplication =
        createOrderCreationApplication({ tasks });

    const recovered = await recreatedApplication.submitOrder(
        lostOperation,
        input,
        actor
    );

    assert.equal(recovered.operation_id, lostOperation);
    assert.equal(inserts, insertsAfterLostReply);

    assert.equal(
        calls.at(-1).request.operationId,
        lostOperation
    );

    assert.deepEqual(records.get(operationId), progressed);

    console.log(
        "Repository errors and lost-result replay preserved: OK"
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Creation submission check failed:",
        error.code ?? error.name
    );

    process.exitCode = 1;
} finally {
    records.clear();
}