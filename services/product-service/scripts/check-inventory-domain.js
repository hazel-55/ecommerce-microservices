import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import {
    validateOperationId,
    normalizeStockCommand,
    normalizeReservationRequest,
} from "../src/domain/inventory.rules.js";

const operationId = randomUUID();

function expectInvalid(operation) {
    assert.throws(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, "INVALID_ARGUMENT");

        return true;
    });
}

validateOperationId(operationId);

assert.deepEqual(
    normalizeStockCommand({
        operationId,
        uid: 13,
        status: "CONFIRMED",
    }),
    { operationId, uid: 13 }
);

assert.deepEqual(
    normalizeStockCommand({
        operationId,
        uid: 2147483647,
    }),
    { operationId, uid: 2147483647 }
);

console.log("Stock operation identity and UID validation: OK");

const input = {
    operationId,
    uid: 13,
    status: "CONFIRMED",
    reservedQuantity: 999,
    items: [
        {
            pid: 2,
            qty: 1,
            price: 0,
            unit_price: "0.00",
        },
        {
            pid: 1,
            qty: 2,
        },
    ],
};

const originalInput = JSON.stringify(input);
const normalized = normalizeReservationRequest(input);

assert.deepEqual(normalized.items, [
    { pid: 1, qty: 2 },
    { pid: 2, qty: 1 },
]);

console.log("Reservation item normalization and sorting: OK");

for (const invalidCommand of [
    null,
    [],
    {},
    { operationId: "invalid", uid: 13 },
    {
        operationId: "123E4567-E89B-42D3-A456-426614174000",
        uid: 13,
    },
    { operationId, uid: 0 },
    { operationId, uid: "13" },
    { operationId, uid: 1.5 },
    { operationId, uid: 2147483648 },
]) {
    expectInvalid(
        () => normalizeStockCommand(invalidCommand)
    );
}

for (const invalidItems of [
    undefined,
    null,
    [],
    [null],
    [[]],
    [{ pid: 0, qty: 1 }],
    [{ pid: "1", qty: 1 }],
    [{ pid: 2147483648, qty: 1 }],
    [{ pid: 1, qty: 0 }],
    [{ pid: 1, qty: -1 }],
    [{ pid: 1, qty: 1.5 }],
    [{ pid: 1, qty: "2" }],
    [{ pid: 1, qty: 2147483648 }],
    [
        { pid: 1, qty: 1 },
        { pid: 1, qty: 2 },
    ],
]) {
    expectInvalid(
        () => normalizeReservationRequest({
            operationId,
            uid: 13,
            items: invalidItems,
        })
    );
}

console.log("Invalid and duplicate reservation input rejected: OK");

assert.deepEqual(normalized, {
    operationId,
    uid: 13,
    items: [
        { pid: 1, qty: 2 },
        { pid: 2, qty: 1 },
    ],
});

assert.equal(JSON.stringify(input), originalInput);

console.log("Server-owned fields ignored and input unchanged: OK");