import assert from "node:assert/strict";
import { DomainError } from "../src/domain/errors.js";
import {
    validateId,
    normalizeOrderInput,
    assertOrderActor,
    assertOrderAccess,
} from "../src/domain/order.rules.js";

function expectDomainError(operation, code) {
    assert.throws(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);

        return true;
    });
}

const input = {
    uid: 999,
    role: "ADMIN",
    discount: 999,
    items: [
        {
            pid: 1,
            qty: 2,
            unit_price: 0,
        },
        {
            pid: 2,
            qty: 1,
        },
    ],
};

const originalInput = JSON.stringify(input);

assert.deepEqual(normalizeOrderInput(input), {
    items: [
        { pid: 1, qty: 2 },
        { pid: 2, qty: 1 },
    ],
});

assert.equal(JSON.stringify(input), originalInput);

validateId(1);
validateId(2147483647);

console.log("Order item input normalization: OK");

for (const invalidInput of [
    null,
    [],
    {},
    { items: null },
    { items: [] },
    { items: [null] },
    { items: [[]] },
    { items: [{ pid: 0, qty: 1 }] },
    { items: [{ pid: "1", qty: 1 }] },
    { items: [{ pid: 2147483648, qty: 1 }] },
    { items: [{ pid: 1, qty: 0 }] },
    { items: [{ pid: 1, qty: -1 }] },
    { items: [{ pid: 1, qty: 1.5 }] },
    { items: [{ pid: 1, qty: "2" }] },
    { items: [{ pid: 1, qty: 2147483648 }] },
    { items: [{ pid: 1 }] },
]) {
    expectDomainError(
        () => normalizeOrderInput(invalidInput),
        "INVALID_ARGUMENT"
    );
}

console.log("Invalid order input rejected: OK");

expectDomainError(
    () => normalizeOrderInput({
        items: [
            { pid: 1, qty: 1 },
            { pid: 1, qty: 2 },
        ],
    }),
    "INVALID_ARGUMENT"
);

console.log("Duplicate product IDs rejected: OK");

const customer = { uid: 13, role: "CUSTOMER" };
const admin = { uid: 1, role: "ADMIN" };
const staff = { uid: 2, role: "SHIPMENT_STAFF" };

assertOrderActor(customer);
assertOrderActor(admin);
assertOrderActor(staff);

assertOrderAccess(customer, 13);
assertOrderAccess(admin, 13);
assertOrderAccess(staff, 2);

expectDomainError(
    () => assertOrderAccess(customer, 14),
    "FORBIDDEN"
);

expectDomainError(
    () => assertOrderAccess(staff, 13),
    "FORBIDDEN"
);

for (const invalidActor of [
    undefined,
    { uid: 0, role: "CUSTOMER" },
    { uid: "13", role: "CUSTOMER" },
    { uid: 2147483648, role: "ADMIN" },
]) {
    expectDomainError(
        () => assertOrderActor(invalidActor),
        "UNAUTHENTICATED"
    );
}

expectDomainError(
    () => assertOrderActor({
        uid: 13,
        role: "UNKNOWN",
    }),
    "FORBIDDEN"
);

console.log("Order actor and ownership permissions: OK");