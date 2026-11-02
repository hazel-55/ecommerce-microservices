import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import {
    normalizeOrderCreatedEvent,
} from "../src/domain/order-created.rules.js";

const event = {
    eventId: randomUUID(),
    eventType: "OrderCreated",
    version: 1,
    oid: 101,
    uid: 13,
    discount: "2.55",
    items: [
        { pid: 2, qty: 1, unit_price: "10.00" },
        { pid: 1, qty: 2, unit_price: "20.50" },
    ],
};

function expectInvalid(input) {
    assert.throws(
        () => normalizeOrderCreatedEvent(input),
        (error) => {
            assert.ok(error instanceof DomainError);
            assert.equal(error.code, "INVALID_ARGUMENT");
            return true;
        }
    );
}

try {
    const normalized = normalizeOrderCreatedEvent(event);

    assert.deepEqual(normalized, {
        eventId: event.eventId,
        eventType: "OrderCreated",
        version: 1,
        oid: 101,
        uid: 13,
        discount: "2.55",
        items: [
            { pid: 1, qty: 2, unit_price: "20.50" },
            { pid: 2, qty: 1, unit_price: "10.00" },
        ],
    });

    assert.equal(typeof normalized.discount, "string");
    assert.equal(
        typeof normalized.items[0].unit_price,
        "string"
    );

    console.log("Canonical OrderCreated event and money strings: OK");

    for (const input of [null, [], "invalid"]) {
        expectInvalid(input);
    }

    for (const field of [
        "eventId",
        "eventType",
        "version",
        "oid",
        "uid",
        "discount",
    ]) {
        const invalid = structuredClone(event);
        delete invalid[field];
        expectInvalid(invalid);
    }

    const invalidFields = [
        ["eventId", "invalid"],
        ["eventId", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"],
        ["eventType", "OtherEvent"],
        ["version", 2],
        ["version", "1"],
        ["oid", 0],
        ["oid", 2147483648],
        ["uid", "13"],
        ["discount", 2.55],
        ["discount", "-1.00"],
        ["discount", "01.00"],
        ["discount", "0.001"],
        ["discount", "10000000000000000.00"],
    ];

    for (const [field, value] of invalidFields) {
        expectInvalid({
            ...structuredClone(event),
            [field]: value,
        });
    }

    console.log(
        "Missing identity, invalid contract and money rejected: OK"
    );

    for (const items of [undefined, null, {}, [], [null]]) {
        expectInvalid({
            ...structuredClone(event),
            items,
        });
    }

    for (const [field, value] of [
        ["pid", 0],
        ["pid", "1"],
        ["qty", 0],
        ["qty", 1.5],
        ["qty", 2147483648],
        ["unit_price", 20.5],
        ["unit_price", "20.500"],
        ["unit_price", "100000000.00"],
    ]) {
        const invalid = structuredClone(event);
        invalid.items[0][field] = value;
        expectInvalid(invalid);
    }

    const duplicate = structuredClone(event);

    duplicate.items.push(
        structuredClone(duplicate.items[0])
    );

    expectInvalid(duplicate);

    console.log("Invalid and duplicate order items rejected: OK");

    const extended = structuredClone(event);

    extended.shipid = 999;
    extended.status = "DELIVERED";
    extended.items[0].internalFlag = true;

    const original = structuredClone(extended);
    const result = normalizeOrderCreatedEvent(extended);

    assert.deepEqual(result, normalized);
    assert.deepEqual(extended, original);
    assert.equal("shipid" in result, false);
    assert.equal("status" in result, false);

    console.log(
        "Server-owned fields ignored and input unchanged: OK"
    );
} catch (error) {
    console.error(
        "OrderCreated event check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
}