import assert from "node:assert/strict";
import { DomainError } from "../src/domain/errors.js";
import {
    discountRateForScore,
    calculateOrderPricing,
} from "../src/domain/order.pricing.js";

function expectInvalid(operation) {
    assert.throws(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, "INVALID_ARGUMENT");

        return true;
    });
}

for (const [score, rate, discount] of [
    [0, "0", "0.00"],
    [99, "0", "0.00"],
    [100, "0.05", "5.00"],
    [499, "0.05", "5.00"],
    [500, "0.10", "10.00"],
    [2147483647, "0.10", "10.00"],
]) {
    assert.equal(discountRateForScore(score), rate);

    const result = calculateOrderPricing(
        [{ pid: 1, qty: 1, unit_price: "100.00" }],
        score
    );

    assert.equal(result.subtotal, "100.00");
    assert.equal(result.discount, discount);
}

console.log("Score discount thresholds: OK");

const decimalResult = calculateOrderPricing(
    [
        { pid: 1, qty: 1, unit_price: "0.10" },
        { pid: 2, qty: 1, unit_price: "0.20" },
    ],
    0
);

assert.equal(decimalResult.subtotal, "0.30");
assert.equal(decimalResult.discount, "0.00");

const quantityResult = calculateOrderPricing(
    [{ pid: 1, qty: 3, unit_price: "25.50" }],
    100
);

assert.equal(quantityResult.subtotal, "76.50");
assert.equal(quantityResult.discount, "3.83");

console.log("Exact decimal arithmetic: OK");

const halfUpResult = calculateOrderPricing(
    [{ pid: 1, qty: 1, unit_price: "0.10" }],
    100
);

// 0.10 * 5% = 0.005 -> HALF_UP = 0.01
assert.equal(halfUpResult.discount, "0.01");

const aggregateRounding = calculateOrderPricing(
    [
        { pid: 1, qty: 1, unit_price: "0.05" },
        { pid: 2, qty: 1, unit_price: "0.05" },
    ],
    100
);

// Làm tròn một lần trên tổng, không làm tròn từng item.
assert.equal(aggregateRounding.subtotal, "0.10");
assert.equal(aggregateRounding.discount, "0.01");

console.log("HALF_UP discount rounding on order subtotal: OK");

const input = [
    { pid: 1, qty: 2, unit_price: "25.5" },
    { pid: 2, qty: 1, unit_price: "0" },
];

const originalInput = JSON.stringify(input);
const snapshot = calculateOrderPricing(input, 500);

assert.deepEqual(snapshot, {
    items: [
        { pid: 1, qty: 2, unit_price: "25.50" },
        { pid: 2, qty: 1, unit_price: "0.00" },
    ],
    subtotal: "51.00",
    discount: "5.10",
});

assert.equal(JSON.stringify(input), originalInput);

console.log("Normalized price snapshots and unchanged input: OK");

for (const score of [
    -1,
    1.5,
    "100",
    null,
    2147483648,
]) {
    expectInvalid(() => discountRateForScore(score));
}

for (const price of [
    25,
    "",
    "-1",
    "1.001",
    "1e2",
    "01.00",
    "100000000.00",
    "NaN",
    "Infinity",
]) {
    expectInvalid(() => calculateOrderPricing(
        [{ pid: 1, qty: 1, unit_price: price }],
        100
    ));
}

expectInvalid(() => calculateOrderPricing(
    [
        { pid: 1, qty: 1, unit_price: "10.00" },
        { pid: 1, qty: 2, unit_price: "10.00" },
    ],
    100
));

expectInvalid(() => calculateOrderPricing(
    [{
        pid: 1,
        qty: 2147483647,
        unit_price: "99999999.99",
    }],
    500
));

console.log("Invalid pricing data and discount overflow rejected: OK");