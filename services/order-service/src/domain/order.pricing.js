import Decimal from "decimal.js";
import { DomainError } from "./errors.js";
import { normalizeOrderInput } from "./order.rules.js";

const Money = Decimal.clone({
    precision: 40,
    rounding: Decimal.ROUND_HALF_UP,
});

const MAX_DISCOUNT = new Money("9999999999999999.99");

export function discountRateForScore(score) {
    if (
        !Number.isInteger(score) ||
        score < 0 ||
        score > 2147483647
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid customer score"
        );
    }

    if (score >= 500) {
        return "0.10";
    }

    if (score >= 100) {
        return "0.05";
    }

    return "0";
}

function normalizeUnitPrice(value) {
    if (typeof value !== "string") {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid product unit price"
        );
    }

    const text = value.trim();

    if (!/^(0|[1-9]\d{0,7})(\.\d{1,2})?$/.test(text)) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid product unit price"
        );
    }

    return new Money(text).toFixed(2);
}

export function calculateOrderPricing(pricedItems, score) {
    const rate = discountRateForScore(score);

    const normalized = normalizeOrderInput({
        items: pricedItems,
    });

    const items = normalized.items.map((item, index) => ({
        pid: item.pid,
        qty: item.qty,
        unit_price: normalizeUnitPrice(
            pricedItems[index].unit_price
        ),
    }));

    let subtotal = new Money("0");

    for (const item of items) {
        const lineTotal = new Money(item.unit_price)
            .times(item.qty);

        subtotal = subtotal.plus(lineTotal);
    }

    const discount = subtotal
        .times(rate)
        .toDecimalPlaces(2, Money.ROUND_HALF_UP);

    if (discount.greaterThan(MAX_DISCOUNT)) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Order discount exceeds supported amount"
        );
    }

    return {
        items,
        subtotal: subtotal.toFixed(2),
        discount: discount.toFixed(2),
    };
}