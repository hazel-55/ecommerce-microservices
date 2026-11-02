import assert from "node:assert/strict";
import { DomainError } from "../src/domain/errors.js";
import {
    validateId,
    normalizePrice,
    normalizeProductInput,
    assertProductRead,
    assertProductAdmin,
} from "../src/domain/product.rules.js";

function expectDomainError(operation, expectedCode) {
    assert.throws(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, expectedCode);
        return true;
    });
}

const validInput = {
    pname: "Test product",
    price: "25.00",
    quantity: 10,
};

try {
    assert.deepEqual(
        normalizeProductInput({
            pname: "  Test product  ",
            price: "25",
            quantity: 10,
            pid: 999,
            role: "ADMIN",
        }),
        validInput,
    );

    console.log("Product input normalization: OK");

    assert.equal(normalizePrice("25"), "25.00");
    assert.equal(normalizePrice("25.5"), "25.50");
    assert.equal(normalizePrice("0"), "0.00");
    assert.equal(normalizePrice("99999999.99"), "99999999.99");

    for (const price of [
        25,
        "-1.00",
        "1.001",
        "1e3",
        "100000000.00",
        "01.00",
    ]) {
        expectDomainError(
            () => normalizePrice(price),
            "INVALID_ARGUMENT",
        );
    }

    console.log("Decimal price validation: OK");

    validateId(1);
    validateId(2147483647);

    for (const pid of [0, -1, 1.5, "1", 2147483648]) {
        expectDomainError(
            () => validateId(pid),
            "INVALID_ARGUMENT",
        );
    }

    for (const input of [
        null,
        [],
        {},
        { ...validInput, pname: "   " },
        { ...validInput, pname: "x".repeat(101) },
        { ...validInput, quantity: -1 },
        { ...validInput, quantity: 1.5 },
        { ...validInput, quantity: "10" },
        { ...validInput, quantity: 2147483648 },
    ]) {
        expectDomainError(
            () => normalizeProductInput(input),
            "INVALID_ARGUMENT",
        );
    }

    assert.equal(
        normalizeProductInput({
            ...validInput,
            quantity: 0,
        }).quantity,
        0,
    );

    console.log("Invalid product data rejected: OK");

    for (const role of ["CUSTOMER", "ADMIN", "SHIPMENT_STAFF"]) {
        assertProductRead({ uid: 1, role });
    }

    assertProductAdmin({ uid: 1, role: "ADMIN" });

    for (const role of ["CUSTOMER", "SHIPMENT_STAFF"]) {
        expectDomainError(
            () => assertProductAdmin({ uid: 1, role }),
            "FORBIDDEN",
        );
    }

    expectDomainError(
        () => assertProductRead(null),
        "UNAUTHENTICATED",
    );

    expectDomainError(
        () => assertProductRead({ uid: 0, role: "ADMIN" }),
        "UNAUTHENTICATED",
    );

    expectDomainError(
        () => assertProductRead({ uid: 1, role: "UNKNOWN" }),
        "FORBIDDEN",
    );

    console.log("Product read and admin permissions: OK");
} catch (error) {
    console.error("Product domain check failed:", error.message);
    process.exitCode = 1;
}