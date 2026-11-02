import assert from "node:assert/strict";
import { DomainError } from "../src/domain/errors.js";
import {
    validateId,
    validateShipmentStatus,
    normalizeShipmentStatusInput,
    assertShipmentActor,
    assertShipmentAccess,
    assertShipmentMutationActor,
    assertShipmentStatusChange,
} from "../src/domain/shipment.rules.js";

const customer = { uid: 13, role: "CUSTOMER" };
const admin = { uid: 99, role: "ADMIN" };
const staff = { uid: 88, role: "SHIPMENT_STAFF" };

function expectDomain(operation, code) {
    assert.throws(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

try {
    assert.equal(validateId(1), 1);
    assert.equal(validateId(2147483647), 2147483647);

    for (const id of [
        0, -1, 1.5, 2147483648, NaN, "13", null,
    ]) {
        expectDomain(
            () => validateId(id),
            "INVALID_ARGUMENT"
        );
    }

    for (const status of [
        "PENDING",
        "SHIPPING",
        "DELIVERED",
        "CANCELLED",
    ]) {
        assert.equal(
            validateShipmentStatus(status),
            status
        );
    }

    for (const status of [
        "", "shipping", " SHIPPING ", "UNKNOWN", null,
    ]) {
        expectDomain(
            () => validateShipmentStatus(status),
            "INVALID_ARGUMENT"
        );
    }

    for (const input of [
        null, [], "SHIPPING", {}, { status: "UNKNOWN" },
    ]) {
        expectDomain(
            () => normalizeShipmentStatusInput(input),
            "INVALID_ARGUMENT"
        );
    }

    const input = {
        status: "SHIPPING",
        uid: 999,
        oid: 999,
        shipid: 999,
        sourceEventId: "ignored",
    };

    const original = structuredClone(input);

    assert.deepEqual(
        normalizeShipmentStatusInput(input),
        { status: "SHIPPING" }
    );

    assert.deepEqual(input, original);

    console.log("Shipment ID and status input validation: OK");

    assertShipmentAccess(customer, 13);
    assertShipmentAccess(admin, 13);
    assertShipmentAccess(staff, 13);

    expectDomain(
        () => assertShipmentAccess(customer, 14),
        "FORBIDDEN"
    );

    for (const actor of [
        null,
        {},
        [],
        { uid: 0, role: "CUSTOMER" },
        { uid: "13", role: "CUSTOMER" },
    ]) {
        expectDomain(
            () => assertShipmentActor(actor),
            "UNAUTHENTICATED"
        );
    }

    expectDomain(
        () => assertShipmentActor({
            uid: 13,
            role: "ROOT",
        }),
        "FORBIDDEN"
    );

    console.log(
        "Shipment read ownership and staff/admin access: OK"
    );

    assertShipmentMutationActor(admin);
    assertShipmentMutationActor(staff);

    expectDomain(
        () => assertShipmentMutationActor(customer),
        "FORBIDDEN"
    );

    expectDomain(
        () => assertShipmentStatusChange(
            customer,
            "PENDING",
            "SHIPPING"
        ),
        "FORBIDDEN"
    );

    // Permission is checked before inspecting stored status.
    expectDomain(
        () => assertShipmentStatusChange(
            customer,
            "INVALID",
            "INVALID"
        ),
        "FORBIDDEN"
    );

    console.log("Shipment mutation role permissions: OK");

    const allowed = [
        ["PENDING", "SHIPPING"],
        ["PENDING", "CANCELLED"],
        ["SHIPPING", "DELIVERED"],
        ["SHIPPING", "CANCELLED"],
    ];

    for (const actor of [admin, staff]) {
        for (const [current, next] of allowed) {
            assert.equal(
                assertShipmentStatusChange(actor, current, next),
                next
            );
        }

        for (const status of [
            "PENDING",
            "SHIPPING",
            "DELIVERED",
            "CANCELLED",
        ]) {
            assert.equal(
                assertShipmentStatusChange(actor, status, status),
                status
            );
        }
    }

    for (const [current, next] of [
        ["PENDING", "DELIVERED"],
        ["SHIPPING", "PENDING"],
        ["DELIVERED", "PENDING"],
        ["DELIVERED", "SHIPPING"],
        ["DELIVERED", "CANCELLED"],
        ["CANCELLED", "PENDING"],
        ["CANCELLED", "SHIPPING"],
        ["CANCELLED", "DELIVERED"],
    ]) {
        expectDomain(
            () => assertShipmentStatusChange(
                admin,
                current,
                next
            ),
            "INVALID_ARGUMENT"
        );
    }

    expectDomain(
        () => assertShipmentStatusChange(
            admin,
            "UNKNOWN",
            "SHIPPING"
        ),
        "INTERNAL"
    );

    expectDomain(
        () => assertShipmentStatusChange(
            admin,
            "PENDING",
            "UNKNOWN"
        ),
        "INVALID_ARGUMENT"
    );

    console.log(
        "Shipment status transitions and terminal states: OK"
    );
} catch (error) {
    console.error(
        "Shipment domain check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
}