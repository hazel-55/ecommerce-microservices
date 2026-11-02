import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import {
    createShipmentApplication,
} from "../src/application/shipment.application.js";

const customer = { uid: 13, role: "CUSTOMER" };
const admin = { uid: 99, role: "ADMIN" };
const staff = { uid: 88, role: "SHIPMENT_STAFF" };

const storedShipment = {
    shipid: 501,
    oid: 1001,
    uid: 13,
    sourceEventId: randomUUID(),
    status: "PENDING",
};

function fixture({
    record = storedShipment,
    onUpdate,
} = {}) {
    const state = {
        shipment: structuredClone(record),
        readError: null,
    };

    const calls = {
        reads: [],
        updates: [],
        events: [],
    };

    const repository = {
        async findById(shipid) {
            calls.reads.push(shipid);

            if (state.readError) {
                throw state.readError;
            }

            if (state.shipment?.shipid !== shipid) {
                return null;
            }

            return structuredClone(state.shipment);
        },

        async updateStatus(
            shipid,
            expectedStatus,
            nextStatus
        ) {
            calls.updates.push([
                shipid,
                expectedStatus,
                nextStatus,
            ]);

            if (onUpdate) {
                const result = await onUpdate({
                    shipid,
                    expectedStatus,
                    nextStatus,
                    state,
                });

                if (result !== undefined) {
                    return result;
                }
            }

            if (
                state.shipment?.shipid !== shipid ||
                state.shipment.status !== expectedStatus
            ) {
                return null;
            }

            state.shipment = {
                ...state.shipment,
                status: nextStatus,
            };

            return structuredClone(state.shipment);
        },

        async createFromOrderEvent(event) {
            calls.events.push(structuredClone(event));

            state.shipment = {
                shipid: 501,
                oid: event.oid,
                uid: event.uid,
                sourceEventId: event.eventId,
                status: "PENDING",
            };

            return {
                created: true,
                shipment: structuredClone(state.shipment),
            };
        },
    };

    return {
        state,
        calls,
        application: createShipmentApplication({
            repository,
        }),
    };
}

async function expectDomain(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    const reading = fixture();

    for (const actor of [customer, admin, staff]) {
        assert.deepEqual(
            await reading.application.getShipment(501, actor),
            {
                shipid: 501,
                oid: 1001,
                status: "PENDING",
            }
        );
    }

    await expectDomain(
        () => reading.application.getShipment(
            501,
            { uid: 14, role: "CUSTOMER" }
        ),
        "FORBIDDEN"
    );

    console.log(
        "Shipment application read permissions and response fields: OK"
    );

    const validation = fixture();

    await expectDomain(
        () => validation.application.getShipment(501, null),
        "UNAUTHENTICATED"
    );

    await expectDomain(
        () => validation.application.getShipment(0, admin),
        "INVALID_ARGUMENT"
    );

    await expectDomain(
        () => validation.application.updateShipmentStatus(
            501,
            { status: "SHIPPING" },
            customer
        ),
        "FORBIDDEN"
    );

    await expectDomain(
        () => validation.application.updateShipmentStatus(
            501,
            { status: "UNKNOWN" },
            admin
        ),
        "INVALID_ARGUMENT"
    );

    assert.equal(validation.calls.reads.length, 0);
    assert.equal(validation.calls.updates.length, 0);

    console.log(
        "Shipment application mutation roles and input validation: OK"
    );

    const updating = fixture();

    const updated = await updating.application.updateShipmentStatus(
        501,
        {
            status: "SHIPPING",
            uid: 999,
            oid: 999,
            shipid: 999,
        },
        admin
    );

    assert.deepEqual(updated, {
        shipid: 501,
        oid: 1001,
        status: "SHIPPING",
    });

    assert.deepEqual(
        updating.calls.updates[0],
        [501, "PENDING", "SHIPPING"]
    );

    assert.equal(updating.state.shipment.uid, 13);

    await updating.application.updateShipmentStatus(
        501,
        { status: "DELIVERED" },
        staff
    );

    assert.deepEqual(
        await updating.application.updateShipmentStatus(
            501,
            { status: "DELIVERED" },
            staff
        ),
        {
            shipid: 501,
            oid: 1001,
            status: "DELIVERED",
        }
    );

    console.log(
        "Shipment status update and repeated request behavior: OK"
    );

    let raced = false;

    const sameTargetRace = fixture({
        onUpdate({ state }) {
            if (!raced) {
                raced = true;
                state.shipment.status = "SHIPPING";
                return null;
            }
        },
    });

    assert.equal(
        (
            await sameTargetRace.application.updateShipmentStatus(
                501,
                { status: "SHIPPING" },
                admin
            )
        ).status,
        "SHIPPING"
    );

    const terminalRace = fixture({
        onUpdate({ state }) {
            state.shipment.status = "CANCELLED";
            return null;
        },
    });

    await expectDomain(
        () => terminalRace.application.updateShipmentStatus(
            501,
            { status: "SHIPPING" },
            staff
        ),
        "INVALID_ARGUMENT"
    );

    assert.equal(
        terminalRace.state.shipment.status,
        "CANCELLED"
    );

    console.log(
        "Concurrent status changes rechecked without terminal rollback: OK"
    );

    const missing = fixture({ record: null });

    await expectDomain(
        () => missing.application.getShipment(501, admin),
        "NOT_FOUND"
    );

    const disappeared = fixture({
        onUpdate({ state }) {
            state.shipment = null;
            return null;
        },
    });

    await expectDomain(
        () => disappeared.application.updateShipmentStatus(
            501,
            { status: "SHIPPING" },
            admin
        ),
        "NOT_FOUND"
    );

    const busy = fixture({
        onUpdate() {
            return null;
        },
    });

    await expectDomain(
        () => busy.application.updateShipmentStatus(
            501,
            { status: "SHIPPING" },
            admin
        ),
        "SERVICE_UNAVAILABLE"
    );

    assert.equal(busy.calls.updates.length, 3);
    assert.equal(busy.calls.reads.length, 4);

    const failing = fixture();
    const failure = new Error("Simulated repository failure");

    failing.state.readError = failure;

    await assert.rejects(
        () => failing.application.getShipment(501, admin),
        (error) => error === failure
    );

    console.log(
        "Missing shipments, bounded contention and repository failures: OK"
    );

    const handling = fixture();

    const event = {
        eventId: randomUUID(),
        eventType: "OrderCreated",
        version: 1,
        oid: 1001,
        uid: 13,
        discount: "0.00",
        items: [
            { pid: 1, qty: 1, unit_price: "10.00" },
        ],
        shipid: 999,
        status: "DELIVERED",
    };

    const handled = await handling.application.handleOrderCreated(
        event
    );

    assert.equal(handled.created, true);
    assert.equal(handled.shipment.status, "PENDING");
    assert.equal(
        handled.shipment.sourceEventId,
        event.eventId
    );

    assert.deepEqual(handling.calls.events[0], {
        eventId: event.eventId,
        eventType: "OrderCreated",
        version: 1,
        oid: 1001,
        uid: 13,
        discount: "0.00",
        items: [
            { pid: 1, qty: 1, unit_price: "10.00" },
        ],
    });

    const invalid = structuredClone(event);
    delete invalid.oid;

    await expectDomain(
        () => handling.application.handleOrderCreated(invalid),
        "INVALID_ARGUMENT"
    );

    assert.equal(handling.calls.events.length, 1);

    console.log(
        "OrderCreated application validation and delegation: OK"
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Shipment application check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
}