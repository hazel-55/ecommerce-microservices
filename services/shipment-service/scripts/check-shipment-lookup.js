import assert from "node:assert/strict";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

import { DomainError } from "../src/domain/errors.js";
import {
    createShipmentApplication,
} from "../src/application/shipment.application.js";

function loadProto(filename) {
    const path = fileURLToPath(
        new URL(
            "../../../proto/" + filename,
            import.meta.url
        )
    );

    return grpc.loadPackageDefinition(
        loadSync(path, {
            includeDirs: [dirname(path)],
            keepCase: false,
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true,
        })
    );
}

function method(service, name) {
    const found = Object.values(service.service).find(
        (item) => item.path?.endsWith("/" + name)
    );

    assert.ok(found, `Missing RPC: ${name}`);
    return found;
}

function requestRoundTrip(rpc, value) {
    return rpc.requestDeserialize(
        rpc.requestSerialize(value)
    );
}

function responseRoundTrip(rpc, value) {
    return rpc.responseDeserialize(
        rpc.responseSerialize(value)
    );
}

async function expectDomain(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    const shipmentProto = loadProto("shipment.proto")
        .ecommerce.shipment.v1;

    const orderProto = loadProto("order.proto")
        .ecommerce.order.v1;

    const get = method(
        shipmentProto.ShipmentService,
        "GetShipment"
    );

    const update = method(
        shipmentProto.ShipmentService,
        "UpdateShipmentStatus"
    );

    assert.deepEqual(
        requestRoundTrip(get, { shipid: 7 }),
        { shipid: 7 }
    );

    assert.deepEqual(
        requestRoundTrip(update, {
            shipid: 7,
            status: "SHIPPING",
        }),
        {
            shipid: 7,
            status: "SHIPPING",
        }
    );

    method(orderProto.OrderService, "ListOrders");
    method(orderProto.OrderService, "GetOrder");

    const createOrder = method(
        orderProto.OrderService,
        "CreateOrder"
    );

    const orderResponse = {
        oid: 42,
        uid: 13,
        discount: "0.00",
        items: [
            {
                pid: 1,
                qty: 2,
                unitPrice: "25.50",
            },
        ],
        shipment: {
            shipid: 7,
            oid: 42,
            status: "PENDING",
        },
    };

    assert.deepEqual(
        responseRoundTrip(createOrder, orderResponse),
        orderResponse
    );

    console.log(
        "Existing Shipment and Order protobuf contracts: OK"
    );

    const sourceEventId =
        "f4d11b6c-6b8d-4ad2-bdb3-02796eff5bb2";

    const lookup = method(
        shipmentProto.ShipmentService,
        "GetShipmentBySourceEvent"
    );

    assert.deepEqual(
        requestRoundTrip(lookup, { sourceEventId }),
        { sourceEventId }
    );

    const projected = {
        shipid: 7,
        oid: 42,
        status: "SHIPPING",
    };

    assert.deepEqual(
        responseRoundTrip(lookup, {
            ...projected,
            uid: 13,
            sourceEventId,
        }),
        projected
    );

    console.log(
        "Internal Shipment lookup request and response fields: OK"
    );

    const row = {
        ...projected,
        uid: 13,
        sourceEventId,
    };

    const original = structuredClone(row);
    const calls = [];
    let repositoryFailure;

    const repository = {
        async findBySourceEventId(value) {
            calls.push(value);

            if (repositoryFailure) {
                throw repositoryFailure;
            }

            return value === sourceEventId
                ? structuredClone(row)
                : null;
        },

        async findById() {
            throw new Error("Lookup must use source event ID");
        },

        async updateStatus() {
            throw new Error("Lookup must not update status");
        },
    };

    const application = createShipmentApplication({
        repository,
    });

    const owner = { uid: 13, role: "CUSTOMER" };

    assert.deepEqual(
        await application.getShipmentBySourceEvent(
            sourceEventId,
            owner
        ),
        projected
    );

    await expectDomain(
        () => application.getShipmentBySourceEvent(
            sourceEventId,
            { uid: 14, role: "CUSTOMER" }
        ),
        "FORBIDDEN"
    );

    for (const role of ["ADMIN", "SHIPMENT_STAFF"]) {
        assert.deepEqual(
            await application.getShipmentBySourceEvent(
                sourceEventId,
                { uid: 99, role }
            ),
            projected
        );
    }

    assert.ok(calls.every((value) => value === sourceEventId));
    assert.deepEqual(row, original);

    console.log(
        "Shipment event lookup ownership and response projection: OK"
    );

    const callsBeforeInvalidInput = calls.length;

    for (const [actor, code] of [
        [null, "UNAUTHENTICATED"],
        [{ uid: 0, role: "CUSTOMER" }, "UNAUTHENTICATED"],
        [{ uid: 13, role: "ROOT" }, "FORBIDDEN"],
    ]) {
        await expectDomain(
            () => application.getShipmentBySourceEvent(
                sourceEventId,
                actor
            ),
            code
        );
    }

    for (const invalidId of [
        "",
        "invalid",
        sourceEventId.toUpperCase(),
        sourceEventId + " ",
        null,
        13,
        {},
    ]) {
        await expectDomain(
            () => application.getShipmentBySourceEvent(
                invalidId,
                owner
            ),
            "INVALID_ARGUMENT"
        );
    }

    assert.equal(calls.length, callsBeforeInvalidInput);

    console.log(
        "Invalid lookup actors and event IDs rejected before repository: OK"
    );

    await expectDomain(
        () => application.getShipmentBySourceEvent(
            "b79f27a2-e290-458c-b03c-6177ed51e618",
            owner
        ),
        "NOT_FOUND"
    );

    repositoryFailure = new Error("Repository unavailable");

    await assert.rejects(
        () => application.getShipmentBySourceEvent(
            sourceEventId,
            owner
        ),
        (error) => error === repositoryFailure
    );

    assert.deepEqual(row, original);

    console.log(
        "Missing Shipment and repository failures preserved: OK"
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Shipment lookup check failed:",
        error.code ?? error.name
    );

    process.exitCode = 1;
}