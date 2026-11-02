import assert from "node:assert/strict";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";

import { DomainError } from "../src/domain/errors.js";
import {
    shipmentClientConfig,
} from "../src/infrastructure/config/env.js";
import {
    createShipmentClient,
    shipmentClient,
} from "../src/infrastructure/grpc/shipment.client.js";

const definition = loadSync(
    fileURLToPath(
        new URL("../../../proto/shipment.proto", import.meta.url)
    ),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    }
);

const shipmentPackage = grpc.loadPackageDefinition(definition)
    .ecommerce.shipment.v1;

function eventId(value) {
    return "a0000000-0000-4000-8000-" +
        String(value).padStart(12, "0");
}

const errorCodes = new Map([
    [2, grpc.status.NOT_FOUND],
    [3, grpc.status.PERMISSION_DENIED],
    [4, grpc.status.INVALID_ARGUMENT],
    [5, grpc.status.UNAVAILABLE],
    [6, grpc.status.DEADLINE_EXCEEDED],
    [7, grpc.status.ABORTED],
    [8, grpc.status.INTERNAL],
]);

const server = new grpc.Server();
const clients = [];
const serverFailures = [];

const actor = { uid: 13, role: "CUSTOMER" };

let calls = 0;
let bound = false;
let currentStatus = "PENDING";

function handler(call, callback) {
    calls += 1;

    const tokens = call.metadata.get("x-service-token");

    if (
        tokens.length !== 1 ||
        tokens[0] !== shipmentClientConfig.orderToken
    ) {
        callback({
            code: grpc.status.UNAUTHENTICATED,
            details: "private backend details",
        });

        return;
    }

    try {
        assert.deepEqual(
            call.metadata.get("x-service-name"),
            ["order-service"]
        );

        assert.deepEqual(
            call.metadata.get("x-user-id"),
            ["13"]
        );

        assert.deepEqual(
            call.metadata.get("x-user-role"),
            ["CUSTOMER"]
        );

        assert.deepEqual(
            Object.keys(call.request),
            ["sourceEventId"]
        );

        const index = Number(
            call.request.sourceEventId.slice(-12)
        );

        assert.equal(
            call.request.sourceEventId,
            eventId(index)
        );

        const code = errorCodes.get(index);

        if (code !== undefined) {
            callback({
                code,
                details: "private backend details",
            });

            return;
        }

        callback(null, {
            shipid: index === 9
                ? 0
                : index === 12
                    ? 2147483648
                    : 7,
            oid: index === 10 ? 1002 : 1001,
            status: index === 11 ? "UNKNOWN" : currentStatus,
        });
    } catch (error) {
        serverFailures.push(error);

        callback({
            code: grpc.status.INTERNAL,
            details: "private backend details",
        });
    }
}

async function expectDomain(operation, code, message) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);

        if (message !== undefined) {
            assert.equal(error.message, message);
        }

        assert.equal(
            error.message.includes("private backend details"),
            false
        );

        return true;
    });
}

async function main() {
    server.addService(shipmentPackage.ShipmentService.service, {
        GetShipmentBySourceEvent: handler,
    });

    const port = await new Promise((resolve, reject) => {
        server.bindAsync(
            "127.0.0.1:0",
            grpc.ServerCredentials.createInsecure(),
            (error, assignedPort) => {
                if (error) {
                    reject(error);
                    return;
                }

                bound = true;
                resolve(assignedPort);
            }
        );
    });

    const address = `127.0.0.1:${port}`;

    const client = createShipmentClient({ address });
    clients.push(client);

    const invalidClient = createShipmentClient({
        address,
        serviceToken: "invalid-token",
    });

    clients.push(invalidClient);

    for (const sourceEventId of [
        "",
        "invalid",
        eventId(1).toUpperCase(),
        null,
    ]) {
        await expectDomain(
            () => client.getShipmentBySourceEvent(
                sourceEventId,
                1001,
                actor
            ),
            "INVALID_ARGUMENT"
        );
    }

    for (const oid of [0, -1, 1.5, "1001", 2147483648]) {
        await expectDomain(
            () => client.getShipmentBySourceEvent(
                eventId(1),
                oid,
                actor
            ),
            "INVALID_ARGUMENT"
        );
    }

    for (const invalidActor of [
        null,
        { uid: 0, role: "CUSTOMER" },
    ]) {
        await expectDomain(
            () => client.getShipmentBySourceEvent(
                eventId(1),
                1001,
                invalidActor
            ),
            "UNAUTHENTICATED"
        );
    }

    await expectDomain(
        () => client.getShipmentBySourceEvent(
            eventId(1),
            1001,
            { uid: 13, role: "ROOT" }
        ),
        "FORBIDDEN"
    );

    assert.equal(calls, 0);

    console.log("Order Shipment client input validation: OK");

    for (const status of [
        "PENDING",
        "SHIPPING",
        "DELIVERED",
        "CANCELLED",
    ]) {
        currentStatus = status;

        assert.deepEqual(
            await client.getShipmentBySourceEvent(
                eventId(1),
                1001,
                actor
            ),
            {
                shipid: 7,
                oid: 1001,
                status,
            }
        );
    }

    assert.equal(serverFailures.length, 0);

    console.log(
        "Order to Shipment lookup metadata and response: OK"
    );

    for (const [index, code] of [
        [2, "NOT_FOUND"],
        [3, "FORBIDDEN"],
        [4, "INVALID_ARGUMENT"],
        [5, "SERVICE_UNAVAILABLE"],
        [6, "SERVICE_UNAVAILABLE"],
        [7, "CONFLICT"],
        [8, "INTERNAL"],
    ]) {
        await expectDomain(
            () => client.getShipmentBySourceEvent(
                eventId(index),
                1001,
                actor
            ),
            code
        );
    }

    console.log(
        "Shipment lookup permission and service error mapping: OK"
    );

    await expectDomain(
        () => invalidClient.getShipmentBySourceEvent(
            eventId(1),
            1001,
            actor
        ),
        "INTERNAL",
        "Shipment service authentication failed"
    );

    console.log(
        "Shipment lookup credentials mapped to INTERNAL: OK"
    );

    for (const index of [9, 10, 11, 12]) {
        await expectDomain(
            () => client.getShipmentBySourceEvent(
                eventId(index),
                1001,
                actor
            ),
            "INTERNAL",
            "Invalid shipment service response"
        );
    }

    assert.equal(serverFailures.length, 0);

    console.log(
        "Shipment response identity and fields protected: OK"
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Order Shipment client check failed:",
        error.code ?? error.name
    );

    process.exitCode = 1;
} finally {
    for (const client of clients) {
        client.close();
    }

    shipmentClient.close();

    try {
        if (bound) {
            const timer = setTimeout(
                () => server.forceShutdown(),
                5000
            );

            try {
                await new Promise((resolve, reject) => {
                    server.tryShutdown((error) => {
                        if (error) {
                            reject(error);
                            return;
                        }

                        resolve();
                    });
                });
            } finally {
                clearTimeout(timer);
            }
        } else {
            server.forceShutdown();
        }

        console.log(
            "Order Shipment client test connections closed: OK"
        );
    } catch (error) {
        server.forceShutdown();

        console.error(
            "Order Shipment client cleanup failed:",
            error.code ?? error.name
        );

        process.exitCode = 1;
    }
}