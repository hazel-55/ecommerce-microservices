import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import jwt from "jsonwebtoken";

import { gatewayConfig } from "../src/config/env.js";
import { HttpError } from "../src/errors/http-error.js";
import {
    authenticate,
} from "../src/middleware/auth.middleware.js";
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

const codes = new Map([
    [2, grpc.status.PERMISSION_DENIED],
    [3, grpc.status.NOT_FOUND],
    [4, grpc.status.INVALID_ARGUMENT],
    [5, grpc.status.UNAVAILABLE],
    [6, grpc.status.DEADLINE_EXCEEDED],
    [7, grpc.status.ABORTED],
    [8, grpc.status.INTERNAL],
]);

const server = new grpc.Server();
const clients = [];
const serverFailures = [];

let bound = false;
let calls = 0;

function handler(method) {
    return (call, callback) => {
        calls += 1;

        const tokens = call.metadata.get("x-service-token");

        if (
            tokens.length !== 1 ||
            tokens[0] !== gatewayConfig.shipmentGatewayToken
        ) {
            callback({
                code: grpc.status.UNAUTHENTICATED,
                details: "private-upstream-detail",
            });

            return;
        }

        try {
            assert.deepEqual(
                call.metadata.get("x-service-name"),
                ["api-gateway"]
            );

            assert.deepEqual(
                call.metadata.get("x-user-id"),
                ["13"]
            );

            const roles = call.metadata.get("x-user-role");

            assert.equal(roles.length, 1);
            assert.ok(
                ["CUSTOMER", "ADMIN", "SHIPMENT_STAFF"]
                    .includes(roles[0])
            );

            const shipid = call.request.shipid;
            const code = codes.get(shipid);

            if (code !== undefined) {
                callback({
                    code,
                    details: "private-upstream-detail",
                });

                return;
            }

            callback(null, {
                shipid: shipid === 10 ? 1 : shipid,
                oid: shipid === 9 ? 0 : 1001,
                status: shipid === 11
                    ? "UNKNOWN"
                    : method === "update"
                        ? call.request.status
                        : "PENDING",
            });
        } catch (error) {
            serverFailures.push(error);

            callback({
                code: grpc.status.INTERNAL,
                details: "private-upstream-detail",
            });
        }
    };
}

async function verifiedActor(role) {
    const token = jwt.sign(
        { role },
        gatewayConfig.jwtSecret,
        {
            algorithm: "HS256",
            subject: "13",
            issuer: gatewayConfig.jwtIssuer,
            audience: gatewayConfig.jwtAudience,
            expiresIn: 300,
        }
    );

    const req = {
        headers: {
            authorization: `Bearer ${token}`,
        },
    };

    return new Promise((resolve, reject) => {
        authenticate(req, {}, (error) => {
            if (error) {
                reject(error);
                return;
            }

            resolve(req.auth);
        });
    });
}

async function expectHttp(operation, status) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof HttpError);
        assert.equal(error.status, status);

        assert.equal(
            error.message.includes("private-upstream-detail"),
            false
        );

        return true;
    });
}

async function main() {
    server.addService(shipmentPackage.ShipmentService.service, {
        GetShipment: handler("get"),
        UpdateShipmentStatus: handler("update"),
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

    const actor = await verifiedActor("CUSTOMER");
    const staff = await verifiedActor("SHIPMENT_STAFF");

    assert.deepEqual(actor, { uid: 13, role: "CUSTOMER" });
    assert.deepEqual(staff, { uid: 13, role: "SHIPMENT_STAFF" });

    console.log("Shipment actor from verified JWT: OK");

    for (const shipid of ["1", 0, -1, 1.5, 2147483648]) {
        await expectHttp(
            () => client.getShipment(shipid, actor),
            400
        );

        await expectHttp(
            () => client.updateShipmentStatus(
                shipid,
                { status: "SHIPPING" },
                staff
            ),
            400
        );
    }

    for (const input of [
        null,
        [],
        {},
        { status: "shipping" },
        { status: 42 },
    ]) {
        await expectHttp(
            () => client.updateShipmentStatus(1, input, staff),
            400
        );
    }

    for (const invalidActor of [
        null,
        { uid: 0, role: "CUSTOMER" },
        { uid: 13, role: "ROOT" },
    ]) {
        await expectHttp(
            () => client.getShipment(1, invalidActor),
            401
        );

        await expectHttp(
            () => client.updateShipmentStatus(
                1,
                { status: "SHIPPING" },
                invalidActor
            ),
            401
        );
    }

    assert.equal(calls, 0);

    console.log("Shipment client input validation: OK");

    assert.deepEqual(
        await client.getShipment(1, actor),
        { shipid: 1, oid: 1001, status: "PENDING" }
    );

    const input = {
        status: "SHIPPING",
        uid: 999,
    };

    assert.deepEqual(
        await client.updateShipmentStatus(1, input, staff),
        { shipid: 1, oid: 1001, status: "SHIPPING" }
    );

    assert.deepEqual(input, {
        status: "SHIPPING",
        uid: 999,
    });

    assert.equal(serverFailures.length, 0);

    console.log(
        "Gateway Shipment RPC metadata and response fields: OK"
    );

    for (const [shipid, status] of [
        [2, 403],
        [3, 404],
        [4, 400],
        [5, 503],
        [6, 503],
        [7, 409],
        [8, 500],
    ]) {
        await expectHttp(
            () => client.getShipment(shipid, actor),
            status
        );
    }

    await expectHttp(
        () => invalidClient.getShipment(1, actor),
        502
    );

    console.log(
        "Shipment RPC errors and credentials mapped to HTTP: OK"
    );

    for (const shipid of [9, 10, 11]) {
        await expectHttp(
            () => client.getShipment(shipid, actor),
            502
        );
    }

    assert.equal(serverFailures.length, 0);

    console.log(
        "Invalid Shipment service responses rejected: OK"
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Shipment client check failed:",
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
            "Shipment client test connections closed: OK"
        );
    } catch (error) {
        server.forceShutdown();

        console.error(
            "Shipment client cleanup failed:",
            error.code ?? error.name
        );

        process.exitCode = 1;
    }
}