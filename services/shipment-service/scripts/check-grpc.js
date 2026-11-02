import assert from "node:assert/strict";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";

import { DomainError } from "../src/domain/errors.js";
import {
    shipmentTransportConfig,
} from "../src/infrastructure/config/env.js";
import {
    createShipmentApplication,
} from "../src/application/shipment.application.js";
import {
    createGrpcServer,
} from "../src/interfaces/grpc/server.js";

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

const rows = new Map([
    [1, {
        shipid: 1,
        oid: 1001,
        uid: 13,
        status: "PENDING",
    }],
    [2, {
        shipid: 2,
        oid: 1002,
        uid: 14,
        status: "PENDING",
    }],
    [3, {
        shipid: 3,
        oid: 1003,
        uid: 13,
        status: "DELIVERED",
    }],
]);

let reads = 0;
let updates = 0;

const repository = {
    async findById(shipid) {
        reads += 1;

        if (shipid === 90) {
            throw new DomainError(
                "SERVICE_UNAVAILABLE",
                "Shipment temporarily unavailable"
            );
        }

        if (shipid === 91) {
            throw new Error("private-storage-detail");
        }

        if (shipid === 92) {
            throw new DomainError(
                "INTERNAL",
                "private-domain-detail"
            );
        }

        const row = rows.get(shipid);
        return row ? structuredClone(row) : null;
    },

    async updateStatus(shipid, expectedStatus, nextStatus) {
        updates += 1;

        const row = rows.get(shipid);

        if (!row || row.status !== expectedStatus) {
            return null;
        }

        row.status = nextStatus;
        return structuredClone(row);
    },
};

const application = createShipmentApplication({ repository });
const logs = [];

const logger = {
    error(fields, message) {
        logs.push({ fields, message });
    },
};

let server;
let client;
let bound = false;

function actorMetadata({
    uid = "13",
    role = "CUSTOMER",
    service = "api-gateway",
    token = shipmentTransportConfig.gatewayToken,
} = {}) {
    const metadata = new grpc.Metadata();

    metadata.set("x-service-name", service);
    metadata.set("x-service-token", token);
    metadata.set("x-user-id", uid);
    metadata.set("x-user-role", role);

    return metadata;
}

function rpc(method, request, metadata = actorMetadata()) {
    return new Promise((resolve, reject) => {
        client[method](
            request,
            metadata,
            { deadline: new Date(Date.now() + 5000) },
            (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }

                resolve(response);
            }
        );
    });
}

async function expectCode(operation, code, details) {
    await assert.rejects(operation, (error) => {
        assert.equal(error.code, code);

        if (details !== undefined) {
            assert.equal(error.details, details);
        }

        return true;
    });
}

async function main() {
    server = createGrpcServer(logger, { application });

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

    client = new shipmentPackage.ShipmentService(
        `127.0.0.1:${port}`,
        grpc.credentials.createInsecure()
    );

    const duplicated = actorMetadata();
    duplicated.add("x-user-id", "13");

    for (const metadata of [
        new grpc.Metadata(),
        actorMetadata({ service: "order-service" }),
        actorMetadata({ token: "invalid-token" }),
        duplicated,
    ]) {
        await expectCode(
            () => rpc("getShipment", { shipid: 1 }, metadata),
            grpc.status.UNAUTHENTICATED
        );

        await expectCode(
            () => rpc(
                "updateShipmentStatus",
                { shipid: 1, status: "SHIPPING" },
                metadata
            ),
            grpc.status.UNAUTHENTICATED
        );
    }

    assert.equal(reads, 0);
    assert.equal(updates, 0);

    console.log("Shipment caller authentication over gRPC: OK");

    assert.deepEqual(
        await rpc("getShipment", { shipid: 1 }),
        {
            shipid: 1,
            oid: 1001,
            status: "PENDING",
        }
    );

    await expectCode(
        () => rpc("getShipment", { shipid: 2 }),
        grpc.status.PERMISSION_DENIED
    );

    for (const role of ["ADMIN", "SHIPMENT_STAFF"]) {
        assert.deepEqual(
            await rpc(
                "getShipment",
                { shipid: 2 },
                actorMetadata({ role })
            ),
            {
                shipid: 2,
                oid: 1002,
                status: "PENDING",
            }
        );
    }

    console.log(
        "Shipment read response and ownership over gRPC: OK"
    );

    const readsBeforeDeniedUpdate = reads;

    await expectCode(
        () => rpc(
            "updateShipmentStatus",
            { shipid: 1, status: "SHIPPING" }
        ),
        grpc.status.PERMISSION_DENIED
    );

    assert.equal(reads, readsBeforeDeniedUpdate);
    assert.equal(updates, 0);

    const staff = actorMetadata({ role: "SHIPMENT_STAFF" });

    assert.deepEqual(
        await rpc(
            "updateShipmentStatus",
            { shipid: 1, status: "SHIPPING" },
            staff
        ),
        {
            shipid: 1,
            oid: 1001,
            status: "SHIPPING",
        }
    );

    assert.equal(
        (
            await rpc(
                "updateShipmentStatus",
                { shipid: 1, status: "SHIPPING" },
                staff
            )
        ).status,
        "SHIPPING"
    );

    const admin = actorMetadata({ role: "ADMIN" });

    assert.equal(
        (
            await rpc(
                "updateShipmentStatus",
                { shipid: 1, status: "DELIVERED" },
                admin
            )
        ).status,
        "DELIVERED"
    );

    await expectCode(
        () => rpc(
            "updateShipmentStatus",
            { shipid: 1, status: "SHIPPING" },
            staff
        ),
        grpc.status.INVALID_ARGUMENT
    );

    assert.equal(rows.get(1).status, "DELIVERED");

    console.log(
        "Shipment mutation roles and status transitions over gRPC: OK"
    );

    for (const shipid of [0, 2147483648]) {
        await expectCode(
            () => rpc("getShipment", { shipid }),
            grpc.status.INVALID_ARGUMENT
        );

        await expectCode(
            () => rpc(
                "updateShipmentStatus",
                { shipid, status: "SHIPPING" },
                staff
            ),
            grpc.status.INVALID_ARGUMENT
        );
    }

    await expectCode(
        () => rpc(
            "updateShipmentStatus",
            { shipid: 2, status: "UNKNOWN" },
            staff
        ),
        grpc.status.INVALID_ARGUMENT
    );

    await expectCode(
        () => rpc("getShipment", { shipid: 999 }),
        grpc.status.NOT_FOUND
    );

    await expectCode(
        () => rpc(
            "updateShipmentStatus",
            { shipid: 999, status: "SHIPPING" },
            staff
        ),
        grpc.status.NOT_FOUND
    );

    console.log(
        "Shipment validation and NOT_FOUND status mapping: OK"
    );

    await expectCode(
        () => rpc("getShipment", { shipid: 90 }),
        grpc.status.UNAVAILABLE
    );

    for (const shipid of [91, 92]) {
        await expectCode(
            () => rpc("getShipment", { shipid }),
            grpc.status.INTERNAL,
            "Shipment operation failed"
        );
    }

    assert.equal(logs.length, 2);

    const serializedLogs = JSON.stringify(logs);

    for (const privateValue of [
        "private-storage-detail",
        "private-domain-detail",
        shipmentTransportConfig.gatewayToken,
    ]) {
        assert.equal(serializedLogs.includes(privateValue), false);
    }

    console.log(
        "Shipment unavailable and unexpected errors mapped safely: OK"
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Shipment gRPC check failed:",
        error.code ?? error.name
    );

    process.exitCode = 1;
} finally {
    client?.close();

    try {
        if (server && bound) {
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
            server?.forceShutdown();
        }

        rows.clear();

        console.log(
            "Shipment gRPC test server and fixtures removed: OK"
        );
    } catch (error) {
        server?.forceShutdown();

        console.error(
            "Shipment gRPC cleanup failed:",
            error.code ?? error.name
        );

        process.exitCode = 1;
    }
}