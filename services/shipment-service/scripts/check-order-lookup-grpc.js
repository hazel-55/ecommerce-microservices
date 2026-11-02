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

const firstSource =
    "f4d11b6c-6b8d-4ad2-bdb3-02796eff5bb2";

const secondSource =
    "b79f27a2-e290-458c-b03c-6177ed51e618";

const missingSource =
    "a0000000-0000-4000-8000-000000000001";

const unavailableSource =
    "a0000000-0000-4000-8000-000000000002";

const internalSource =
    "a0000000-0000-4000-8000-000000000003";

const privateMessage = "private lookup storage detail";

const rows = [
    {
        shipid: 1,
        oid: 1001,
        uid: 13,
        sourceEventId: firstSource,
        status: "SHIPPING",
    },
    {
        shipid: 2,
        oid: 1002,
        uid: 14,
        sourceEventId: secondSource,
        status: "PENDING",
    },
];

const stats = { lookup: 0, read: 0, update: 0 };
const logs = [];

const repository = {
    async findBySourceEventId(sourceEventId) {
        stats.lookup += 1;

        if (sourceEventId === unavailableSource) {
            throw new DomainError(
                "SERVICE_UNAVAILABLE",
                "Shipment lookup unavailable"
            );
        }

        if (sourceEventId === internalSource) {
            throw new Error(privateMessage);
        }

        const row = rows.find(
            (item) => item.sourceEventId === sourceEventId
        );

        return row ? structuredClone(row) : null;
    },

    async findById(shipid) {
        stats.read += 1;

        const row = rows.find(
            (item) => item.shipid === shipid
        );

        return row ? structuredClone(row) : null;
    },

    async updateStatus(shipid, expectedStatus, nextStatus) {
        stats.update += 1;

        const row = rows.find(
            (item) =>
                item.shipid === shipid &&
                item.status === expectedStatus
        );

        if (!row) {
            return null;
        }

        row.status = nextStatus;
        return structuredClone(row);
    },
};

const application = createShipmentApplication({ repository });

const logger = {
    error(fields, message) {
        logs.push({ fields, message });
    },
};

let server;
let client;
let bound = false;

function credentials({
    service = "order-service",
    token = shipmentTransportConfig.orderToken,
    uid = "13",
    role = "CUSTOMER",
} = {}) {
    const metadata = new grpc.Metadata();

    metadata.set("x-service-name", service);
    metadata.set("x-service-token", token);
    metadata.set("x-user-id", uid);
    metadata.set("x-user-role", role);

    return metadata;
}

function gatewayCredentials(role = "CUSTOMER") {
    return credentials({
        service: "api-gateway",
        token: shipmentTransportConfig.gatewayToken,
        role,
    });
}

function rpc(method, request, metadata = credentials()) {
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

function lookup(sourceEventId, metadata = credentials()) {
    return rpc(
        "getShipmentBySourceEvent",
        { sourceEventId },
        metadata
    );
}

function dto(row) {
    return {
        shipid: row.shipid,
        oid: row.oid,
        status: row.status,
    };
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

    const duplicateUid = credentials();
    duplicateUid.add("x-user-id", "13");

    for (const metadata of [
        new grpc.Metadata(),
        credentials({ token: "invalid-token" }),
        gatewayCredentials("ADMIN"),
        credentials({
            token: shipmentTransportConfig.gatewayToken,
        }),
        credentials({ service: "api-gateway" }),
        credentials({ uid: "0" }),
        credentials({ role: "ROOT" }),
        duplicateUid,
    ]) {
        await expectCode(
            () => lookup(firstSource, metadata),
            grpc.status.UNAUTHENTICATED
        );
    }

    assert.deepEqual(stats, {
        lookup: 0,
        read: 0,
        update: 0,
    });

    console.log(
        "Order lookup caller authentication over gRPC: OK"
    );

    const original = structuredClone(rows);

    assert.deepEqual(
        await lookup(firstSource),
        dto(rows[0])
    );

    await expectCode(
        () => lookup(
            firstSource,
            credentials({ uid: "14" })
        ),
        grpc.status.PERMISSION_DENIED
    );

    assert.deepEqual(
        await lookup(
            secondSource,
            credentials({ uid: "14" })
        ),
        dto(rows[1])
    );

    for (const role of ["ADMIN", "SHIPMENT_STAFF"]) {
        assert.deepEqual(
            await lookup(
                secondSource,
                credentials({ uid: "99", role })
            ),
            dto(rows[1])
        );
    }

    console.log(
        "Order lookup response and ownership over gRPC: OK"
    );

    const orderAdmin = credentials({ role: "ADMIN" });

    await expectCode(
        () => rpc("getShipment", { shipid: 1 }, orderAdmin),
        grpc.status.UNAUTHENTICATED
    );

    await expectCode(
        () => rpc(
            "updateShipmentStatus",
            { shipid: 1, status: "DELIVERED" },
            orderAdmin
        ),
        grpc.status.UNAUTHENTICATED
    );

    assert.equal(stats.read, 0);
    assert.equal(stats.update, 0);
    assert.deepEqual(rows, original);

    console.log(
        "Order caller cannot use Gateway Shipment RPCs: OK"
    );

    const lookupsBeforeInvalid = stats.lookup;

    for (const value of [
        "",
        "invalid",
        firstSource.toUpperCase(),
    ]) {
        await expectCode(
            () => lookup(value),
            grpc.status.INVALID_ARGUMENT
        );
    }

    assert.equal(stats.lookup, lookupsBeforeInvalid);

    await expectCode(
        () => lookup(missingSource),
        grpc.status.NOT_FOUND
    );

    await expectCode(
        () => lookup(unavailableSource),
        grpc.status.UNAVAILABLE
    );

    await expectCode(
        () => lookup(internalSource),
        grpc.status.INTERNAL,
        "Shipment operation failed"
    );

    assert.equal(logs.length, 1);

    const serializedLogs = JSON.stringify(logs);

    assert.equal(
        serializedLogs.includes(privateMessage),
        false
    );

    assert.equal(
        serializedLogs.includes(shipmentTransportConfig.orderToken),
        false
    );

    console.log(
        "Order lookup validation and safe error mapping: OK"
    );

    assert.deepEqual(
        await rpc(
            "getShipment",
            { shipid: 1 },
            gatewayCredentials()
        ),
        dto(rows[0])
    );

    assert.deepEqual(
        await rpc(
            "updateShipmentStatus",
            { shipid: 1, status: "DELIVERED" },
            gatewayCredentials("SHIPMENT_STAFF")
        ),
        {
            shipid: 1,
            oid: 1001,
            status: "DELIVERED",
        }
    );

    assert.deepEqual(
        await lookup(firstSource),
        {
            shipid: 1,
            oid: 1001,
            status: "DELIVERED",
        }
    );

    console.log(
        "Existing Gateway Shipment RPCs remain available: OK"
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Order lookup gRPC check failed:",
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

        rows.length = 0;

        console.log(
            "Order lookup gRPC test server and fixtures removed: OK"
        );
    } catch (error) {
        server?.forceShutdown();

        console.error(
            "Order lookup gRPC cleanup failed:",
            error.code ?? error.name
        );

        process.exitCode = 1;
    }
}