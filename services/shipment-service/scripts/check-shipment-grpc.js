import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import pino from "pino";

import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import {
    ShipmentModel,
} from "../src/infrastructure/database/models/shipment.model.js";
import {
    CounterModel,
} from "../src/infrastructure/database/models/counter.model.js";
import {
    createShipmentIdAllocator,
} from "../src/infrastructure/database/shipment-id.js";
import {
    createShipmentRepository,
} from "../src/infrastructure/repositories/shipment.repository.js";
import {
    createShipmentApplication,
} from "../src/application/shipment.application.js";
import {
    shipmentTransportConfig,
} from "../src/infrastructure/config/env.js";
import {
    createGrpcServer,
} from "../src/interfaces/grpc/server.js";

const suffix = randomUUID().replaceAll("-", "");

const shipmentCollection = `shipment_grpc_check_${suffix}`;
const counterCollection = `counter_grpc_check_${suffix}`;

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

let connection;
let ShipmentFixture;
let CounterFixture;
let server;
let client;
let bound = false;

let shipmentCollectionCreated = false;
let counterCollectionCreated = false;

function metadata({
    uid = "13",
    role = "CUSTOMER",
} = {}) {
    const result = new grpc.Metadata();

    result.set("x-service-name", "api-gateway");
    result.set(
        "x-service-token",
        shipmentTransportConfig.gatewayToken
    );
    result.set("x-user-id", uid);
    result.set("x-user-role", role);

    return result;
}

function rpc(method, request, actor = metadata()) {
    return new Promise((resolve, reject) => {
        client[method](
            request,
            actor,
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

function update(shipid, status, role = "SHIPMENT_STAFF") {
    return rpc(
        "updateShipmentStatus",
        { shipid, status },
        metadata({ role })
    );
}

async function expectCode(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.equal(error.code, code);
        return true;
    });
}

function createEvent(oid, uid) {
    return {
        eventId: randomUUID(),
        eventType: "OrderCreated",
        version: 1,
        oid,
        uid,
        discount: "0.00",
        items: [
            {
                pid: 1,
                qty: 1,
                unit_price: "25.50",
            },
        ],
    };
}

function response(shipment, status) {
    return {
        shipid: shipment.shipid,
        oid: shipment.oid,
        status,
    };
}

async function stored(shipid) {
    return ShipmentFixture.findOne({ shipid }).lean();
}

function reportFixtures() {
    console.error("Fixture collections:", {
        shipmentCollection,
        counterCollection,
    });
}

async function main() {
    connection = await connectDatabase();
    assert.equal(connection.name, "shipment_db");

    ShipmentFixture = connection.model(
        `ShipmentGrpc_${suffix}`,
        ShipmentModel.schema.clone(),
        shipmentCollection
    );

    CounterFixture = connection.model(
        `ShipmentGrpcCounter_${suffix}`,
        CounterModel.schema.clone(),
        counterCollection
    );

    await ShipmentFixture.createCollection();
    shipmentCollectionCreated = true;

    await CounterFixture.createCollection();
    counterCollectionCreated = true;

    await ShipmentFixture.createIndexes();
    await CounterFixture.createIndexes();

    const repository = createShipmentRepository({
        model: ShipmentFixture,
        allocateId: createShipmentIdAllocator({
            model: CounterFixture,
        }),
    });

    const application = createShipmentApplication({
        repository,
    });

    const { shipment: first } =
        await application.handleOrderCreated(
            createEvent(1001, 13)
        );

    const { shipment: second } =
        await application.handleOrderCreated(
            createEvent(1002, 14)
        );

    server = createGrpcServer(pino(), { application });

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

    console.log(
        "Isolated MongoDB and gRPC fixtures ready: OK"
    );

    assert.deepEqual(
        await rpc("getShipment", { shipid: first.shipid }),
        response(first, "PENDING")
    );

    await expectCode(
        () => rpc("getShipment", { shipid: second.shipid }),
        grpc.status.PERMISSION_DENIED
    );

    assert.deepEqual(
        await rpc(
            "getShipment",
            { shipid: second.shipid },
            metadata({ uid: "14" })
        ),
        response(second, "PENDING")
    );

    for (const role of ["ADMIN", "SHIPMENT_STAFF"]) {
        assert.deepEqual(
            await rpc(
                "getShipment",
                { shipid: second.shipid },
                metadata({ role })
            ),
            response(second, "PENDING")
        );
    }

    console.log("Live gRPC Shipment reads and ownership: OK");

    const beforeDeniedUpdate = await stored(first.shipid);

    await expectCode(
        () => update(first.shipid, "SHIPPING", "CUSTOMER"),
        grpc.status.PERMISSION_DENIED
    );

    assert.deepEqual(
        await stored(first.shipid),
        beforeDeniedUpdate
    );

    assert.deepEqual(
        await update(first.shipid, "SHIPPING"),
        response(first, "SHIPPING")
    );

    const shipping = await stored(first.shipid);

    assert.equal(shipping.status, "SHIPPING");
    assert.equal(shipping.uid, beforeDeniedUpdate.uid);
    assert.equal(shipping.oid, beforeDeniedUpdate.oid);
    assert.equal(
        shipping.sourceEventId,
        beforeDeniedUpdate.sourceEventId
    );

    assert.deepEqual(
        await update(first.shipid, "SHIPPING"),
        response(first, "SHIPPING")
    );

    // Repeating the same status must not change timestamps.
    assert.deepEqual(await stored(first.shipid), shipping);

    console.log(
        "Live gRPC status updates and repeated request behavior: OK"
    );

    assert.deepEqual(
        await update(first.shipid, "DELIVERED", "ADMIN"),
        response(first, "DELIVERED")
    );

    const delivered = await stored(first.shipid);

    await expectCode(
        () => update(first.shipid, "CANCELLED"),
        grpc.status.INVALID_ARGUMENT
    );

    assert.deepEqual(await stored(first.shipid), delivered);

    assert.deepEqual(
        await update(second.shipid, "CANCELLED"),
        response(second, "CANCELLED")
    );

    const cancelled = await stored(second.shipid);

    await expectCode(
        () => update(second.shipid, "SHIPPING"),
        grpc.status.INVALID_ARGUMENT
    );

    assert.deepEqual(await stored(second.shipid), cancelled);

    await expectCode(
        () => rpc("getShipment", { shipid: 2147483647 }),
        grpc.status.NOT_FOUND
    );

    await expectCode(
        () => update(2147483647, "SHIPPING"),
        grpc.status.NOT_FOUND
    );

    assert.equal(await ShipmentFixture.countDocuments(), 2);

    const counter = await CounterFixture
        .findById("shipment")
        .lean();

    assert.ok(counter);
    assert.equal(counter.seq, 2);

    console.log(
        "Live gRPC terminal states and missing Shipment mapping: OK"
    );
}

async function closeTransport() {
    client?.close();

    if (!server) {
        return;
    }

    if (!bound) {
        server.forceShutdown();
        return;
    }

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
}

async function dropFixture(model) {
    try {
        await model.collection.drop();
    } catch (error) {
        if (error.code !== 26) {
            throw error;
        }
    }
}

let cleanupFailed = false;

async function cleanupStep(label, operation) {
    try {
        await operation();
    } catch (error) {
        cleanupFailed = true;
        process.exitCode = 1;

        console.error(
            `${label} failed:`,
            error.code ?? error.name
        );
    }
}

try {
    await main();
} catch (error) {
    console.error(
        "Shipment live gRPC check failed:",
        error.code ?? error.name
    );

    reportFixtures();
    process.exitCode = 1;
} finally {
    // Stop RPC processing before removing its MongoDB fixtures.
    try {
        await closeTransport();
    } catch (error) {
        server?.forceShutdown();

        console.error(
            "Shipment test server shutdown failed:",
            error.code ?? error.name
        );

        reportFixtures();
        process.exit(1);
    }

    if (shipmentCollectionCreated) {
        await cleanupStep(
            "Shipment collection cleanup",
            () => dropFixture(ShipmentFixture)
        );
    }

    if (counterCollectionCreated) {
        await cleanupStep(
            "Counter collection cleanup",
            () => dropFixture(CounterFixture)
        );
    }

    if (connection?.readyState === 1) {
        await cleanupStep("Fixture cleanup verification", async () => {
            const remaining = await connection.db
                .listCollections({
                    name: {
                        $in: [
                            shipmentCollection,
                            counterCollection,
                        ],
                    },
                })
                .toArray();

            assert.equal(remaining.length, 0);
        });
    }

    if (!cleanupFailed) {
        console.log("Scoped MongoDB fixtures removed: OK");
    }

    try {
        await disconnectDatabase();

        console.log(
            "Shipment gRPC integration connections closed: OK"
        );
    } catch (error) {
        cleanupFailed = true;
        process.exitCode = 1;

        console.error(
            "MongoDB disconnect failed:",
            error.code ?? error.name
        );
    }

    if (cleanupFailed) {
        reportFixtures();
    }
}