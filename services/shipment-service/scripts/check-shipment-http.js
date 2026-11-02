import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import dotenv from "dotenv";
import grpc from "@grpc/grpc-js";
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
    shipmentServerConfig,
    shipmentTransportConfig,
} from "../src/infrastructure/config/env.js";
import {
    createGrpcServer,
} from "../src/interfaces/grpc/server.js";

const gatewayRequire = createRequire(
    new URL("../../api-gateway/package.json", import.meta.url)
);

const jwt = gatewayRequire("jsonwebtoken");

const suffix = randomUUID().replaceAll("-", "");
const shipmentCollection = `shipments_http_check_${suffix}`;
const counterCollection = `counter_http_check_${suffix}`;

let base;
let jwtSecret;
let connection;
let ShipmentFixture;
let CounterFixture;
let server;
let bound = false;

let shipmentCollectionCreated = false;
let counterCollectionCreated = false;
let cleanupFailed = false;

function token(uid, role, expiresIn = 300) {
    return jwt.sign(
        { role },
        jwtSecret,
        {
            algorithm: "HS256",
            subject: String(uid),
            issuer: "ecommerce-auth",
            audience: "ecommerce-api",
            expiresIn,
        }
    );
}

async function expectHttp(
    method,
    path,
    expectedStatus,
    { accessToken, body, headers = {} } = {}
) {
    const requestHeaders = { ...headers };

    if (accessToken) {
        requestHeaders.authorization = `Bearer ${accessToken}`;
    }

    if (body !== undefined) {
        requestHeaders["content-type"] = "application/json";
    }

    const response = await fetch(base + path, {
        method,
        headers: requestHeaders,
        body: body === undefined
            ? undefined
            : JSON.stringify(body),
        signal: AbortSignal.timeout(15_000),
    });

    const result = await response.json();

    assert.equal(
        response.status,
        expectedStatus,
        `${method} ${path}`
    );

    return result;
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

function expected(shipment, status) {
    return {
        shipid: shipment.shipid,
        oid: shipment.oid,
        status,
    };
}

function pathOf(shipid) {
    return `/api/shipments/${shipid}`;
}

function stored(shipid) {
    return ShipmentFixture.findOne({ shipid }).lean();
}

function reportFixtures() {
    console.error("HTTP fixture collections:", {
        shipmentCollection,
        counterCollection,
    });
}

async function main() {
    const gatewayEnv = dotenv.parse(
        await readFile(
            new URL("../../api-gateway/.env", import.meta.url),
            "utf8"
        )
    );

    assert.ok(
        gatewayEnv.SHIPMENT_GATEWAY_TOKEN ===
        shipmentTransportConfig.gatewayToken
    );

    assert.equal(
        gatewayEnv.SHIPMENT_GRPC_ADDRESS,
        shipmentServerConfig.grpcAddress
    );

    jwtSecret = gatewayEnv.JWT_SECRET;

    assert.equal(typeof jwtSecret, "string");
    assert.ok(jwtSecret.length >= 32);

    const port = Number(gatewayEnv.GATEWAY_PORT);

    assert.ok(Number.isInteger(port));
    assert.ok(port > 0 && port <= 65535);

    base = `http://127.0.0.1:${port}`;

    // Confirm Gateway is reachable before creating fixtures.
    await expectHttp("GET", "/api/shipments/1", 401);

    connection = await connectDatabase();
    assert.equal(connection.name, "shipment_db");

    ShipmentFixture = connection.model(
        `ShipmentHttp_${suffix}`,
        ShipmentModel.schema.clone(),
        shipmentCollection
    );

    CounterFixture = connection.model(
        `ShipmentHttpCounter_${suffix}`,
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
        await application.handleOrderCreated(createEvent(1001, 13));

    const { shipment: second } =
        await application.handleOrderCreated(createEvent(1002, 14));

    server = createGrpcServer(pino(), { application });

    await new Promise((resolve, reject) => {
        server.bindAsync(
            shipmentServerConfig.grpcAddress,
            grpc.ServerCredentials.createInsecure(),
            (error) => {
                if (error) {
                    reject(error);
                    return;
                }

                bound = true;
                resolve();
            }
        );
    });

    console.log("Isolated HTTP Shipment fixtures ready: OK");

    const customer13 = token(13, "CUSTOMER");
    const customer14 = token(14, "CUSTOMER");
    const staff = token(13, "SHIPMENT_STAFF");
    const admin = token(13, "ADMIN");

    await expectHttp(
        "PATCH",
        pathOf(first.shipid) + "/status",
        401,
        { body: { status: "SHIPPING" } }
    );

    await expectHttp(
        "GET",
        pathOf(first.shipid),
        401,
        { accessToken: "invalid-token" }
    );

    await expectHttp(
        "GET",
        pathOf(first.shipid),
        401,
        { accessToken: token(13, "CUSTOMER", -1) }
    );

    console.log("HTTP Shipment JWT validation: OK");

    const spoofedHeaders = {
        "x-user-id": "14",
        "x-user-role": "ADMIN",
        "x-service-name": "order-service",
        "x-service-token": "invalid-token",
    };

    assert.deepEqual(
        await expectHttp(
            "GET",
            pathOf(first.shipid),
            200,
            {
                accessToken: customer13,
                headers: spoofedHeaders,
            }
        ),
        expected(first, "PENDING")
    );

    await expectHttp(
        "GET",
        pathOf(second.shipid),
        403,
        {
            accessToken: customer13,
            headers: spoofedHeaders,
        }
    );

    for (const accessToken of [customer14, staff, admin]) {
        assert.deepEqual(
            await expectHttp(
                "GET",
                pathOf(second.shipid),
                200,
                { accessToken }
            ),
            expected(second, "PENDING")
        );
    }

    console.log("HTTP Shipment ownership and actor binding: OK");

    for (const invalidId of [
        "0",
        "-1",
        "01",
        "1.5",
        "1e2",
        "2147483648",
        "abc",
    ]) {
        await expectHttp(
            "GET",
            pathOf(invalidId),
            400,
            { accessToken: customer13 }
        );

        await expectHttp(
            "PATCH",
            pathOf(invalidId) + "/status",
            400,
            {
                accessToken: staff,
                body: { status: "SHIPPING" },
            }
        );
    }

    for (const body of [
        {},
        null,
        [],
        { status: "shipping" },
        { status: "UNKNOWN" },
        { status: 42 },
    ]) {
        await expectHttp(
            "PATCH",
            pathOf(first.shipid) + "/status",
            400,
            { accessToken: staff, body }
        );
    }

    console.log("HTTP Shipment ID and status validation: OK");

    const beforeDenied = [
        await stored(first.shipid),
        await stored(second.shipid),
    ];

    for (const shipment of [first, second]) {
        await expectHttp(
            "PATCH",
            pathOf(shipment.shipid) + "/status",
            403,
            {
                accessToken: customer13,
                headers: spoofedHeaders,
                body: {
                    status: "SHIPPING",
                    role: "ADMIN",
                    uid: 14,
                },
            }
        );
    }

    assert.deepEqual(
        [
            await stored(first.shipid),
            await stored(second.shipid),
        ],
        beforeDenied
    );

    console.log("Customer HTTP Shipment mutations forbidden: OK");

    assert.deepEqual(
        await expectHttp(
            "PATCH",
            pathOf(first.shipid) + "/status",
            200,
            {
                accessToken: staff,
                body: {
                    status: "SHIPPING",
                    shipid: second.shipid,
                    oid: 999,
                    uid: 14,
                    sourceEventId: randomUUID(),
                },
            }
        ),
        expected(first, "SHIPPING")
    );

    const shipping = await stored(first.shipid);

    for (const field of [
        "shipid",
        "oid",
        "uid",
        "sourceEventId",
    ]) {
        assert.equal(shipping[field], beforeDenied[0][field]);
    }

    assert.equal(shipping.status, "SHIPPING");

    await expectHttp(
        "PATCH",
        pathOf(first.shipid) + "/status",
        200,
        {
            accessToken: staff,
            body: { status: "SHIPPING" },
        }
    );

    assert.deepEqual(await stored(first.shipid), shipping);

    assert.deepEqual(
        await expectHttp(
            "PATCH",
            pathOf(first.shipid) + "/status",
            200,
            {
                accessToken: admin,
                body: { status: "DELIVERED" },
            }
        ),
        expected(first, "DELIVERED")
    );

    const delivered = await stored(first.shipid);

    await expectHttp(
        "PATCH",
        pathOf(first.shipid) + "/status",
        400,
        {
            accessToken: staff,
            body: { status: "SHIPPING" },
        }
    );

    assert.deepEqual(await stored(first.shipid), delivered);

    assert.deepEqual(
        await expectHttp(
            "PATCH",
            pathOf(second.shipid) + "/status",
            200,
            {
                accessToken: staff,
                body: { status: "CANCELLED" },
            }
        ),
        expected(second, "CANCELLED")
    );

    const cancelled = await stored(second.shipid);

    await expectHttp(
        "PATCH",
        pathOf(second.shipid) + "/status",
        400,
        {
            accessToken: staff,
            body: { status: "SHIPPING" },
        }
    );

    assert.deepEqual(await stored(second.shipid), cancelled);

    assert.deepEqual(
        await expectHttp(
            "GET",
            pathOf(first.shipid),
            200,
            { accessToken: customer13 }
        ),
        expected(first, "DELIVERED")
    );

    assert.deepEqual(
        await expectHttp(
            "GET",
            pathOf(second.shipid),
            200,
            { accessToken: customer14 }
        ),
        expected(second, "CANCELLED")
    );

    console.log(
        "Staff/admin HTTP updates and terminal states: OK"
    );

    await expectHttp(
        "GET",
        pathOf(2147483647),
        404,
        { accessToken: customer13 }
    );

    await expectHttp(
        "PATCH",
        pathOf(2147483647) + "/status",
        404,
        {
            accessToken: staff,
            body: { status: "SHIPPING" },
        }
    );

    assert.equal(await ShipmentFixture.countDocuments(), 2);

    const counter = await CounterFixture
        .findById("shipment")
        .lean();

    assert.ok(counter);
    assert.equal(counter.seq, 2);

    console.log("HTTP Shipment NOT_FOUND mapping: OK");
}

async function closeServer() {
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
        "Shipment HTTP check failed:",
        error.code ?? error.name
    );

    reportFixtures();
    process.exitCode = 1;
} finally {
    try {
        await closeServer();
    } catch (error) {
        server?.forceShutdown();

        console.error(
            "HTTP fixture server shutdown failed:",
            error.code ?? error.name
        );

        reportFixtures();
        process.exit(1);
    }

    if (shipmentCollectionCreated) {
        await cleanupStep(
            "Shipment fixture cleanup",
            () => dropFixture(ShipmentFixture)
        );
    }

    if (counterCollectionCreated) {
        await cleanupStep(
            "Counter fixture cleanup",
            () => dropFixture(CounterFixture)
        );
    }

    if (connection?.readyState === 1) {
        await cleanupStep("Cleanup verification", async () => {
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
        console.log("Scoped HTTP Shipment fixtures removed: OK");
    }

    try {
        await disconnectDatabase();

        console.log(
            "HTTP Shipment integration connections closed: OK"
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