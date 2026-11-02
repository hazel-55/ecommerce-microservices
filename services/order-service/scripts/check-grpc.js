import assert from "node:assert/strict";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
    pool,
} from "../src/infrastructure/database/pool.js";
import {
    orderTransportConfig,
} from "../src/infrastructure/config/env.js";
import {
    createOrderRepository,
} from "../src/infrastructure/repositories/order.repository.js";
import {
    createOrderApplication,
} from "../src/application/order.application.js";
import {
    createGrpcServer,
} from "../src/interfaces/grpc/server.js";
import {
    customerClient,
} from "../src/infrastructure/grpc/customer.client.js";
import {
    productClient,
} from "../src/infrastructure/grpc/product.client.js";
import {
    shipmentClient,
} from "../src/infrastructure/grpc/shipment.client.js";

const protoPath = fileURLToPath(
    new URL("../../../proto/order.proto", import.meta.url),
);

const definition = loadSync(protoPath, {
    includeDirs: [dirname(protoPath)],
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.order.v1;

const PRIVATE_ERROR = "TEST_PRIVATE_DATABASE_DETAILS";
const logs = [];

let databaseClient;
let transactionStarted = false;
let server;
let rpcClient;

function gatewayMetadata(uid = 13, role = "CUSTOMER") {
    const metadata = new grpc.Metadata();

    metadata.set("x-service-name", "api-gateway");
    metadata.set(
        "x-service-token",
        orderTransportConfig.gatewayToken,
    );
    metadata.set("x-user-id", String(uid));
    metadata.set("x-user-role", role);

    return metadata;
}

function invoke(
    method,
    request,
    metadata = gatewayMetadata(),
) {
    return new Promise((resolve, reject) => {
        rpcClient[method](
            request,
            metadata,
            { deadline: new Date(Date.now() + 5000) },
            (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }

                resolve(response);
            },
        );
    });
}

async function expectStatus(operation, code) {
    await assert.rejects(
        operation,
        (error) => error.code === code,
    );
}

async function seedOrder(uid, discount, pid, price) {
    const inserted = await databaseClient.query(
        `
        INSERT INTO orders (uid, discount)
        VALUES ($1, $2)
        RETURNING oid
        `,
        [uid, discount],
    );

    const oid = inserted.rows[0].oid;

    await databaseClient.query(
        `
        INSERT INTO order_details (
            oid,
            pid,
            qty,
            unit_price
        )
        VALUES ($1, $2, 2, $3)
        `,
        [oid, pid, price],
    );

    return oid;
}

async function main() {
    databaseClient = await pool.connect();

    await databaseClient.query("BEGIN");
    transactionStarted = true;

    await databaseClient.query(
        `
        CREATE TEMP TABLE orders (
            LIKE public.orders INCLUDING ALL
        )
        ON COMMIT DROP
        `,
    );

    await databaseClient.query(
        `
        CREATE TEMP TABLE order_details (
            LIKE public.order_details INCLUDING ALL
        )
        ON COMMIT DROP
        `,
    );

    const firstId = await seedOrder(
        13,
        "2.55",
        1,
        "25.50",
    );

    const otherId = await seedOrder(
        14,
        "0.00",
        2,
        "10.00",
    );

    const application = createOrderApplication({
        repository: createOrderRepository({
            database: databaseClient,
        }),
    });

    server = createGrpcServer(
        {
            error(data, message) {
                logs.push({ data, message });
            },
        },
        {
            application: {
                listOrders: application.listOrders,

                async getOrder(oid, actor) {
                    if (oid === 2147483646) {
                        throw new Error(PRIVATE_ERROR);
                    }

                    return application.getOrder(
                        oid,
                        actor,
                    );
                },
            },
        },
    );

    const port = await new Promise((resolve, reject) => {
        server.bindAsync(
            "127.0.0.1:0",
            grpc.ServerCredentials.createInsecure(),
            (error, boundPort) => {
                if (error) {
                    reject(error);
                    return;
                }

                resolve(boundPort);
            },
        );
    });

    rpcClient = new proto.OrderService(
        `127.0.0.1:${port}`,
        grpc.credentials.createInsecure(),
    );

    await new Promise((resolve, reject) => {
        rpcClient.waitForReady(
            new Date(Date.now() + 5000),
            (error) => {
                if (error) {
                    reject(error);
                    return;
                }

                resolve();
            },
        );
    });

    await expectStatus(
        () => invoke(
            "listOrders",
            {},
            new grpc.Metadata(),
        ),
        grpc.status.UNAUTHENTICATED,
    );

    const wrongToken = gatewayMetadata();
    wrongToken.set("x-service-token", "invalid-token");

    await expectStatus(
        () => invoke(
            "getOrder",
            { oid: firstId },
            wrongToken,
        ),
        grpc.status.UNAUTHENTICATED,
    );

    const duplicateUid = gatewayMetadata();
    duplicateUid.add("x-user-id", "13");

    await expectStatus(
        () => invoke(
            "listOrders",
            {},
            duplicateUid,
        ),
        grpc.status.UNAUTHENTICATED,
    );

    console.log(
        "Order caller authentication over gRPC: OK",
    );

    assert.deepEqual(
        await invoke("listOrders", {}),
        {
            items: [{
                oid: firstId,
                uid: 13,
                discount: "2.55",
            }],
        },
    );

    assert.deepEqual(
        await invoke("getOrder", { oid: firstId }),
        {
            oid: firstId,
            uid: 13,
            discount: "2.55",
            items: [{
                pid: 1,
                qty: 2,
                unitPrice: "25.50",
            }],
        },
    );

    console.log(
        "Order list and detail response mapping over gRPC: OK",
    );

    await expectStatus(
        () => invoke("getOrder", { oid: otherId }),
        grpc.status.PERMISSION_DENIED,
    );

    const adminOrder = await invoke(
        "getOrder",
        { oid: otherId },
        gatewayMetadata(99, "ADMIN"),
    );

    assert.equal(adminOrder.uid, 14);

    for (const oid of [0, 2147483648]) {
        await expectStatus(
            () => invoke("getOrder", { oid }),
            grpc.status.INVALID_ARGUMENT,
        );
    }

    await expectStatus(
        () => invoke(
            "getOrder",
            { oid: 2147483647 },
        ),
        grpc.status.NOT_FOUND,
    );

    console.log(
        "Order ownership and validation status mapping: OK",
    );

    await assert.rejects(
        () => invoke(
            "getOrder",
            { oid: 2147483646 },
        ),
        (error) =>
            error.code === grpc.status.INTERNAL &&
            error.details === "Order operation failed",
    );

    assert.ok(logs.length >= 1);

    assert.equal(
        JSON.stringify(logs).includes(PRIVATE_ERROR),
        false,
    );

    console.log(
        "Unexpected errors return safe INTERNAL responses: OK",
    );

    await expectStatus(
        () => invoke(
            "createOrder",
            { items: [{ pid: 1, qty: 1 }] },
        ),
        grpc.status.INVALID_ARGUMENT,
    );

    await expectStatus(
        () => invoke(
            "createOrder",
            { items: [{ pid: 1, qty: 1 }] },
            new grpc.Metadata(),
        ),
        grpc.status.UNAUTHENTICATED,
    );

    console.log(
        "CreateOrder requires operation identity and authenticated Gateway: OK",
    );
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Order gRPC check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    try {
        rpcClient?.close();
        server?.forceShutdown();
        customerClient.close();
        productClient.close();
        shipmentClient.close();

        if (databaseClient && transactionStarted) {
            await databaseClient.query("ROLLBACK");
            transactionStarted = false;

            const remaining = await databaseClient.query(
                `
                SELECT
                    to_regclass('pg_temp.orders') AS orders,
                    to_regclass('pg_temp.order_details') AS details
                `,
            );

            assert.equal(remaining.rows[0].orders, null);
            assert.equal(remaining.rows[0].details, null);

            console.log(
                "Order gRPC test server and fixtures removed: OK",
            );
        }
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Order gRPC cleanup failed:",
            error.code ?? error.name,
            error.message,
        );
    } finally {
        databaseClient?.release();
        await pool.end();
    }
}