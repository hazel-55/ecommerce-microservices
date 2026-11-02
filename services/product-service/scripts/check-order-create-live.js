import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import { createRequire } from "node:module";
import { readFile, writeFile } from "node:fs/promises";
import { setTimeout as sleep } from "node:timers/promises";
import mongoose from "mongoose";
import dotenv from "dotenv";

import { pool } from "../../order-service/src/infrastructure/database/pool.js";
import { orderRepository } from "../../order-service/src/infrastructure/repositories/order.repository.js";
import { orderCreationRepository } from "../../order-service/src/infrastructure/repositories/order-creation.repository.js";
import { calculateOrderPricing } from "../../order-service/src/domain/order.pricing.js";
import { gatewayConfig } from "../../api-gateway/src/config/env.js";

import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import { ProductModel } from "../src/infrastructure/database/models/product.model.js";
import { StockReservationModel } from "../src/infrastructure/database/models/stock-reservation.model.js";
import { productRepository } from "../src/infrastructure/repositories/product.repository.js";

const gatewayRequire = createRequire(
    new URL("../../api-gateway/package.json", import.meta.url),
);

const jwt = gatewayRequire("jsonwebtoken");

const actor = { uid: 13, role: "CUSTOMER" };
const operationId = randomUUID();

const manifestPath = new URL(
    "./order-create-live.fixture.json",
    import.meta.url,
);

const fixture = {
    version: 1,
    operationId,
    actor,
    submitted: false,
    verified: false,
    products: [
        {
            pname: `order_http_${operationId}_A`,
            price: "25.50",
            quantity: 10,
            qty: 2,
        },
        {
            pname: `order_http_${operationId}_B`,
            price: "10.00",
            quantity: 6,
            qty: 1,
        },
    ],
};

let shipmentConnection;
let ownsManifest = false;
let stage = "preflight";
let accessToken;

const baseUrl = `http://127.0.0.1:${gatewayConfig.port}`;

async function saveManifest() {
    await writeFile(
        manifestPath,
        JSON.stringify(fixture, null, 2),
    );
}

async function request(path, { input, key, authenticated = true } = {}) {
    const headers = {};

    if (authenticated && accessToken) {
        headers.Authorization = `Bearer ${accessToken}`;
    }

    if (input !== undefined) {
        headers["Content-Type"] = "application/json";
    }

    if (key !== undefined) {
        headers["Idempotency-Key"] = key;
    }

    const response = await fetch(`${baseUrl}${path}`, {
        method: input === undefined ? "GET" : "POST",
        headers,
        body: input === undefined
            ? undefined
            : JSON.stringify(input),
        signal: AbortSignal.timeout(15_000),
    });

    const data = await response.json();

    return {
        status: response.status,
        key: response.headers.get("idempotency-key"),
        data,
    };
}

function requireStatus(response, expected) {
    if (response.status !== expected) {
        const error = new Error("Unexpected HTTP status");

        error.code =
            `HTTP_${response.status}_EXPECTED_${expected}`;

        throw error;
    }
}

async function eventsOf() {
    const result = await pool.query(
        `
        SELECT e.*
        FROM outbox_events e
        JOIN orders o ON o.oid = e.oid
        WHERE o.operation_id = $1::uuid
        ORDER BY e.event_id
        `,
        [operationId],
    );

    return result.rows;
}

async function stockSnapshot() {
    const snapshot = [];

    for (const product of fixture.products) {
        const row = await ProductModel.findOne({
            pid: product.pid,
            pname: product.pname,
        }).lean();

        assert.ok(row);

        assert.equal(
            row.quantity,
            product.quantity - product.qty,
        );

        assert.equal(row.reservedQuantity, 0);

        snapshot.push({
            pid: row.pid,
            quantity: row.quantity,
            reservedQuantity: row.reservedQuantity,
        });
    }

    return snapshot;
}

async function readDockerCustomer() {
    const execute = promisify(execFile);

    const { stdout } = await execute("docker", [
        "compose",
        "-f", "docker-compose.yml",
        "-f", "docker-compose.apps.yml",
        "exec", "-T", "postgres-customer",
        "psql", "-X", "-q", "-t", "-A",
        "-v", "ON_ERROR_STOP=1",
        "-U", "customer_user",
        "-d", "customer_db",
        "-c",
        `SELECT json_build_object('uid', c.uid, 'score', m.score)
         FROM customers c
         JOIN memberships m ON m.mid = c.mid
         WHERE c.uid = ${actor.uid}`,
    ], {
        cwd: fileURLToPath(new URL("../../../", import.meta.url)),
        timeout: 15_000,
        maxBuffer: 256 * 1024,
    });

    return JSON.parse(stdout.trim());
}

async function readReservation() {
    const row = await StockReservationModel
        .findById(operationId)
        .lean();

    assert.ok(row);
    assert.equal(row.uid, actor.uid);

    return {
        operationId: row._id,
        uid: row.uid,
        status: row.status,
        items: row.items.map(({ pid, qty, unit_price }) => ({
            pid,
            qty,
            unit_price,
        })).sort((a, b) => a.pid - b.pid),
    };
}

async function main() {
    // Không ghi đè fixture của lần kiểm tra trước.
    await writeFile(
        manifestPath,
        JSON.stringify(fixture, null, 2),
        { flag: "wx" },
    );

    ownsManifest = true;

    const login = await request("/api/auth/login", {
        input: {
            username: "http_8678dd4139734fe18d85dee359c76237",
            password: "P@ss123",
        },
        authenticated: false,
    });

    requireStatus(login, 200);
    accessToken = login.data.accessToken;

    const claims = jwt.verify(accessToken, gatewayConfig.jwtSecret, {
        algorithms: ["HS256"],
        issuer: gatewayConfig.jwtIssuer,
        audience: gatewayConfig.jwtAudience,
    });

    assert.equal(claims.sub, String(actor.uid));
    assert.equal(claims.role, actor.role);

    const customer = await readDockerCustomer();
    const profile = await request(`/api/customers/${actor.uid}`);

    requireStatus(profile, 200);
    assert.equal(profile.data.uid, actor.uid);

    assert.equal(customer.uid, actor.uid);
    assert.ok(Number.isInteger(customer.score));
    assert.ok(
        customer.score >= 0 &&
        customer.score <= 2147483647,
    );

    fixture.score = customer.score;

    await connectDatabase();

    assert.equal(ProductModel.db.name, "product_db");

    await pool.query(
        "SELECT operation_id FROM order_creation_tasks LIMIT 0",
    );

    // Dùng connection riêng cho Shipment.
    const shipmentEnv = dotenv.parse(
        await readFile(
            new URL(
                "../../shipment-service/.env",
                import.meta.url,
            ),
        ),
    );

    assert.ok(shipmentEnv.MONGODB_URI);

    shipmentConnection = mongoose.createConnection(
        shipmentEnv.MONGODB_URI,
        {
            serverSelectionTimeoutMS: 5000,
            connectTimeoutMS: 5000,
            maxPoolSize: 2,
        },
    );

    await shipmentConnection.asPromise();

    assert.equal(
        shipmentConnection.name,
        "shipment_db",
    );

    await shipmentConnection.db.command(
        { ping: 1 },
        { maxTimeMS: 5000 },
    );

    requireStatus(
        await request("/api/orders"),
        200,
    );

    console.log(
        "Live Gateway, Customer and database preflight: OK",
    );

    stage = "product fixtures";

    for (const product of fixture.products) {
        const created = await productRepository.create({
            pname: product.pname,
            price: product.price,
            quantity: product.quantity,
        });

        product.pid = created.pid;

        await saveManifest();
    }

    const input = {
        items: fixture.products.map(({ pid, qty }) => ({
            pid,
            qty,
        })),
        uid: 999,
        role: "ADMIN",
        score: 2147483647,
        discount: 0,
    };

    const expectedItems = fixture.products
        .map(({ pid, qty, price }) => ({
            pid,
            qty,
            unit_price: price,
        }))
        .sort((a, b) => a.pid - b.pid);

    requireStatus(await request("/api/orders", {
        input,
        key: randomUUID(),
        authenticated: false,
    }), 401);

    console.log("TC-ORD-02 Order creation requires JWT: OK");

    stage = "HTTP POST";

    fixture.submitted = true;
    await saveManifest();

    const created = await request("/api/orders", {
        input,
        key: operationId,
    });

    requireStatus(created, 201);

    assert.equal(created.key, operationId);
    assert.equal(created.data.uid, actor.uid);

    assert.ok(
        Number.isInteger(created.data.oid) &&
        created.data.oid > 0,
    );

    assert.ok(
        Number.isInteger(created.data.shipment?.shipid),
    );

    assert.ok(created.data.shipment.shipid > 0);

    assert.equal(
        created.data.shipment.oid,
        created.data.oid,
    );

    assert.equal(
        created.data.shipment.status,
        "PENDING",
    );

    fixture.oid = created.data.oid;
    fixture.shipid = created.data.shipment.shipid;

    await saveManifest();

    console.log(
        "Live HTTP POST returns Order and concrete Shipment: OK",
    );

    stage = "stored order";

    const task =
        await orderCreationRepository.findByOperationId(
            operationId,
        );

    assert.ok(task);
    assert.equal(task.uid, actor.uid);
    assert.equal(task.actor_role, actor.role);
    assert.equal(task.status, "COMPLETED");
    assert.equal(task.score, fixture.score);

    const pricing = calculateOrderPricing(
        expectedItems,
        task.score,
    );

    const order =
        await orderRepository.findByOperationId(
            operationId,
        );

    assert.ok(order);
    assert.equal(order.oid, fixture.oid);
    assert.equal(order.uid, actor.uid);
    assert.equal(order.discount, pricing.discount);

    const subtotalCents = fixture.products.reduce((sum, item) =>
        sum + BigInt(item.price.replace(".", "")) * BigInt(item.qty),
        0n
    );

    const percentage =
        task.score >= 500 ? 10n : task.score >= 100 ? 5n : 0n;

    const discountCents = (subtotalCents * percentage + 50n) / 100n;

    const expectedDiscount =
        String(discountCents / 100n) + "." +
        String(discountCents % 100n).padStart(2, "0");

    assert.equal(order.discount, expectedDiscount);

    console.log("TC-ORD-04 stored discount matches the score policy: OK");
    assert.deepEqual(order.items, pricing.items);

    const detail = {
        oid: order.oid,
        uid: order.uid,
        discount: Number(order.discount),
        items: order.items.map((item) => ({
            ...item,
            unit_price: Number(item.unit_price),
        })),
    };

    assert.deepEqual(created.data, {
        ...detail,
        shipment: created.data.shipment,
    });

    console.log(
        "Durable task, trusted actor and decimal snapshots: OK",
    );

    stage = "confirmed stock";

    const reservation =
        await readReservation();

    assert.equal(reservation.status, "CONFIRMED");
    assert.deepEqual(
        reservation.items,
        expectedItems,
    );

    const stock = await stockSnapshot();

    assert.equal(
        await StockReservationModel.countDocuments({
            _id: operationId,
        }),
        1,
    );

    console.log(
        "Live stock confirmed and deducted exactly once: OK",
    );

    stage = "published event and shipment";

    let events;

    const until = Date.now() + 15_000;

    do {
        events = await eventsOf();

        if (
            events.length === 1 &&
            events[0].published_at !== null
        ) {
            break;
        }

        await sleep(200);
    } while (Date.now() < until);

    assert.equal(events.length, 1);

    assert.ok(
        events[0].published_at instanceof Date,
    );

    assert.equal(events[0].event_id, operationId);
    assert.equal(
        events[0].event_type,
        "OrderCreated",
    );

    assert.equal(
        events[0].payload.eventId,
        operationId,
    );

    assert.equal(events[0].payload.version, 1);
    assert.equal(events[0].payload.oid, order.oid);
    assert.equal(events[0].payload.uid, actor.uid);

    assert.equal(
        events[0].payload.discount,
        pricing.discount,
    );

    assert.deepEqual(
        events[0].payload.items,
        pricing.items,
    );

    const shipments =
        shipmentConnection.db.collection("shipments");

    const shipment = await shipments.findOne({
        sourceEventId: operationId,
    });

    assert.ok(shipment);
    assert.equal(shipment.uid, actor.uid);

    assert.deepEqual(
        {
            shipid: shipment.shipid,
            oid: shipment.oid,
            status: shipment.status,
        },
        created.data.shipment,
    );

    assert.equal(
        await shipments.countDocuments({
            sourceEventId: operationId,
        }),
        1,
    );

    console.log(
        "Published Outbox event creates one real Shipment: OK",
    );

    stage = "HTTP reads";

    const readOrder = await request(
        `/api/orders/${order.oid}`,
    );

    requireStatus(readOrder, 200);
    assert.deepEqual(readOrder.data, detail);

    const readShipment = await request(
        `/api/shipments/${shipment.shipid}`,
    );

    requireStatus(readShipment, 200);

    assert.deepEqual(
        readShipment.data,
        created.data.shipment,
    );

    console.log(
        "Created Order and Shipment readable through Gateway: OK",
    );

    stage = "concurrent replay";

    const replays = await Promise.all([
        request("/api/orders", {
            input,
            key: operationId,
        }),
        request("/api/orders", {
            input,
            key: operationId,
        }),
    ]);

    for (const replay of replays) {
        requireStatus(replay, 201);
        assert.equal(replay.key, operationId);
        assert.deepEqual(replay.data, created.data);
    }

    console.log(
        "Concurrent HTTP replay returns the same Order and Shipment: OK",
    );

    stage = "operation binding";

    const changed = structuredClone(input);
    changed.items[0].qty += 1;

    requireStatus(
        await request("/api/orders", {
            input: changed,
            key: operationId,
        }),
        409,
    );

    assert.deepEqual(
        await orderRepository.findByOperationId(
            operationId,
        ),
        order,
    );

    assert.deepEqual(await eventsOf(), events);
    assert.deepEqual(await stockSnapshot(), stock);

    assert.deepEqual(
        await readReservation(),
        reservation,
    );

    assert.deepEqual(
        await shipments.findOne({
            sourceEventId: operationId,
        }),
        shipment,
    );

    assert.equal(
        await shipments.countDocuments({
            sourceEventId: operationId,
        }),
        1,
    );

    const count = await pool.query(
        `
        SELECT COUNT(*) AS count
        FROM orders
        WHERE operation_id = $1::uuid
        `,
        [operationId],
    );

    assert.equal(Number(count.rows[0].count), 1);

    console.log(
        "Changed request rejected without extra orders, events or stock deduction: OK",
    );

    fixture.verified = true;
    fixture.verifiedAt = new Date().toISOString();

    await saveManifest();

    console.log(
        "Verified live fixtures preserved for controlled cleanup: OK",
    );
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Live HTTP order check failed:",
        {
            stage,
            code: error.code ?? error.name,
        },
    );

    if (error instanceof ReferenceError) {
        console.error(error.stack);
    }

    if (error.code === "EEXIST") {
        console.error(
            "Existing fixture manifest preserved; do not delete or overwrite it.",
        );
    }
} finally {
    if (ownsManifest) {
        console.log(
            "Fixture operation ID:",
            operationId,
        );

        console.log(
            "Fixture manifest: scripts/order-create-live.fixture.json",
        );
    }


    const closed = await Promise.allSettled([
        pool.end(),
        disconnectDatabase(),
        shipmentConnection
            ? shipmentConnection.close()
            : Promise.resolve(),
    ]);

    if (
        closed.some(
            (result) => result.status === "rejected",
        )
    ) {
        process.exitCode = 1;

        console.error(
            "Live HTTP check connection cleanup failed",
        );
    } else {
        console.log(
            "Live HTTP integration connections closed: OK",
        );
    }
}