import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { readFile, writeFile, unlink } from "node:fs/promises";
import { setTimeout as sleep } from "node:timers/promises";
import mongoose from "mongoose";
import dotenv from "dotenv";

import { pool } from "../../order-service/src/infrastructure/database/pool.js";
import { gatewayConfig } from "../../api-gateway/src/config/env.js";

import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";

import { ProductModel } from "../src/infrastructure/database/models/product.model.js";
import { StockReservationModel } from "../src/infrastructure/database/models/stock-reservation.model.js";

const mode = process.argv[2];

assert.ok(
    ["capture", "verify"].includes(mode),
    "Use capture or verify",
);

const fixture = JSON.parse(
    await readFile(
        new URL("./order-create-live.fixture.json", import.meta.url),
        "utf8",
    ),
);

assert.equal(fixture.verified, true);
assert.deepEqual(fixture.actor, { uid: 13, role: "CUSTOMER" });

const snapshotPath = new URL(
    "./order-persistence.snapshot.fixture.json",
    import.meta.url,
);

const root = fileURLToPath(new URL("../../../", import.meta.url));
const execute = promisify(execFile);

const jwt = createRequire(
    new URL("../../api-gateway/package.json", import.meta.url),
)("jsonwebtoken");

let shipmentConnection;
let token;

const normalize = (value) => JSON.parse(JSON.stringify(value));

async function docker(args) {
    const { stdout } = await execute(
        "docker",
        [
            "compose",
            "-f", "docker-compose.yml",
            "-f", "docker-compose.apps.yml",
            ...args,
        ],
        {
            cwd: root,
            timeout: 20_000,
            maxBuffer: 1024 * 1024,
        },
    );

    return stdout.trim();
}

async function http(path, body, key) {
    const headers = {};

    if (token) headers.Authorization = `Bearer ${token}`;
    if (body !== undefined) headers["Content-Type"] = "application/json";
    if (key) headers["Idempotency-Key"] = key;

    const response = await fetch(
        `http://127.0.0.1:${gatewayConfig.port}${path}`,
        {
            method: body === undefined ? "GET" : "POST",
            headers,
            body: body === undefined ? undefined : JSON.stringify(body),
            signal: AbortSignal.timeout(15_000),
        },
    );

    const data = await response.json();

    assert.equal(
        response.status,
        key ? 201 : 200,
        `Unexpected HTTP status ${response.status} at ${path}`,
    );

    if (key) {
        assert.equal(response.headers.get("idempotency-key"), key);
    }

    return data;
}

async function kafkaOffsets() {
    const deadline = Date.now() + 30_000;

    while (Date.now() < deadline) {
        const output = await docker([
            "exec", "-T", "kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "localhost:9092",
            "--describe",
            "--group", "shipment-service-v1",
        ]);

        const rows = output.split(/\r?\n/)
            .map((line) => line.trim().split(/\s+/))
            .filter((fields) =>
                fields[0] === "shipment-service-v1" &&
                fields[1] === "order-created"
            );

        if (
            rows.length > 0 &&
            rows.every((fields) =>
                fields[5] === "0" &&
                fields[6] &&
                fields[6] !== "-"
            )
        ) {
            return rows.map((fields) => ({
                partition: Number(fields[2]),
                committed: fields[3],
                end: fields[4],
                lag: fields[5],
            })).sort((a, b) => a.partition - b.partition);
        }

        await sleep(500);
    }

    throw new Error(
        "Shipment consumer did not become active with zero lag",
    );
}

async function snapshot() {
    const taskResult = await pool.query(
        "SELECT * FROM order_creation_tasks WHERE operation_id = $1::uuid",
        [fixture.operationId],
    );

    const orderResult = await pool.query(
        "SELECT * FROM orders WHERE operation_id = $1::uuid",
        [fixture.operationId],
    );

    const details = await pool.query(
        "SELECT pid, qty, unit_price FROM order_details WHERE oid = $1 ORDER BY pid",
        [fixture.oid],
    );

    const events = await pool.query(
        "SELECT * FROM outbox_events WHERE event_id = $1::uuid",
        [fixture.operationId],
    );

    assert.equal(taskResult.rowCount, 1);
    assert.equal(taskResult.rows[0].status, "COMPLETED");
    assert.equal(taskResult.rows[0].uid, fixture.actor.uid);
    assert.equal(orderResult.rowCount, 1);
    assert.equal(orderResult.rows[0].oid, fixture.oid);
    assert.equal(details.rowCount, fixture.products.length);
    assert.equal(events.rowCount, 1);
    assert.equal(events.rows[0].oid, fixture.oid);
    assert.notEqual(events.rows[0].published_at, null);

    const products = [];

    for (const item of fixture.products) {
        const row = await ProductModel.findOne({
            pid: item.pid,
            pname: item.pname,
        }).lean();

        assert.ok(row);
        assert.equal(row.quantity, item.quantity - item.qty);
        assert.equal(row.reservedQuantity, 0);

        products.push(row);
    }

    const reservation = await StockReservationModel
        .findById(fixture.operationId)
        .lean();

    assert.ok(reservation);
    assert.equal(reservation.status, "CONFIRMED");
    assert.equal(reservation.uid, fixture.actor.uid);

    const collection = shipmentConnection.db.collection("shipments");

    const shipment = await collection.findOne({
        sourceEventId: fixture.operationId,
    });

    assert.ok(shipment);
    assert.equal(shipment.shipid, fixture.shipid);
    assert.equal(shipment.oid, fixture.oid);
    assert.equal(shipment.uid, fixture.actor.uid);
    assert.equal(shipment.status, "SHIPPING");

    assert.equal(
        await collection.countDocuments({
            sourceEventId: fixture.operationId,
        }),
        1,
    );

    const customer = JSON.parse(
        await docker([
            "exec", "-T", "postgres-customer",
            "psql", "-X", "-q", "-t", "-A",
            "-v", "ON_ERROR_STOP=1",
            "-U", "customer_user",
            "-d", "customer_db",
            "-c",
            `SELECT row_to_json(p)
             FROM (
                 SELECT c.uid, c.fullname, c.mid, m.mname, m.score
                 FROM customers c
                 JOIN memberships m ON m.mid = c.mid
                 WHERE c.uid = 13
             ) p`,
        ]),
    );

    assert.equal(customer.uid, fixture.actor.uid);

    return normalize({
        task: taskResult.rows[0],
        order: orderResult.rows[0],
        details: details.rows,
        event: events.rows[0],
        products,
        reservation,
        shipment,
        customer,
        kafka: await kafkaOffsets(),
    });
}

try {
    const saved = mode === "verify"
        ? JSON.parse(await readFile(snapshotPath, "utf8"))
        : null;

    if (saved) {
        assert.equal(saved.version, 1);
        assert.equal(saved.operationId, fixture.operationId);
        assert.equal(saved.oid, fixture.oid);
        assert.equal(saved.shipid, fixture.shipid);
    }

    await connectDatabase();
    assert.equal(ProductModel.db.name, "product_db");

    const database = await pool.query(
        "SELECT current_database() AS name",
    );

    assert.equal(database.rows[0].name, "order_db");

    const env = dotenv.parse(
        await readFile(
            new URL("../../shipment-service/.env", import.meta.url),
        ),
    );

    shipmentConnection = mongoose.createConnection(
        env.MONGODB_URI,
        {
            serverSelectionTimeoutMS: 5000,
            connectTimeoutMS: 5000,
            maxPoolSize: 2,
        },
    );

    await shipmentConnection.asPromise();
    assert.equal(shipmentConnection.name, "shipment_db");

    const login = await http("/api/auth/login", {
        username: "http_8678dd4139734fe18d85dee359c76237",
        password: "P@ss123",
    });

    const claims = jwt.verify(
        login.accessToken,
        gatewayConfig.jwtSecret,
        {
            algorithms: ["HS256"],
            issuer: gatewayConfig.jwtIssuer,
            audience: gatewayConfig.jwtAudience,
        },
    );

    assert.equal(claims.sub, String(fixture.actor.uid));
    assert.equal(claims.role, fixture.actor.role);

    token = login.accessToken;

    assert.equal((await http("/api/health")).status, "UP");

    const order = await http(`/api/orders/${fixture.oid}`);
    const shipment = await http(`/api/shipments/${fixture.shipid}`);

    assert.equal(order.oid, fixture.oid);
    assert.equal(order.uid, fixture.actor.uid);

    assert.deepEqual(shipment, {
        shipid: fixture.shipid,
        oid: fixture.oid,
        status: "SHIPPING",
    });

    const replay = await http(
        "/api/orders",
        {
            items: fixture.products.map(({ pid, qty }) => ({
                pid,
                qty,
            })),
        },
        fixture.operationId,
    );

    assert.deepEqual(replay, { ...order, shipment });

    console.log(
        "Real Auth login, Gateway reads and same-operation replay: OK",
    );

    const state = await snapshot();

    if (mode === "capture") {
        await writeFile(
            snapshotPath,
            JSON.stringify({
                version: 1,
                operationId: fixture.operationId,
                oid: fixture.oid,
                shipid: fixture.shipid,
                state,
            }, null, 2),
            { flag: "wx" },
        );

        console.log(
            "SQL, Product, Shipment, Customer and Kafka snapshot saved: OK",
        );
    } else {
        assert.deepEqual(state, saved.state);

        console.log(
            "SQL and MongoDB fixture data survived container recreation unchanged: OK",
        );

        console.log(
            "Stock remains confirmed once and Shipment remains SHIPPING: OK",
        );

        console.log(
            "Kafka committed offsets persisted; active consumer has zero lag: OK",
        );

        await unlink(snapshotPath);

        console.log(
            "Persistence snapshot removed; main fixture preserved: OK",
        );
    }
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Docker persistence check failed:",
        error.code ?? error.name,
        error.message,
    );

    console.error("Fixture operation:", fixture.operationId);
} finally {
    const results = await Promise.allSettled([
        pool.end(),
        disconnectDatabase(),
        shipmentConnection
            ? shipmentConnection.close()
            : Promise.resolve(),
    ]);

    if (results.some((item) => item.status === "rejected")) {
        process.exitCode = 1;
        console.error("Persistence check connection cleanup failed");
    } else {
        console.log("Persistence check connections closed: OK");
    }
}