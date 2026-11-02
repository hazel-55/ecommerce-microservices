import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
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

const requireGateway = createRequire(
    new URL("../../api-gateway/package.json", import.meta.url),
);

const jwt = requireGateway("jsonwebtoken");

const source = JSON.parse(
    await readFile(
        new URL("./order-create-live.fixture.json", import.meta.url),
        "utf8",
    ),
);

assert.equal(source.verified, true);
assert.deepEqual(source.actor, { uid: 13, role: "CUSTOMER" });
assert.equal(source.products.length, 2);

const operationId = randomUUID();

const manifestPath = new URL(
    "./order-insufficient-stock.fixture.json",
    import.meta.url,
);

let shipmentConnection;
let ownsManifest = false;
let verified = false;
let token;
let items;

async function post(path, body, key) {
    const headers = { "Content-Type": "application/json" };

    if (token) {
        headers.Authorization = `Bearer ${token}`;
    }

    if (key) {
        headers["Idempotency-Key"] = key;
    }

    const response = await fetch(
        `http://127.0.0.1:${gatewayConfig.port}${path}`,
        {
            method: "POST",
            headers,
            body: JSON.stringify(body),
            signal: AbortSignal.timeout(15_000),
        },
    );

    const result = {
        status: response.status,
        key: response.headers.get("idempotency-key"),
        data: await response.json(),
    };

    if (response.status === 429) {
        throw new Error(
            `Rate limit reached; retry after ${response.headers.get("Retry-After") ?? "60"
            } seconds`,
        );
    }

    return result;
}

async function stockSnapshot() {
    const rows = [];

    for (const product of source.products) {
        const row = await ProductModel.findOne({
            pid: product.pid,
            pname: product.pname,
        }).lean();

        assert.ok(row);

        rows.push({
            pid: row.pid,
            pname: row.pname,
            quantity: row.quantity,
            reservedQuantity: row.reservedQuantity,
        });
    }

    return rows;
}

async function readTask() {
    const result = await pool.query(
        `
        SELECT *
        FROM order_creation_tasks
        WHERE operation_id = $1::uuid
        `,
        [operationId],
    );

    return result.rows[0] ?? null;
}

async function assertNoOrderOrEvent() {
    const result = await pool.query(
        `
        SELECT
            (SELECT count(*)
             FROM orders
             WHERE operation_id = $1::uuid)
            +
            (SELECT count(*)
             FROM outbox_events
             WHERE event_id = $1::uuid)
            AS count
        `,
        [operationId],
    );

    assert.equal(Number(result.rows[0].count), 0);
}

async function cleanupFailedTask() {
    const client = await pool.connect();
    let started = false;
    let releaseError;

    try {
        await client.query("BEGIN");
        started = true;

        await client.query("SET LOCAL lock_timeout = '5s'");
        await client.query("SET LOCAL statement_timeout = '10s'");

        const locked = await client.query(
            `
            SELECT uid, actor_role, status, failure_code, items
            FROM order_creation_tasks
            WHERE operation_id = $1::uuid
            FOR UPDATE
            `,
            [operationId],
        );

        assert.equal(locked.rowCount, 1);

        const task = locked.rows[0];

        assert.equal(task.uid, source.actor.uid);
        assert.equal(task.actor_role, source.actor.role);
        assert.equal(task.status, "FAILED");
        assert.equal(task.failure_code, "CONFLICT");

        assert.deepEqual(
            task.items,
            [...items].sort((a, b) => a.pid - b.pid),
        );

        const removed = await client.query(
            `
            DELETE FROM order_creation_tasks
            WHERE operation_id = $1::uuid
              AND uid = $2
              AND status = 'FAILED'
            `,
            [operationId, source.actor.uid],
        );

        assert.equal(removed.rowCount, 1);

        await client.query("COMMIT");
        started = false;
    } catch (error) {
        if (started) {
            try {
                await client.query("ROLLBACK");
            } catch (rollbackError) {
                releaseError = rollbackError;
            }
        }

        throw error;
    } finally {
        client.release(releaseError);
    }

    const removed = await StockReservationModel.deleteOne({
        _id: operationId,
        uid: source.actor.uid,
        status: "RELEASED",
    });

    assert.equal(removed.deletedCount, 1);
    assert.equal(await readTask(), null);

    assert.equal(
        await StockReservationModel.countDocuments({
            _id: operationId,
        }),
        0,
    );

    await unlink(manifestPath);

    console.log(
        "Scoped failed task, release tombstone and test manifest removed: OK",
    );
}

try {
    await connectDatabase();

    assert.equal(ProductModel.db.name, "product_db");

    const database = await pool.query(
        "SELECT current_database() AS name",
    );

    assert.equal(database.rows[0].name, "order_db");

    const shipmentEnv = dotenv.parse(
        await readFile(
            new URL("../../shipment-service/.env", import.meta.url),
        ),
    );

    shipmentConnection = mongoose.createConnection(
        shipmentEnv.MONGODB_URI,
        {
            serverSelectionTimeoutMS: 5000,
            connectTimeoutMS: 5000,
            maxPoolSize: 2,
        },
    );

    await shipmentConnection.asPromise();
    assert.equal(shipmentConnection.name, "shipment_db");

    const shipments =
        shipmentConnection.db.collection("shipments");

    assert.equal(await readTask(), null);

    assert.equal(
        await StockReservationModel.countDocuments({
            _id: operationId,
        }),
        0,
    );

    const login = await post("/api/auth/login", {
        username: "http_8678dd4139734fe18d85dee359c76237",
        password: "P@ss123",
    });

    assert.equal(login.status, 200);

    const claims = jwt.verify(
        login.data.accessToken,
        gatewayConfig.jwtSecret,
        {
            algorithms: ["HS256"],
            issuer: gatewayConfig.jwtIssuer,
            audience: gatewayConfig.jwtAudience,
        },
    );

    assert.equal(claims.sub, String(source.actor.uid));
    assert.equal(claims.role, "CUSTOMER");

    token = login.data.accessToken;

    const before = await stockSnapshot();

    for (const row of before) {
        assert.equal(row.reservedQuantity, 0);
    }

    assert.ok(before[0].quantity >= 1);
    assert.ok(before[1].quantity < 2147483647);

    items = [
        { pid: before[0].pid, qty: 1 },
        {
            pid: before[1].pid,
            qty: before[1].quantity + 1,
        },
    ];

    await writeFile(
        manifestPath,
        JSON.stringify({
            version: 1,
            operationId,
            sourceOperationId: source.operationId,
            actor: source.actor,
            items,
            before,
        }, null, 2),
        { flag: "wx" },
    );

    ownsManifest = true;

    console.log(
        "Isolated insufficient-stock operation and fixture snapshot: OK",
    );

    const rejected = await post(
        "/api/orders",
        { items },
        operationId,
    );

    assert.equal(rejected.status, 409);
    assert.equal(rejected.key, operationId);

    console.log(
        "TC-ORD-03 insufficient stock returns HTTP 409: OK",
    );

    const deadline = Date.now() + 45_000;
    let task;

    while (Date.now() < deadline) {
        task = await readTask();

        assert.ok(task);

        assert.ok(
            ["PROCESSING", "RELEASE_PENDING", "FAILED"]
                .includes(task.status),
        );

        if (task.status === "FAILED") {
            break;
        }

        await sleep(200);
    }

    assert.equal(task.status, "FAILED");
    assert.equal(task.failure_code, "CONFLICT");
    assert.equal(task.uid, source.actor.uid);

    const reservation = await StockReservationModel
        .findById(operationId)
        .lean();

    assert.ok(reservation);
    assert.equal(reservation.uid, source.actor.uid);
    assert.equal(reservation.status, "RELEASED");
    assert.deepEqual(reservation.items, []);

    console.log(
        "Permanent stock failure completes its durable release decision: OK",
    );

    await assertNoOrderOrEvent();

    assert.equal(
        await shipments.countDocuments({
            sourceEventId: operationId,
        }),
        0,
    );

    assert.deepEqual(await stockSnapshot(), before);

    console.log(
        "No Order, Outbox or Shipment created; both stock quantities unchanged: OK",
    );

    const replay = await post(
        "/api/orders",
        { items },
        operationId,
    );

    assert.equal(replay.status, 409);
    assert.equal(replay.key, operationId);
    assert.deepEqual(await readTask(), task);

    assert.deepEqual(
        await StockReservationModel
            .findById(operationId)
            .lean(),
        reservation,
    );

    assert.deepEqual(await stockSnapshot(), before);
    await assertNoOrderOrEvent();

    assert.equal(
        await shipments.countDocuments({
            sourceEventId: operationId,
        }),
        0,
    );

    verified = true;

    console.log(
        "Failed operation replay preserves failure and stock state: OK",
    );
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Insufficient-stock Docker check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    if (ownsManifest) {
        if (verified) {
            try {
                await cleanupFailedTask();
            } catch (error) {
                process.exitCode = 1;

                console.error(
                    "Scoped failure cleanup failed:",
                    error.code ?? error.name,
                );
            }
        }

        if (process.exitCode) {
            console.error(
                "Preserved failure operation:",
                operationId,
            );

            console.error(
                "Manifest: scripts/order-insufficient-stock.fixture.json",
            );
        }
    }

    const closed = await Promise.allSettled([
        pool.end(),
        disconnectDatabase(),
        shipmentConnection
            ? shipmentConnection.close()
            : Promise.resolve(),
    ]);

    if (closed.some((item) => item.status === "rejected")) {
        process.exitCode = 1;

        console.error(
            "Insufficient-stock check connection cleanup failed",
        );
    } else {
        console.log(
            "Insufficient-stock test connections closed: OK",
        );
    }
}