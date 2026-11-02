import assert from "node:assert/strict";
import { readFile, unlink } from "node:fs/promises";
import mongoose from "mongoose";
import dotenv from "dotenv";

import { pool } from "../../order-service/src/infrastructure/database/pool.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import { ProductModel } from "../src/infrastructure/database/models/product.model.js";
import { StockReservationModel } from "../src/infrastructure/database/models/stock-reservation.model.js";

let operationId;
let oid;
let shipid;
let uid;
let products;

const manifestPath = new URL(
    "./order-create-live.fixture.json",
    import.meta.url,
);

let shipmentConnection;
let stage = "manifest";

async function cleanupSql() {
    const client = await pool.connect();

    let started = false;
    let releaseError;

    try {
        await client.query("BEGIN");
        started = true;

        const tasks = await client.query(
            `
            SELECT uid, status
            FROM order_creation_tasks
            WHERE operation_id = $1::uuid
            FOR UPDATE
            `,
            [operationId],
        );

        for (const task of tasks.rows) {
            assert.equal(task.uid, uid);
            assert.equal(task.status, "COMPLETED");
        }

        const orders = await client.query(
            `
            SELECT oid, uid, operation_id
            FROM orders
            WHERE oid = $2
               OR operation_id = $1::uuid
            FOR UPDATE
            `,
            [operationId, oid],
        );

        for (const order of orders.rows) {
            assert.equal(order.oid, oid);
            assert.equal(order.uid, uid);
            assert.equal(order.operation_id, operationId);
        }

        const events = await client.query(
            `
            SELECT event_id, oid, published_at
            FROM outbox_events
            WHERE oid = $2
               OR event_id = $1::uuid
            FOR UPDATE
            `,
            [operationId, oid],
        );

        for (const event of events.rows) {
            assert.equal(event.event_id, operationId);
            assert.equal(event.oid, oid);
            assert.notEqual(event.published_at, null);
        }

        await client.query(
            `
            DELETE FROM outbox_events
            WHERE event_id = $1::uuid
              AND oid = $2
            `,
            [operationId, oid],
        );

        await client.query(
            "DELETE FROM order_details WHERE oid = $1",
            [oid],
        );

        await client.query(
            `
            DELETE FROM orders
            WHERE oid = $2
              AND operation_id = $1::uuid
              AND uid = $3
            `,
            [operationId, oid, uid],
        );

        await client.query(
            `
            DELETE FROM order_creation_tasks
            WHERE operation_id = $1::uuid
              AND uid = $2
            `,
            [operationId, uid],
        );

        const remaining = await client.query(
            `
            SELECT
                (SELECT COUNT(*)
                 FROM order_creation_tasks
                 WHERE operation_id = $1::uuid)
                +
                (SELECT COUNT(*)
                 FROM orders
                 WHERE operation_id = $1::uuid OR oid = $2)
                +
                (SELECT COUNT(*)
                 FROM order_details
                 WHERE oid = $2)
                +
                (SELECT COUNT(*)
                 FROM outbox_events
                 WHERE event_id = $1::uuid OR oid = $2)
                AS count
            `,
            [operationId, oid],
        );

        assert.equal(
            Number(remaining.rows[0].count),
            0,
        );

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
}

async function main() {
    const fixture = JSON.parse(
        await readFile(manifestPath, "utf8"),
    );

    assert.equal(fixture.version, 1);
    assert.equal(fixture.verified, true);
    assert.equal(fixture.submitted, true);

    assert.deepEqual(fixture.actor, {
        uid: 13,
        role: "CUSTOMER",
    });

    assert.equal(typeof fixture.operationId, "string");

    assert.match(
        fixture.operationId,
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
    );

    for (const value of [fixture.oid, fixture.shipid]) {
        assert.ok(
            Number.isInteger(value) &&
            value > 0 &&
            value <= 2147483647
        );
    }

    assert.ok(Array.isArray(fixture.products));
    assert.equal(fixture.products.length, 2);

    for (const [index, product] of fixture.products.entries()) {
        assert.ok(
            Number.isInteger(product.pid) &&
            product.pid > 0 &&
            product.pid <= 2147483647
        );

        assert.equal(
            product.pname,
            `order_http_${fixture.operationId}_${index === 0 ? "A" : "B"}`
        );
    }

    assert.notEqual(
        fixture.products[0].pid,
        fixture.products[1].pid
    );

    operationId = fixture.operationId;
    oid = fixture.oid;
    shipid = fixture.shipid;
    uid = fixture.actor.uid;

    products = fixture.products.map(({ pid, pname }) => ({
        pid,
        pname,
    }));

    console.log(
        "Verified fixture identity and deletion scope: OK",
    );

    stage = "Mongo preflight";

    await connectDatabase();

    assert.equal(ProductModel.db.name, "product_db");

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

    const shipments =
        shipmentConnection.db.collection("shipments");

    const shipmentRows = await shipments.find({
        $or: [
            { sourceEventId: operationId },
            { shipid },
        ],
    }).toArray();

    for (const row of shipmentRows) {
        assert.equal(row.sourceEventId, operationId);
        assert.equal(row.shipid, shipid);
        assert.equal(row.oid, oid);
        assert.equal(row.uid, uid);
    }

    for (const product of products) {
        const rows = await ProductModel.find({
            $or: [
                { pid: product.pid },
                { pname: product.pname },
            ],
        }).lean();

        for (const row of rows) {
            assert.equal(row.pid, product.pid);
            assert.equal(row.pname, product.pname);
            assert.equal(row.reservedQuantity, 0);
        }
    }

    const reservation =
        await StockReservationModel.findById(
            operationId,
        ).lean();

    if (reservation) {
        assert.equal(reservation.uid, uid);
        assert.equal(
            reservation.status,
            "CONFIRMED",
        );
    }

    stage = "SQL cleanup";

    await cleanupSql();

    console.log(
        "Scoped Order, details, Outbox and task removed: OK",
    );

    stage = "Product cleanup";

    await StockReservationModel.deleteOne({
        _id: operationId,
        uid,
    });

    for (const product of products) {
        await ProductModel.deleteOne(product);
    }

    assert.equal(
        await StockReservationModel.countDocuments({
            _id: operationId,
        }),
        0,
    );

    assert.equal(
        await ProductModel.countDocuments({
            $or: products.flatMap(({ pid, pname }) => [
                { pid },
                { pname },
            ]),
        }),
        0,
    );

    console.log(
        "Scoped Product reservation and products removed: OK",
    );

    stage = "Shipment cleanup";

    await shipments.deleteOne({
        sourceEventId: operationId,
        shipid,
        oid,
        uid,
    });

    assert.equal(
        await shipments.countDocuments({
            $or: [
                { sourceEventId: operationId },
                { shipid },
            ],
        }),
        0,
    );

    console.log("Scoped Shipment removed: OK");

    stage = "manifest removal";

    await unlink(manifestPath);

    console.log(
        "Verified fixture manifest removed: OK",
    );
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Live fixture cleanup failed:",
        {
            stage,
            code: error.code ?? error.name,
        },
    );

    if (error instanceof ReferenceError) {
        console.error(error.stack);
    }

    console.error(
        "Fixture operation ID:",
        operationId,
    );
} finally {
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
            "Cleanup connections failed to close",
        );
    } else {
        console.log(
            "Cleanup database connections closed: OK",
        );
    }
}