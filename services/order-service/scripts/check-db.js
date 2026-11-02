import assert from "node:assert/strict";
import { pool } from "../src/infrastructure/database/pool.js";

async function main() {
    const connection = await pool.query(`
        SELECT
            current_database() AS database_name,
            current_user AS user_name
    `);

    assert.equal(connection.rowCount, 1);

    console.log("Order DB connection: OK");

    assert.equal(
        connection.rows[0].database_name,
        "order_db"
    );

    assert.equal(
        connection.rows[0].user_name,
        "order_user"
    );

    console.log("Order database and user: OK");

    const orders = await pool.query(`
        SELECT oid, uid, discount
        FROM orders
        LIMIT 0
    `);

    assert.deepEqual(
        orders.fields.map((field) => field.name),
        ["oid", "uid", "discount"]
    );

    const details = await pool.query(`
        SELECT oid, pid, qty, unit_price
        FROM order_details
        LIMIT 0
    `);

    assert.deepEqual(
        details.fields.map((field) => field.name),
        ["oid", "pid", "qty", "unit_price"]
    );

    const outbox = await pool.query(`
        SELECT
            event_id,
            event_type,
            oid,
            payload,
            created_at,
            published_at,
            attempts,
            last_error,
            next_attempt_at
        FROM outbox_events
        LIMIT 0
    `);

    assert.deepEqual(
        outbox.fields.map((field) => field.name),
        [
            "event_id",
            "event_type",
            "oid",
            "payload",
            "created_at",
            "published_at",
            "attempts",
            "last_error",
            "next_attempt_at",
        ]
    );

    console.log("Order, detail and outbox tables/columns: OK");

    const index = await pool.query(`
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = 'outbox_events'
          AND indexname = 'outbox_events_pending_idx'
    `);

    assert.equal(index.rowCount, 1);

    const definition = index.rows[0].indexdef;

    assert.match(
        definition,
        /next_attempt_at,\s*created_at/i
    );

    assert.match(
        definition,
        /WHERE\s+\(?published_at IS NULL\)?/i
    );

    console.log("Pending outbox index: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Order database check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    await pool.end();
}