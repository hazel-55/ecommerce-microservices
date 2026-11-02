import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    createOrderRepository,
} from "../src/infrastructure/repositories/order.repository.js";
import {
    calculateOrderPricing,
} from "../src/domain/order.pricing.js";

const capturedOids = new Set();
const eventId = randomUUID();

function trackingDatabase({
    onOrderInserted = () => { },
    onDetailInserted = () => { },
} = {}) {
    return {
        async connect() {
            const client = await pool.connect();

            return {
                async query(sql, values) {
                    const result = await client.query(sql, values);

                    if (/INSERT INTO orders\s*\(/i.test(sql)) {
                        const oid = result.rows[0].oid;

                        capturedOids.add(oid);
                        onOrderInserted(oid);
                    }

                    if (/INSERT INTO order_details\s*\(/i.test(sql)) {
                        onDetailInserted();
                    }

                    return result;
                },

                release(error) {
                    client.release(error);
                },
            };
        },
    };
}

async function main() {
    const pricing = calculateOrderPricing(
        [
            { pid: 1, qty: 2, unit_price: "25.50" },
            { pid: 2, qty: 1, unit_price: "0.00" },
        ],
        500
    );

    const repository = createOrderRepository({
        database: trackingDatabase(),
    });

    const created = await repository.createOrder({
        uid: 2147483647,
        discount: pricing.discount,
        items: pricing.items,
        eventId,
    });

    const order = await pool.query(
        `
        SELECT oid, uid, discount
        FROM orders
        WHERE oid = $1
        `,
        [created.oid]
    );

    const details = await pool.query(
        `
        SELECT pid, qty, unit_price
        FROM order_details
        WHERE oid = $1
        ORDER BY pid
        `,
        [created.oid]
    );

    const outbox = await pool.query(
        `
        SELECT
            event_id,
            event_type,
            oid,
            payload,
            published_at,
            attempts,
            last_error
        FROM outbox_events
        WHERE event_id = $1
        `,
        [eventId]
    );

    assert.equal(order.rowCount, 1);
    assert.equal(details.rowCount, 2);
    assert.equal(outbox.rowCount, 1);
    assert.equal(outbox.rows[0].oid, created.oid);

    console.log("Atomic Order, OrderDetail and Outbox create: OK");

    assert.deepEqual(order.rows[0], {
        oid: created.oid,
        uid: 2147483647,
        discount: "5.10",
    });

    assert.deepEqual(details.rows, pricing.items);
    assert.deepEqual(created.items, pricing.items);
    assert.equal(created.discount, "5.10");

    console.log("Stored decimal prices and discount snapshots: OK");

    const event = outbox.rows[0];

    assert.equal(event.event_id, eventId);
    assert.equal(event.event_type, "OrderCreated");
    assert.equal(event.published_at, null);
    assert.equal(event.attempts, 0);
    assert.equal(event.last_error, null);

    assert.deepEqual(event.payload, {
        eventId,
        eventType: "OrderCreated",
        version: 1,
        oid: created.oid,
        uid: 2147483647,
        discount: "5.10",
        items: pricing.items,
    });

    console.log("Pending Outbox event and payload: OK");

    let failedOid;
    let insertedDetailCount = 0;

    const failingRepository = createOrderRepository({
        database: trackingDatabase({
            onOrderInserted(oid) {
                failedOid = oid;
            },

            onDetailInserted() {
                insertedDetailCount++;
            },
        }),
    });

    // Dùng eventId đã tồn tại để gây lỗi tại bước ghi Outbox.
    await assert.rejects(
        () => failingRepository.createOrder({
            uid: 2147483647,
            discount: pricing.discount,
            items: pricing.items,
            eventId,
        }),
        (error) => {
            assert.equal(error.code, "23505");
            assert.equal(error.constraint, "outbox_events_pkey");

            return true;
        }
    );

    assert.ok(Number.isInteger(failedOid));
    assert.notEqual(failedOid, created.oid);
    assert.equal(insertedDetailCount, 2);

    const rolledBack = await pool.query(
        `
        SELECT
            (SELECT COUNT(*)::int
             FROM orders WHERE oid = $1) AS orders_count,
            (SELECT COUNT(*)::int
             FROM order_details WHERE oid = $1) AS details_count,
            (SELECT COUNT(*)::int
             FROM outbox_events WHERE oid = $1) AS outbox_count
        `,
        [failedOid]
    );

    assert.deepEqual(rolledBack.rows[0], {
        orders_count: 0,
        details_count: 0,
        outbox_count: 0,
    });

    console.log("Late Outbox failure rolls back order and details: OK");
}

async function cleanup() {
    const oids = [...capturedOids];

    if (oids.length === 0) {
        return;
    }

    const client = await pool.connect();
    let releaseError;

    try {
        await client.query("BEGIN");

        await client.query(
            "DELETE FROM outbox_events WHERE oid = ANY($1::int[])",
            [oids]
        );

        await client.query(
            "DELETE FROM order_details WHERE oid = ANY($1::int[])",
            [oids]
        );

        await client.query(
            "DELETE FROM orders WHERE oid = ANY($1::int[])",
            [oids]
        );

        await client.query("COMMIT");
    } catch (error) {
        try {
            await client.query("ROLLBACK");
        } catch (rollbackError) {
            releaseError = rollbackError;
        }

        throw error;
    } finally {
        client.release(releaseError);
    }

    const remaining = await pool.query(
        `
        SELECT
            (SELECT COUNT(*)::int FROM orders
             WHERE oid = ANY($1::int[])) AS orders_count,
            (SELECT COUNT(*)::int FROM order_details
             WHERE oid = ANY($1::int[])) AS details_count,
            (SELECT COUNT(*)::int FROM outbox_events
             WHERE oid = ANY($1::int[])) AS outbox_count
        `,
        [oids]
    );

    assert.deepEqual(remaining.rows[0], {
        orders_count: 0,
        details_count: 0,
        outbox_count: 0,
    });
}

try {
    await main();
} catch (error) {
    console.error(
        "Order repository check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        await cleanup();

        console.log("Scoped order fixtures removed: OK");
    } catch (error) {
        console.error(
            "Order fixture cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture OIDs:", [...capturedOids]);
        process.exitCode = 1;
    } finally {
        await pool.end();
    }
}