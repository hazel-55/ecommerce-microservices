import assert from "node:assert/strict";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    createOrderRepository,
} from "../src/infrastructure/repositories/order.repository.js";
import {
    calculateOrderPricing,
} from "../src/domain/order.pricing.js";

const capturedOids = new Set();

const fixtureDatabase = {
    query(sql, values) {
        return pool.query(sql, values);
    },

    async connect() {
        const client = await pool.connect();

        return {
            async query(sql, values) {
                const result = await client.query(sql, values);

                if (/INSERT INTO orders\s*\(/i.test(sql)) {
                    capturedOids.add(result.rows[0].oid);
                }

                return result;
            },

            release(error) {
                client.release(error);
            },
        };
    },
};

const repository = createOrderRepository({
    database: fixtureDatabase,
});

async function createFixture(uid, items, score) {
    const pricing = calculateOrderPricing(items, score);

    return repository.createOrder({
        uid,
        discount: pricing.discount,
        items: pricing.items,
    });
}

async function removeFixtures(oids) {
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
}

async function main() {
    const uidA = 2147483647;
    const uidB = 2147483646;

    const first = await createFixture(
        uidA,
        [
            { pid: 2, qty: 1, unit_price: "25.00" },
            { pid: 1, qty: 2, unit_price: "10.00" },
        ],
        100
    );

    const second = await createFixture(
        uidA,
        [{ pid: 3, qty: 1, unit_price: "100.00" }],
        500
    );

    const other = await createFixture(
        uidB,
        [{ pid: 4, qty: 1, unit_price: "15.00" }],
        0
    );

    const list = await repository.listByUid(uidA);

    assert.ok(list.every((order) => order.uid === uidA));
    assert.ok(!list.some((order) => order.oid === other.oid));

    const ownFixtureIds = new Set([
        first.oid,
        second.oid,
    ]);

    const ownFixtures = list.filter(
        (order) => ownFixtureIds.has(order.oid)
    );

    assert.deepEqual(
        ownFixtures.map((order) => order.oid),
        [second.oid, first.oid]
    );

    console.log("Order list scoped to customer UID: OK");

    for (const order of ownFixtures) {
        assert.deepEqual(
            Object.keys(order).sort(),
            ["oid", "uid", "discount"].sort()
        );

        assert.equal(typeof order.discount, "string");
    }

    assert.deepEqual(ownFixtures, [
        {
            oid: second.oid,
            uid: uidA,
            discount: "10.00",
        },
        {
            oid: first.oid,
            uid: uidA,
            discount: "2.25",
        },
    ]);

    console.log("Order summary fields and decimal discount: OK");

    const detail = await repository.findById(first.oid);

    assert.deepEqual(detail, {
        oid: first.oid,
        uid: uidA,
        discount: "2.25",
        items: [
            { pid: 1, qty: 2, unit_price: "10.00" },
            { pid: 2, qty: 1, unit_price: "25.00" },
        ],
    });

    const otherDetail = await repository.findById(other.oid);

    assert.equal(otherDetail.uid, uidB);
    assert.equal(otherDetail.discount, "0.00");

    console.log("Order detail fields and price snapshots: OK");

    await removeFixtures([other.oid]);

    assert.equal(
        await repository.findById(other.oid),
        null
    );

    console.log("Missing order returns null: OK");
}

async function cleanup() {
    const oids = [...capturedOids];

    await removeFixtures(oids);

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
        "Order read repository check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        await cleanup();

        console.log("Scoped read fixtures removed: OK");
    } catch (error) {
        console.error(
            "Order read fixture cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture OIDs:", [...capturedOids]);
        process.exitCode = 1;
    } finally {
        await pool.end();
    }
}