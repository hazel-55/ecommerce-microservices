import assert from "node:assert/strict";
import { createRequire } from "node:module";
import {
    pool,
} from "../src/infrastructure/database/pool.js";
import {
    gatewayConfig,
} from "../../api-gateway/src/config/env.js";

const gatewayRequire = createRequire(
    new URL("../../api-gateway/package.json", import.meta.url),
);

const jwt = gatewayRequire("jsonwebtoken");
const fixtureOrderIds = [];

const baseUrl =
    `http://127.0.0.1:${gatewayConfig.port}`;

function tokenFor(uid, role, expiresIn = "5m") {
    return jwt.sign(
        { role },
        gatewayConfig.jwtSecret,
        {
            algorithm: "HS256",
            issuer: gatewayConfig.jwtIssuer,
            audience: gatewayConfig.jwtAudience,
            subject: String(uid),
            expiresIn,
        },
    );
}

async function request(path, token, extraHeaders = {}) {
    const headers = { ...extraHeaders };

    if (token !== undefined) {
        headers.authorization = `Bearer ${token}`;
    }

    const response = await fetch(`${baseUrl}${path}`, {
        headers,
        signal: AbortSignal.timeout(15_000),
    });

    assert.ok(
        response.headers.get("content-type")
            ?.includes("application/json"),
    );

    return {
        status: response.status,
        body: await response.json(),
    };
}

async function seedFixtures() {
    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        async function insert(uid, discount, pid, qty, price) {
            const inserted = await client.query(
                `
                INSERT INTO orders (uid, discount)
                VALUES ($1, $2)
                RETURNING oid
                `,
                [uid, discount],
            );

            const oid = inserted.rows[0].oid;
            fixtureOrderIds.push(oid);

            await client.query(
                `
                INSERT INTO order_details (
                    oid,
                    pid,
                    qty,
                    unit_price
                )
                VALUES ($1, $2, $3, $4)
                `,
                [oid, pid, qty, price],
            );

            return oid;
        }

        const ownId = await insert(
            13,
            "2.55",
            1,
            2,
            "25.50",
        );

        const otherId = await insert(
            14,
            "0.00",
            2,
            1,
            "10.00",
        );

        await client.query("COMMIT");

        return { ownId, otherId };
    } catch (error) {
        await client.query("ROLLBACK");
        throw error;
    } finally {
        client.release();
    }
}

async function main() {
    const customerToken = tokenFor(13, "CUSTOMER");
    const adminToken = tokenFor(99, "ADMIN");

    assert.equal(
        (await request("/api/orders")).status,
        401,
    );

    assert.equal(
        (await request("/api/orders/1")).status,
        401,
    );

    assert.equal(
        (
            await request(
                "/api/orders",
                "invalid-token",
            )
        ).status,
        401,
    );

    assert.equal(
        (
            await request(
                "/api/orders",
                tokenFor(13, "CUSTOMER", -1),
            )
        ).status,
        401,
    );

    console.log(
        "Order HTTP routes require valid JWT: OK",
    );

    const { ownId, otherId } = await seedFixtures();

    const spoofedHeaders = {
        "x-user-id": "14",
        "x-user-role": "ADMIN",
        "x-service-name": "api-gateway",
        "x-service-token": "spoofed-token",
    };

    const list = await request(
        "/api/orders?uid=14",
        customerToken,
        spoofedHeaders,
    );

    assert.equal(list.status, 200);
    assert.ok(Array.isArray(list.body.items));

    assert.ok(
        list.body.items.some(
            (item) => item.oid === ownId,
        ),
    );

    assert.equal(
        list.body.items.some(
            (item) => item.oid === otherId,
        ),
        false,
    );

    for (const item of list.body.items) {
        assert.equal(item.uid, 13);
        assert.equal(typeof item.discount, "number");

        assert.deepEqual(
            Object.keys(item).sort(),
            ["discount", "oid", "uid"],
        );
    }

    console.log(
        "HTTP Order list scoped to verified user: OK",
    );

    const detail = await request(
        `/api/orders/${ownId}`,
        customerToken,
    );

    assert.equal(detail.status, 200);

    assert.deepEqual(detail.body, {
        oid: ownId,
        uid: 13,
        discount: 2.55,
        items: [{
            pid: 1,
            qty: 2,
            unit_price: 25.5,
        }],
    });

    console.log(
        "HTTP own Order detail and response fields: OK",
    );

    assert.equal(
        (
            await request(
                `/api/orders/${otherId}`,
                customerToken,
                spoofedHeaders,
            )
        ).status,
        403,
    );

    const adminRead = await request(
        `/api/orders/${otherId}`,
        adminToken,
    );

    assert.equal(adminRead.status, 200);
    assert.equal(adminRead.body.uid, 14);

    console.log(
        "HTTP Order ownership and admin access: OK",
    );

    for (const oid of [
        "0",
        "-1",
        "01",
        "1.5",
        "1e2",
        "2147483648",
        "abc",
    ]) {
        assert.equal(
            (
                await request(
                    `/api/orders/${oid}`,
                    customerToken,
                )
            ).status,
            400,
        );
    }

    assert.equal(
        (
            await request(
                "/api/orders/2147483647",
                customerToken,
            )
        ).status,
        404,
    );

    console.log(
        "HTTP Order ID validation and NOT_FOUND mapping: OK",
    );
}

async function cleanup() {
    if (fixtureOrderIds.length === 0) {
        return;
    }

    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        await client.query(
            `DELETE FROM outbox_events
             WHERE oid = ANY($1::integer[])`,
            [fixtureOrderIds],
        );

        await client.query(
            `DELETE FROM order_details
             WHERE oid = ANY($1::integer[])`,
            [fixtureOrderIds],
        );

        await client.query(
            `DELETE FROM orders
             WHERE oid = ANY($1::integer[])`,
            [fixtureOrderIds],
        );

        const remaining = await client.query(
            `
            SELECT
                (SELECT COUNT(*) FROM orders
                 WHERE oid = ANY($1::integer[]))
                +
                (SELECT COUNT(*) FROM order_details
                 WHERE oid = ANY($1::integer[]))
                +
                (SELECT COUNT(*) FROM outbox_events
                 WHERE oid = ANY($1::integer[]))
                AS count
            `,
            [fixtureOrderIds],
        );

        assert.equal(Number(remaining.rows[0].count), 0);

        await client.query("COMMIT");

        console.log(
            "Scoped Order HTTP fixtures removed: OK",
        );
    } catch (error) {
        await client.query("ROLLBACK");
        throw error;
    } finally {
        client.release();
    }
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Order HTTP check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    try {
        await cleanup();
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Order HTTP cleanup failed:",
            error.code ?? error.name,
            error.message,
        );
    } finally {
        await pool.end();
    }
}