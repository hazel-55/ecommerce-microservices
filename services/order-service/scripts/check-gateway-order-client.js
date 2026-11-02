import assert from "node:assert/strict";
import { createRequire } from "node:module";
import {
    pool,
} from "../src/infrastructure/database/pool.js";
import {
    orderClient,
    createOrderClient,
} from "../../api-gateway/src/infrastructure/grpc/order.client.js";
import {
    authenticate,
} from "../../api-gateway/src/middleware/auth.middleware.js";
import {
    gatewayConfig,
} from "../../api-gateway/src/config/env.js";
import {
    HttpError,
} from "../../api-gateway/src/errors/http-error.js";

const gatewayRequire = createRequire(
    new URL("../../api-gateway/package.json", import.meta.url),
);

const jwt = gatewayRequire("jsonwebtoken");

const fixtureOrderIds = [];

const invalidClient = createOrderClient({
    serviceToken: "invalid-order-gateway-token",
});

async function verifiedActor(uid, role) {
    const token = jwt.sign(
        { role },
        gatewayConfig.jwtSecret,
        {
            algorithm: "HS256",
            issuer: gatewayConfig.jwtIssuer,
            audience: gatewayConfig.jwtAudience,
            subject: String(uid),
            expiresIn: "5m",
        },
    );

    const req = {
        headers: {
            authorization: `Bearer ${token}`,
        },
    };

    await new Promise((resolve, reject) => {
        authenticate(req, {}, (error) => {
            if (error) {
                reject(error);
                return;
            }

            resolve();
        });
    });

    assert.deepEqual(req.auth, { uid, role });

    return req.auth;
}

async function expectHttpError(operation, status) {
    await assert.rejects(
        operation,
        (error) =>
            error instanceof HttpError &&
            error.status === status,
    );
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
    const customer = await verifiedActor(13, "CUSTOMER");
    const admin = await verifiedActor(99, "ADMIN");

    console.log("Order actor from verified JWT: OK");

    for (const oid of [
        0,
        -1,
        1.5,
        "1",
        null,
        2147483648,
    ]) {
        await expectHttpError(
            () => orderClient.getOrder(oid, customer),
            400,
        );
    }

    for (const actor of [
        undefined,
        { uid: 0, role: "CUSTOMER" },
        { uid: 13, role: "UNKNOWN" },
    ]) {
        await expectHttpError(
            () => orderClient.listOrders(actor),
            401,
        );
    }

    console.log("Order client input validation: OK");

    const { ownId, otherId } = await seedFixtures();

    const list = await orderClient.listOrders(customer);

    assert.ok(Array.isArray(list.items));
    assert.ok(
        list.items.some((item) => item.oid === ownId),
    );

    for (const item of list.items) {
        assert.equal(item.uid, customer.uid);
        assert.equal(typeof item.discount, "string");
    }

    const detail = await orderClient.getOrder(
        ownId,
        customer,
    );

    assert.deepEqual(detail, {
        oid: ownId,
        uid: 13,
        discount: "2.55",
        items: [{
            pid: 1,
            qty: 2,
            unit_price: "25.50",
        }],
    });

    console.log(
        "Gateway to Order live list and detail: OK",
    );

    await expectHttpError(
        () => orderClient.getOrder(otherId, customer),
        403,
    );

    const adminRead = await orderClient.getOrder(
        otherId,
        admin,
    );

    assert.equal(adminRead.uid, 14);

    await expectHttpError(
        () => orderClient.getOrder(
            2147483647,
            customer,
        ),
        404,
    );

    console.log(
        "Order permission and NOT_FOUND mapping: OK",
    );

    await expectHttpError(
        () => invalidClient.listOrders(customer),
        502,
    );

    console.log(
        "Order service credential error mapping: OK",
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
            "Scoped Order client fixtures removed: OK",
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
        "Gateway Order client check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    orderClient.close();
    invalidClient.close();

    try {
        await cleanup();
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Gateway Order client cleanup failed:",
            error.code ?? error.name,
            error.message,
        );
    } finally {
        await pool.end();
    }
}