import assert from "node:assert/strict";
import { randomUUID, randomBytes } from "node:crypto";
import jwt from "jsonwebtoken";
import { pool } from "../src/infrastructure/database/pool.js";
import { hashPassword } from "../src/infrastructure/security/password.js";
import { jwtConfig } from "../src/infrastructure/config/env.js";

const baseUrl = "http://127.0.0.1:3001";
const fixtureId = randomUUID();

const username = `product_admin_${fixtureId}`;
const email = `product_admin_${fixtureId}@example.test`;
const password = `Test!Aa1${randomBytes(20).toString("hex")}`;

const originalName = `http_product_${fixtureId}`;
const updatedName = `http_product_updated_${fixtureId}`;
const fixtureNames = new Set([originalName, updatedName]);

let adminUid;
let token;

async function request(path, {
    method = "GET",
    body,
    authenticated = true,
} = {}) {
    const headers = {};

    if (authenticated && token) {
        headers.Authorization = `Bearer ${token}`;
    }

    if (body !== undefined) {
        headers["Content-Type"] = "application/json";
    }

    const response = await fetch(`${baseUrl}${path}`, {
        method,
        headers,
        body: body === undefined
            ? undefined
            : JSON.stringify(body),
        signal: AbortSignal.timeout(15000),
    });

    return {
        status: response.status,
        data: await response.json(),
    };
}

function assertProduct(product) {
    assert.deepEqual(
        Object.keys(product).sort(),
        ["pid", "pname", "price", "quantity"].sort()
    );

    assert.ok(Number.isInteger(product.pid));
    assert.ok(product.pid > 0);
    assert.equal(typeof product.pname, "string");
    assert.equal(typeof product.price, "number");
    assert.ok(Number.isFinite(product.price));
    assert.ok(product.price >= 0);
    assert.ok(Number.isInteger(product.quantity));
    assert.ok(product.quantity >= 0);
}

function assertMutation(result, pid) {
    assert.equal(result.status, 200);

    assert.deepEqual(
        Object.keys(result.data).sort(),
        ["message", "pid"].sort()
    );

    assert.equal(result.data.pid, pid);
    assert.equal(typeof result.data.message, "string");
    assert.ok(result.data.message.length > 0);
}

async function main() {
    const database = await pool.query(
        "SELECT current_database() AS name"
    );

    assert.equal(database.rows[0].name, "auth_db");

    const passwordHash = await hashPassword(password);

    const inserted = await pool.query(
        `
        INSERT INTO users (
            username,
            password,
            email,
            phone,
            roleid
        )
        SELECT $1, $2, $3, $4, roleid
        FROM roles
        WHERE roleid = 2 AND rolename = 'ADMIN'
        RETURNING uid
        `,
        [
            username,
            passwordHash,
            email,
            "0900000000",
        ]
    );

    assert.equal(inserted.rowCount, 1);
    adminUid = inserted.rows[0].uid;

    const login = await request("/api/auth/login", {
        method: "POST",
        authenticated: false,
        body: { username, password },
    });

    assert.equal(login.status, 200);
    assert.equal(typeof login.data.accessToken, "string");

    const payload = jwt.verify(
        login.data.accessToken,
        jwtConfig.secret,
        {
            algorithms: ["HS256"],
            issuer: jwtConfig.issuer,
            audience: jwtConfig.audience,
        }
    );

    assert.equal(payload.sub, String(adminUid));
    assert.equal(payload.role, "ADMIN");

    token = login.data.accessToken;

    console.log("Admin account and verified HTTP login: OK");

    const missingCustomer = await request(
        `/api/customers/${adminUid}`
    );

    assert.equal(missingCustomer.status, 404);

    console.log("TC-CUS-02 missing Customer returns 404: OK");

    const created = await request("/api/products", {
        method: "POST",
        body: {
            pname: originalName,
            price: 25.5,
            quantity: 10,
        },
    });

    assert.equal(created.status, 201);
    assertProduct(created.data);
    assert.equal(created.data.pname, originalName);
    assert.equal(created.data.price, 25.5);
    assert.equal(created.data.quantity, 10);

    const pid = created.data.pid;

    console.log("Admin HTTP product create: OK");

    const detail = await request(`/api/products/${pid}`);

    assert.equal(detail.status, 200);
    assertProduct(detail.data);
    assert.deepEqual(detail.data, created.data);

    const list = await request("/api/products");

    assert.equal(list.status, 200);
    assert.ok(Array.isArray(list.data.items));

    const listedProduct = list.data.items.find(
        (product) => product.pid === pid
    );

    assert.deepEqual(listedProduct, created.data);

    console.log("HTTP existing product read and list: OK");

    for (const invalidBody of [
        {
            pname: originalName,
            price: "25",
            quantity: 1,
        },
        {
            pname: originalName,
            price: 1.001,
            quantity: 1,
        },
        {
            pname: originalName,
            price: 25,
            quantity: -1,
        },
    ]) {
        const invalid = await request("/api/products", {
            method: "POST",
            body: invalidBody,
        });

        assert.equal(invalid.status, 400);
    }

    console.log("Admin HTTP product input validation: OK");

    const updated = await request(`/api/products/${pid}`, {
        method: "PUT",
        body: {
            pname: updatedName,
            price: 30.75,
            quantity: 0,
        },
    });

    assertMutation(updated, pid);

    const afterUpdate = await request(`/api/products/${pid}`);

    assert.equal(afterUpdate.status, 200);
    assertProduct(afterUpdate.data);

    assert.deepEqual(afterUpdate.data, {
        pid,
        pname: updatedName,
        price: 30.75,
        quantity: 0,
    });

    console.log("Admin HTTP product update and decimal price: OK");

    const deleted = await request(`/api/products/${pid}`, {
        method: "DELETE",
    });

    assertMutation(deleted, pid);

    const afterDelete = await request(`/api/products/${pid}`);

    assert.equal(afterDelete.status, 404);
    assert.equal(afterDelete.data.message, "Product not found");

    console.log("Admin HTTP product delete and NOT_FOUND: OK");
}

async function cleanupProducts() {
    if (!token) {
        return;
    }

    const list = await request("/api/products");

    assert.equal(list.status, 200);
    assert.ok(Array.isArray(list.data.items));

    const fixtures = list.data.items.filter(
        (product) => fixtureNames.has(product.pname)
    );

    for (const product of fixtures) {
        const deleted = await request(
            `/api/products/${product.pid}`,
            { method: "DELETE" }
        );

        assert.ok(
            deleted.status === 200 ||
            deleted.status === 404
        );
    }

    const remaining = await request("/api/products");

    assert.equal(remaining.status, 200);
    assert.ok(Array.isArray(remaining.data.items));

    assert.equal(
        remaining.data.items.filter(
            (product) => fixtureNames.has(product.pname)
        ).length,
        0
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Admin Product HTTP check failed:",
        error.message
    );

    process.exitCode = 1;
} finally {
    let cleanupSucceeded = true;

    try {
        await cleanupProducts();
    } catch (error) {
        cleanupSucceeded = false;
        process.exitCode = 1;

        console.error(
            "Product fixture cleanup failed:",
            error.message
        );

        console.error(
            "Fixture names to check:",
            [...fixtureNames].join(", ")
        );
    }

    try {
        if (adminUid !== undefined) {
            const removed = await pool.query(
                `
                DELETE FROM users
                WHERE uid = $1
                  AND username = $2
                  AND email = $3
                  AND roleid = 2
                `,
                [adminUid, username, email]
            );

            assert.equal(removed.rowCount, 1);
        }
    } catch (error) {
        cleanupSucceeded = false;
        process.exitCode = 1;

        console.error(
            "Admin fixture cleanup failed:",
            error.message
        );

        console.error("Admin fixture username:", username);
    }

    try {
        await pool.end();
    } catch (error) {
        cleanupSucceeded = false;
        process.exitCode = 1;

        console.error(
            "Database connection cleanup failed:",
            error.message
        );
    }

    if (cleanupSucceeded) {
        console.log("Scoped product and admin fixtures removed: OK");
    }
}