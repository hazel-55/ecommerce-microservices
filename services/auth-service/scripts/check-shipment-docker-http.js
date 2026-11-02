import assert from "node:assert/strict";
import { randomUUID, randomBytes } from "node:crypto";
import { readFile } from "node:fs/promises";
import jwt from "jsonwebtoken";

import { pool } from "../src/infrastructure/database/pool.js";
import { hashPassword } from "../src/infrastructure/security/password.js";
import { jwtConfig } from "../src/infrastructure/config/env.js";

const fixture = JSON.parse(
    await readFile(
        new URL(
            "../../product-service/scripts/order-create-live.fixture.json",
            import.meta.url,
        ),
        "utf8",
    ),
);

assert.equal(fixture.version, 1);
assert.equal(fixture.verified, true);
assert.deepEqual(fixture.actor, {
    uid: 13,
    role: "CUSTOMER",
});

for (const value of [fixture.oid, fixture.shipid]) {
    assert.ok(
        Number.isInteger(value) &&
        value > 0 &&
        value <= 2147483647,
    );
}

const fixtureId = randomUUID();
const username = `shipment_admin_${fixtureId}`;
const email = `shipment_admin_${fixtureId}@example.test`;
const password = `Test!Aa1${randomBytes(20).toString("hex")}`;

let adminUid;
let token;

async function request(path, {
    method = "GET",
    body,
    accessToken = token,
} = {}) {
    const headers = {};

    if (accessToken) {
        headers.Authorization = `Bearer ${accessToken}`;
    }

    if (body !== undefined) {
        headers["Content-Type"] = "application/json";
    }

    const response = await fetch(
        `http://127.0.0.1:3001${path}`,
        {
            method,
            headers,
            body: body === undefined
                ? undefined
                : JSON.stringify(body),
            signal: AbortSignal.timeout(15_000),
        },
    );

    if (response.status === 429) {
        throw new Error(
            `Rate limit reached; retry after ${response.headers.get("Retry-After") ?? "60"
            } seconds`,
        );
    }

    return {
        status: response.status,
        data: await response.json(),
    };
}

function verifyLogin(login, uid, role) {
    assert.equal(login.status, 200);

    const claims = jwt.verify(
        login.data.accessToken,
        jwtConfig.secret,
        {
            algorithms: ["HS256"],
            issuer: jwtConfig.issuer,
            audience: jwtConfig.audience,
        },
    );

    assert.equal(claims.sub, String(uid));
    assert.equal(claims.role, role);

    return login.data.accessToken;
}

try {
    const database = await pool.query(
        "SELECT current_database() AS name",
    );

    assert.equal(database.rows[0].name, "auth_db");

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
            await hashPassword(password),
            email,
            "0900000000",
        ],
    );

    assert.equal(inserted.rowCount, 1);
    adminUid = inserted.rows[0].uid;

    token = verifyLogin(
        await request("/api/auth/login", {
            method: "POST",
            body: { username, password },
            accessToken: null,
        }),
        adminUid,
        "ADMIN",
    );

    console.log(
        "Temporary Admin and verified Docker HTTP login: OK",
    );

    const path = `/api/shipments/${fixture.shipid}`;
    const before = await request(path);

    assert.equal(before.status, 200);

    assert.deepEqual(
        Object.keys(before.data).sort(),
        ["oid", "shipid", "status"],
    );

    assert.equal(before.data.shipid, fixture.shipid);
    assert.equal(before.data.oid, fixture.oid);

    assert.ok(
        ["PENDING", "SHIPPING"].includes(before.data.status),
    );

    assert.equal(
        (
            await request(path, {
                accessToken: null,
            })
        ).status,
        401,
    );

    assert.equal(
        (
            await request(`${path}/status`, {
                method: "PATCH",
                body: { status: "SHIPPING" },
                accessToken: null,
            })
        ).status,
        401,
    );

    console.log(
        "Docker Shipment HTTP routes require JWT: OK",
    );

    const customerToken = verifyLogin(
        await request("/api/auth/login", {
            method: "POST",
            accessToken: null,
            body: {
                username:
                    "http_8678dd4139734fe18d85dee359c76237",
                password: "P@ss123",
            },
        }),
        fixture.actor.uid,
        "CUSTOMER",
    );

    assert.equal(
        (
            await request(`${path}/status`, {
                method: "PATCH",
                body: { status: "SHIPPING" },
                accessToken: customerToken,
            })
        ).status,
        403,
    );

    assert.deepEqual(
        (await request(path)).data,
        before.data,
    );

    console.log(
        "Customer mutation is forbidden and Shipment stays unchanged: OK",
    );

    let missingId;

    for (
        let candidate = 2147483647;
        candidate > 2147483637;
        candidate -= 1
    ) {
        const result = await request(
            `/api/shipments/${candidate}`,
        );

        if (result.status === 404) {
            missingId = candidate;
            break;
        }

        assert.equal(result.status, 200);
    }

    assert.ok(
        missingId,
        "No missing Shipment ID found in the checked range",
    );

    assert.equal(
        (
            await request(
                `/api/shipments/${missingId}/status`,
                {
                    method: "PATCH",
                    body: { status: "SHIPPING" },
                },
            )
        ).status,
        404,
    );

    console.log(
        "TC-SHP-04 missing Shipment GET and PATCH return 404: OK",
    );

    const updated = await request(`${path}/status`, {
        method: "PATCH",
        body: { status: "SHIPPING" },
    });

    assert.equal(updated.status, 200);

    const expected = {
        shipid: fixture.shipid,
        oid: fixture.oid,
        status: "SHIPPING",
    };

    assert.deepEqual(updated.data, expected);
    assert.deepEqual(
        (await request(path)).data,
        expected,
    );

    console.log(
        "TC-SHP-03 Admin updates the real Shipment to SHIPPING: OK",
    );

    const replay = await request(`${path}/status`, {
        method: "PATCH",
        body: { status: "SHIPPING" },
    });

    assert.equal(replay.status, 200);
    assert.deepEqual(replay.data, expected);

    console.log(
        "Repeated SHIPPING request preserves Shipment identity: OK",
    );
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Docker Shipment HTTP check failed:",
        error.code ?? error.name,
        error.message,
    );

    console.error("Shipment fixture:", {
        oid: fixture.oid,
        shipid: fixture.shipid,
    });
} finally {
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
                [adminUid, username, email],
            );

            assert.equal(removed.rowCount, 1);
        }

        console.log(
            "Scoped Shipment Admin fixture removed: OK",
        );
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Admin fixture cleanup failed:",
            error.code ?? error.name,
        );

        console.error("Admin fixture username:", username);
    } finally {
        try {
            await pool.end();
        } catch (error) {
            process.exitCode = 1;

            console.error(
                "Auth pool close failed:",
                error.code ?? error.name,
            );
        }
    }
}