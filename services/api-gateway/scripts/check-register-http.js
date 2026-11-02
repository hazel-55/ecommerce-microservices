import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import jwt from "jsonwebtoken";
import { gatewayConfig } from "../src/config/env.js";

const execute = promisify(execFile);

const projectRoot = fileURLToPath(
    new URL("../../../", import.meta.url),
);

const baseUrl = `http://127.0.0.1:${gatewayConfig.port}`;
const suffix = randomUUID().replaceAll("-", "");

const account = {
    username: `http_${suffix}`,
    password: "P@ss123",
    email: `http_${suffix}@example.com`,
    phone: "0900000000",
};

const emptyPassword = {
    ...account,
    username: `empty_${suffix}`,
    email: `empty_${suffix}@example.com`,
    password: "",
};

const duplicateUsername = {
    ...account,
    email: `dup_${suffix}@example.com`,
};

const duplicateEmail = {
    ...account,
    username: `other_${suffix}`,
};

const candidates = [
    account,
    emptyPassword,
    duplicateUsername,
    duplicateEmail,
];

let cleanupAllowed = false;
const cleanupIds = [];

function sqlText(value) {
    return `'${value.replaceAll("'", "''")}'`;
}

const scope = candidates
    .map((item) =>
        `(
            username = ${sqlText(item.username)}
            AND email = ${sqlText(item.email)}
        )`,
    )
    .join(" OR ");

async function sql(service, user, database, statement) {
    const { stdout } = await execute(
        "docker",
        [
            "compose",
            "-f", "docker-compose.yml",
            "-f", "docker-compose.apps.yml",
            "exec", "-T", service,
            "psql",
            "-X",
            "-q",
            "-t",
            "-A",
            "-v", "ON_ERROR_STOP=1",
            "-U", user,
            "-d", database,
            "-c", statement,
        ],
        {
            cwd: projectRoot,
            timeout: 20_000,
            maxBuffer: 256 * 1024,
        },
    );

    return stdout.trim();
}

async function post(path, body) {
    const response = await fetch(`${baseUrl}${path}`, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(15_000),
    });

    if (response.status === 429) {
        throw new Error(
            `Rate limit reached; retry after ${response.headers.get("Retry-After") ?? "60"
            } seconds`,
        );
    }

    return {
        status: response.status,
        body: await response.json(),
    };
}

async function cleanup() {
    const removed = await sql(
        "postgres-auth",
        "auth_user",
        "auth_db",
        `
        BEGIN;

        SET LOCAL lock_timeout = '5s';
        SET LOCAL statement_timeout = '10s';

        DELETE FROM customer_provisioning_tasks
        WHERE uid IN (
            SELECT uid FROM users WHERE ${scope}
        );

        DELETE FROM users
        WHERE ${scope}
        RETURNING uid;

        COMMIT;
        `,
    );

    const ids = removed === ""
        ? []
        : removed.split(/\r?\n/).map((line) => {
            assert.match(line.trim(), /^[1-9]\d*$/);

            const id = Number(line.trim());

            assert.ok(
                Number.isInteger(id) &&
                id <= 2147483647,
            );

            return id;
        });

    cleanupIds.push(...ids);

    assert.equal(
        await sql(
            "postgres-auth",
            "auth_user",
            "auth_db",
            `SELECT count(*) FROM users WHERE ${scope}`,
        ),
        "0",
    );

    console.log(
        "Scoped Auth accounts and provisioning tasks removed: OK",
    );

    if (ids.length > 0) {
        const idList = ids.join(",");

        await sql(
            "postgres-customer",
            "customer_user",
            "customer_db",
            `
            BEGIN;

            SET LOCAL lock_timeout = '5s';
            SET LOCAL statement_timeout = '10s';

            DELETE FROM customers
            WHERE uid IN (${idList});

            COMMIT;
            `,
        );

        assert.equal(
            await sql(
                "postgres-customer",
                "customer_user",
                "customer_db",
                `
                SELECT count(*)
                FROM customers
                WHERE uid IN (${idList})
                `,
            ),
            "0",
        );
    }

    console.log("Scoped Customer profiles removed: OK");
}

try {
    assert.equal(
        await sql(
            "postgres-auth",
            "auth_user",
            "auth_db",
            `SELECT count(*) FROM users WHERE ${scope}`,
        ),
        "0",
    );

    assert.equal(
        await sql(
            "postgres-customer",
            "customer_user",
            "customer_db",
            "SELECT current_database()",
        ),
        "customer_db",
    );

    cleanupAllowed = true;

    console.log("Docker database preflight: OK");

    const registration = await post(
        "/api/auth/register",
        {
            ...account,
            role: "ADMIN",
        },
    );

    assert.equal(registration.status, 201);
    assert.equal(
        registration.body.message,
        "User registered successfully",
    );

    assert.ok(
        Number.isInteger(registration.body.uid) &&
        registration.body.uid > 0,
    );

    assert.deepEqual(
        Object.keys(registration.body).sort(),
        ["message", "uid"],
    );

    console.log(
        "TC-IAM-01 valid registration returns 201: OK",
    );

    assert.equal(
        (
            await post(
                "/api/auth/register",
                duplicateUsername,
            )
        ).status,
        409,
    );

    console.log(
        "TC-IAM-02 duplicate username returns 409: OK",
    );

    assert.equal(
        (
            await post(
                "/api/auth/register",
                emptyPassword,
            )
        ).status,
        400,
    );

    console.log(
        "TC-IAM-03 empty password returns 400: OK",
    );

    const login = await post("/api/auth/login", {
        username: account.username,
        password: account.password,
    });

    assert.equal(login.status, 200);
    assert.equal(login.body.tokenType, "Bearer");
    assert.equal(login.body.expiresIn, 3600);
    assert.equal(typeof login.body.accessToken, "string");

    const payload = jwt.verify(
        login.body.accessToken,
        gatewayConfig.jwtSecret,
        {
            algorithms: ["HS256"],
            issuer: gatewayConfig.jwtIssuer,
            audience: gatewayConfig.jwtAudience,
        },
    );

    assert.equal(
        payload.sub,
        String(registration.body.uid),
    );

    assert.equal(payload.role, "CUSTOMER");

    console.log(
        "TC-IAM-04 valid login returns verified JWT: OK",
    );

    console.log(
        "Registration cannot assign ADMIN role: OK",
    );

    assert.equal(
        (
            await post("/api/auth/login", {
                username: account.username,
                password: "WrongPassword123!",
            })
        ).status,
        401,
    );

    console.log(
        "TC-IAM-05 incorrect password returns 401: OK",
    );

    assert.equal(
        (
            await post(
                "/api/auth/register",
                duplicateEmail,
            )
        ).status,
        409,
    );

    console.log("Duplicate email returns 409: OK");
} catch (error) {
    console.error(
        "Registration Docker HTTP check failed:",
        error.code ?? error.name,
    );

    if (error.message?.startsWith("Rate limit reached")) {
        console.error(error.message);
    }

    process.exitCode = 1;
} finally {
    try {
        if (cleanupAllowed) {
            await cleanup();
        }
    } catch (error) {
        console.error(
            "Registration fixture cleanup failed:",
            error.code ?? error.name,
        );

        console.error(
            "Fixture Customer UIDs:",
            cleanupIds,
        );

        console.error(
            "Fixture usernames:",
            [...new Set(
                candidates.map((item) => item.username),
            )],
        );

        process.exitCode = 1;
    }
}