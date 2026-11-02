import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
    mkdtemp,
    readFile,
    readdir,
    writeFile,
    rm,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, sep } from "node:path";
import { pathToFileURL } from "node:url";
import dotenv from "dotenv";
import pg from "pg";

import {
    runMigrations,
} from "../src/infrastructure/database/migrate.js";

const service = JSON.parse(
    await readFile(
        new URL("../package.json", import.meta.url),
        "utf8",
    ),
).name;

const definitions = {
    "auth-service": {
        database: "auth_db",
        tables: [
            "roles",
            "users",
            "customer_provisioning_tasks",
        ],
        files: [
            "001_init.sql",
            "002_customer_provisioning.sql",
        ],
    },
    "customer-service": {
        database: "customer_db",
        tables: ["memberships", "customers"],
        files: ["001_init.sql"],
    },
    "order-service": {
        database: "order_db",
        tables: [
            "orders",
            "order_details",
            "outbox_events",
            "order_creation_tasks",
        ],
        files: [
            "001_init.sql",
            "002_order_creation_tasks.sql",
        ],
    },
};

const definition = definitions[service];

const directory = new URL(
    "../src/infrastructure/database/migrations/",
    import.meta.url,
);

const schemas = [];
const clients = [];

const prefix =
    `migration_check_${randomUUID().replaceAll("-", "")}`;

let admin;
let adminConnected = false;
let temporaryDirectory;
let config;
let stage = "configuration";

async function schema(label) {
    const name = `${prefix}_${label}`;

    assert.match(
        name,
        /^migration_check_[a-f0-9]{32}_[a-z]+$/,
    );

    await admin.query(`CREATE SCHEMA "${name}"`);

    schemas.push(name);

    return name;
}

async function clientFor(name) {
    const client = new pg.Client(config);

    clients.push(client);

    client.on("error", (error) => {
        process.exitCode = 1;

        console.error(
            "Migration test connection error:",
            error.code ?? "POSTGRES_ERROR",
        );
    });

    await client.connect();

    await client.query(
        `SET search_path TO "${name}"`,
    );

    return client;
}

function run(client, name, source = directory) {
    return runMigrations(client, {
        directory: source,
        expectedSchema: name,
    });
}

async function history(client) {
    const result = await client.query(`
        SELECT name, checksum, applied_at
        FROM _schema_migrations
        ORDER BY name
    `);

    return result.rows;
}

async function tableNames(name) {
    const result = await admin.query(
        `
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = $1
        ORDER BY tablename
        `,
        [name],
    );

    return result.rows
        .map((row) => row.tablename)
        .sort();
}

async function businessSnapshot(client, insert = false) {
    if (service === "auth-service") {
        if (insert) {
            await client.query(`
                INSERT INTO users (
                    username, password, email, phone, roleid
                )
                VALUES (
                    'migration_fixture',
                    'fixture-hash',
                    'migration@example.test',
                    '0900000000',
                    1
                )
            `);
        }

        return client.query(
            "SELECT * FROM users ORDER BY uid",
        ).then((result) => result.rows);
    }

    if (service === "customer-service") {
        if (insert) {
            await client.query(`
                INSERT INTO customers (uid, fullname, mid)
                VALUES (13, 'Migration fixture', 1)
            `);
        }

        return client.query(
            "SELECT * FROM customers ORDER BY uid",
        ).then((result) => result.rows);
    }

    if (insert) {
        await client.query(`
            INSERT INTO orders (uid, discount)
            VALUES (13, '1.25')
        `);
    }

    return client.query(
        "SELECT * FROM orders ORDER BY oid",
    ).then((result) => result.rows);
}

async function main() {
    assert.ok(definition);

    const settings = dotenv.parse(
        await readFile(
            new URL("../.env", import.meta.url),
        ),
    );

    for (const key of [
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSWORD",
    ]) {
        assert.equal(typeof settings[key], "string");
        assert.ok(settings[key].length > 0);
    }

    assert.equal(
        settings.PGDATABASE,
        definition.database,
    );

    const port = Number(settings.PGPORT);

    assert.ok(
        Number.isInteger(port) &&
        port > 0 &&
        port <= 65535,
    );

    config = {
        host: settings.PGHOST,
        port,
        database: settings.PGDATABASE,
        user: settings.PGUSER,
        password: settings.PGPASSWORD,
        connectionTimeoutMillis: 5000,
        statement_timeout: 30_000,
        lock_timeout: 5000,
    };

    admin = new pg.Client(config);

    admin.on("error", (error) => {
        process.exitCode = 1;

        console.error(
            "Migration test admin error:",
            error.code ?? "POSTGRES_ERROR",
        );
    });

    await admin.connect();
    adminConnected = true;

    const identity = await admin.query(
        "SELECT current_database() AS database",
    );

    assert.equal(
        identity.rows[0].database,
        definition.database,
    );

    const files = (await readdir(directory))
        .filter((name) => name.endsWith(".sql"))
        .sort();

    assert.deepEqual(files, definition.files);

    stage = "fresh schema";

    const freshSchema = await schema("fresh");
    const fresh = await clientFor(freshSchema);

    const first = await run(fresh, freshSchema);

    assert.deepEqual(
        first.map((result) => result.status),
        files.map(() => "APPLIED"),
    );

    assert.deepEqual(
        await tableNames(freshSchema),
        [
            ...definition.tables,
            "_schema_migrations",
        ].sort(),
    );

    const originalHistory = await history(fresh);

    assert.deepEqual(
        originalHistory.map((row) => row.name),
        files,
    );

    for (const row of originalHistory) {
        assert.match(row.checksum, /^[a-f0-9]{64}$/);
    }

    if (service === "auth-service") {
        const roles = await fresh.query(
            "SELECT rolename FROM roles ORDER BY roleid",
        );

        assert.deepEqual(
            roles.rows.map((row) => row.rolename),
            ["CUSTOMER", "ADMIN", "SHIPMENT_STAFF"],
        );
    }

    if (service === "customer-service") {
        const memberships = await fresh.query(
            "SELECT score FROM memberships ORDER BY mid",
        );

        assert.deepEqual(
            memberships.rows.map((row) => row.score),
            [0, 100, 500],
        );
    }

    console.log(
        "Fresh schema, seeds and migration history: OK",
    );

    stage = "replay";

    const snapshot = await businessSnapshot(
        fresh,
        true,
    );

    const replay = await run(fresh, freshSchema);

    assert.deepEqual(
        replay.map((result) => result.status),
        files.map(() => "ALREADY_APPLIED"),
    );

    assert.deepEqual(
        await history(fresh),
        originalHistory,
    );

    assert.deepEqual(
        await businessSnapshot(fresh),
        snapshot,
    );

    console.log(
        "Migration replay preserves history and existing data: OK",
    );

    stage = "previously initialized schema";

    const legacySchema = await schema("legacy");
    const legacy = await clientFor(legacySchema);

    for (const name of files) {
        const source = await readFile(
            new URL(name, directory),
            "utf8",
        );

        await legacy.query(
            source.replace(/^\uFEFF/, ""),
        );
    }

    const legacySnapshot = await businessSnapshot(
        legacy,
        true,
    );

    const adopted = await run(
        legacy,
        legacySchema,
    );

    assert.deepEqual(
        adopted.map((result) => result.status),
        files.map(() => "APPLIED"),
    );

    assert.deepEqual(
        await businessSnapshot(legacy),
        legacySnapshot,
    );

    assert.equal(
        (await history(legacy)).length,
        files.length,
    );

    console.log(
        "Existing schema without migration history is preserved: OK",
    );

    stage = "concurrent migrations";

    const concurrentSchema = await schema("concurrent");
    const left = await clientFor(concurrentSchema);
    const right = await clientFor(concurrentSchema);

    const results = await Promise.allSettled([
        run(left, concurrentSchema),
        run(right, concurrentSchema),
    ]);

    for (const result of results) {
        if (result.status === "rejected") {
            throw result.reason;
        }
    }

    const outcomes = results.flatMap(
        (result) => result.value,
    );

    assert.equal(
        outcomes.filter(
            (row) => row.status === "APPLIED",
        ).length,
        files.length,
    );

    assert.equal(
        outcomes.filter(
            (row) => row.status === "ALREADY_APPLIED",
        ).length,
        files.length,
    );

    assert.equal(
        (await history(left)).length,
        files.length,
    );

    console.log(
        "Concurrent migration runners apply each file once: OK",
    );

    stage = "checksum protection";

    temporaryDirectory = await mkdtemp(
        join(tmpdir(), "ecommerce-migrations-"),
    );

    const temporaryUrl = pathToFileURL(
        temporaryDirectory + sep,
    );

    for (const name of files) {
        await writeFile(
            join(temporaryDirectory, name),
            await readFile(new URL(name, directory)),
        );
    }

    const firstSource = await readFile(
        new URL(files[0], directory),
        "utf8",
    );

    await writeFile(
        join(temporaryDirectory, files[0]),
        firstSource.replace(
            /BEGIN;/i,
            "BEGIN;\n-- isolated checksum edit",
        ),
    );

    await assert.rejects(
        () => run(
            fresh,
            freshSchema,
            temporaryUrl,
        ),
        (error) =>
            error.code === "MIGRATION_CHECKSUM_MISMATCH",
    );

    assert.deepEqual(
        await history(fresh),
        originalHistory,
    );

    assert.deepEqual(
        await businessSnapshot(fresh),
        snapshot,
    );

    console.log(
        "Changed migration checksum rejected without data changes: OK",
    );

    stage = "batch rollback";

    await writeFile(
        join(temporaryDirectory, files[0]),
        firstSource,
    );

    await writeFile(
        join(
            temporaryDirectory,
            "999_forced_failure.sql",
        ),
        `
        BEGIN;
        CREATE TABLE rollback_marker (id INTEGER);
        SELECT 1 / 0;
        COMMIT;
        `,
    );

    const rollbackSchema = await schema("rollback");
    const rollbackClient = await clientFor(
        rollbackSchema,
    );

    await assert.rejects(
        () => run(
            rollbackClient,
            rollbackSchema,
            temporaryUrl,
        ),
        (error) =>
            error.code === "22012" &&
            error.rollbackFailed !== true,
    );

    assert.deepEqual(
        await tableNames(rollbackSchema),
        [],
    );

    console.log(
        "Late SQL failure rolls back the entire migration batch: OK",
    );

    stage = "retry after rollback";

    const recovered = await run(
        rollbackClient,
        rollbackSchema,
    );

    assert.deepEqual(
        recovered.map((result) => result.status),
        files.map(() => "APPLIED"),
    );

    assert.deepEqual(
        await tableNames(rollbackSchema),
        [
            ...definition.tables,
            "_schema_migrations",
        ].sort(),
    );

    console.log(
        "Connection and migration batch recover after rollback: OK",
    );
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Migration check failed:",
        {
            service,
            stage,
            code: error.code ?? error.name,
        },
    );

    if (error instanceof ReferenceError) {
        console.error(error.stack);
    }
} finally {
    let cleaned = true;

    const closed = await Promise.allSettled(
        clients.map((client) => client.end()),
    );

    if (
        closed.some(
            (result) => result.status === "rejected",
        )
    ) {
        cleaned = false;
    }

    if (adminConnected) {
        for (const name of schemas) {
            try {
                await admin.query(
                    `DROP SCHEMA "${name}" CASCADE`,
                );
            } catch {
                cleaned = false;
            }
        }

        try {
            const remaining = await admin.query(
                `
                SELECT nspname
                FROM pg_namespace
                WHERE nspname = ANY($1::text[])
                `,
                [schemas],
            );

            assert.equal(remaining.rowCount, 0);
        } catch {
            cleaned = false;
        }
    }

    if (temporaryDirectory) {
        try {
            await rm(temporaryDirectory, {
                recursive: true,
                force: true,
            });
        } catch {
            cleaned = false;
        }
    }

    if (admin) {
        try {
            await admin.end();
        } catch {
            cleaned = false;
        }
    }

    if (cleaned) {
        console.log(
            "Scoped migration schemas, temporary files and clients removed: OK",
        );
    } else {
        process.exitCode = 1;

        console.error(
            "Migration cleanup failed; fixture schemas:",
            schemas,
        );
    }
}