import { createHash } from "node:crypto";
import { readFile, readdir } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import dotenv from "dotenv";
import pg from "pg";

function migrationError(code, file) {
    const error = new Error(code);
    error.code = code;

    if (file) {
        error.file = file;
    }

    return error;
}

async function loadMigrations(directory) {
    const entries = await readdir(directory, {
        withFileTypes: true,
    });

    const names = entries
        .filter((entry) =>
            entry.isFile() &&
            entry.name.endsWith(".sql"))
        .map((entry) => entry.name)
        .sort();

    if (names.length === 0) {
        throw migrationError("NO_MIGRATIONS");
    }

    const migrations = [];

    for (const name of names) {
        if (!/^\d{3}_[a-z0-9_]+\.sql$/.test(name)) {
            throw migrationError(
                "INVALID_MIGRATION_NAME",
                name,
            );
        }

        const source = (
            await readFile(
                new URL(name, directory),
                "utf8",
            )
        )
            .replace(/^\uFEFF/, "")
            .replace(/\r\n/g, "\n")
            .trim();

        const match =
            /^BEGIN;\s*([\s\S]*?)\s*COMMIT;$/i.exec(source);

        if (
            !match ||
            match[1].trim().length === 0
        ) {
            throw migrationError(
                "INVALID_MIGRATION_WRAPPER",
                name,
            );
        }

        migrations.push({
            name,
            body: match[1],
            checksum: createHash("sha256")
                .update(source)
                .digest("hex"),
        });
    }

    return migrations;
}

export async function runMigrations(
    client,
    {
        directory = new URL(
            "./migrations/",
            import.meta.url,
        ),
        expectedSchema = "public",
    } = {},
) {
    if (
        !client ||
        typeof client.query !== "function"
    ) {
        throw new TypeError(
            "A connected PostgreSQL client is required",
        );
    }

    const migrations = await loadMigrations(directory);
    const report = [];

    let started = false;
    let currentFile;

    try {
        await client.query("BEGIN");
        started = true;

        await client.query(
            "SET LOCAL lock_timeout = '5s'",
        );

        await client.query(
            "SET LOCAL statement_timeout = '30s'",
        );

        const identity = await client.query(
            "SELECT current_schema() AS schema",
        );

        if (
            identity.rows[0].schema !==
            expectedSchema
        ) {
            throw migrationError(
                "MIGRATION_SCHEMA_MISMATCH",
            );
        }

        await client.query(`
            SELECT pg_advisory_xact_lock(
                165091,
                hashtext(
                    current_database()
                    || ':'
                    || current_schema()
                )
            )
        `);

        await client.query(`
            CREATE TABLE IF NOT EXISTS _schema_migrations (
                name TEXT PRIMARY KEY,
                checksum CHAR(64) NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL
                    DEFAULT CURRENT_TIMESTAMP
            )
        `);

        const history = await client.query(`
            SELECT name, checksum
            FROM _schema_migrations
            ORDER BY name
        `);

        for (
            let index = 0;
            index < history.rows.length;
            index += 1
        ) {
            const stored = history.rows[index];
            const migration = migrations[index];

            if (
                !migration ||
                stored.name !== migration.name
            ) {
                throw migrationError(
                    "MIGRATION_HISTORY_MISMATCH",
                    stored.name,
                );
            }

            if (
                stored.checksum !==
                migration.checksum
            ) {
                throw migrationError(
                    "MIGRATION_CHECKSUM_MISMATCH",
                    stored.name,
                );
            }
        }

        for (
            let index = 0;
            index < migrations.length;
            index += 1
        ) {
            const migration = migrations[index];

            currentFile = migration.name;

            if (index < history.rows.length) {
                report.push({
                    file: migration.name,
                    status: "ALREADY_APPLIED",
                });

                continue;
            }

            await client.query(migration.body);

            await client.query(
                `
                INSERT INTO _schema_migrations (
                    name,
                    checksum
                )
                VALUES ($1, $2)
                `,
                [
                    migration.name,
                    migration.checksum,
                ],
            );

            report.push({
                file: migration.name,
                status: "APPLIED",
            });
        }

        await client.query("COMMIT");
        started = false;

        return report;
    } catch (error) {
        if (currentFile && !error.file) {
            error.file = currentFile;
        }

        if (started) {
            try {
                await client.query("ROLLBACK");
            } catch {
                error.rollbackFailed = true;
            }
        }

        throw error;
    }
}

async function main() {
    dotenv.config({
        path: new URL(
            "../../../.env",
            import.meta.url,
        ),
        quiet: true,
    });

    const service = JSON.parse(
        await readFile(
            new URL(
                "../../../package.json",
                import.meta.url,
            ),
            "utf8",
        ),
    ).name;

    const databases = {
        "auth-service": "auth_db",
        "customer-service": "customer_db",
        "order-service": "order_db",
    };

    function required(name) {
        const value = process.env[name];

        if (
            typeof value !== "string" ||
            value.length === 0
        ) {
            throw migrationError(
                "MISSING_DATABASE_CONFIGURATION",
            );
        }

        return value;
    }

    const database = required("PGDATABASE");

    if (
        !databases[service] ||
        database !== databases[service]
    ) {
        throw migrationError(
            "MIGRATION_DATABASE_MISMATCH",
        );
    }

    const port = Number(required("PGPORT"));

    if (
        !Number.isInteger(port) ||
        port < 1 ||
        port > 65535
    ) {
        throw migrationError(
            "INVALID_DATABASE_PORT",
        );
    }

    const client = new pg.Client({
        host: required("PGHOST"),
        port,
        database,
        user: required("PGUSER"),
        password: required("PGPASSWORD"),
        connectionTimeoutMillis: 5000,
        application_name: `${service}-migrate`,
    });

    client.on("error", (error) => {
        process.exitCode = 1;

        console.error(
            "Migration connection error:",
            error.code ?? "POSTGRES_ERROR",
        );
    });

    try {
        await client.connect();

        const identity = await client.query(
            "SELECT current_database() AS database",
        );

        if (
            identity.rows[0].database !== database
        ) {
            throw migrationError(
                "MIGRATION_DATABASE_MISMATCH",
            );
        }

        const report = await runMigrations(client);

        for (const result of report) {
            console.log(result);
        }

        console.log(
            `${service} database migrations: OK`,
        );
    } finally {
        await client.end();
    }
}

if (
    process.argv[1] &&
    pathToFileURL(
        resolve(process.argv[1]),
    ).href === import.meta.url
) {
    main().catch((error) => {
        process.exitCode = 1;

        console.error(
            "Database migration failed:",
            {
                code: error.code ?? error.name,
                file: error.file,
                rollbackFailed:
                    error.rollbackFailed === true,
            },
        );
    });
}