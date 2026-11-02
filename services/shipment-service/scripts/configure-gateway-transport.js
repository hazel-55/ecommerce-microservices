import { readFile, writeFile } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import dotenv from "dotenv";

const TOKEN_KEY = "SHIPMENT_GATEWAY_TOKEN";
const ADDRESS_KEY = "SHIPMENT_GRPC_ADDRESS";
const TARGET_KEYS = [TOKEN_KEY, ADDRESS_KEY];

async function load(url) {
    const source = await readFile(url, "utf8");

    for (const key of TARGET_KEYS) {
        const pattern = new RegExp(
            `^\\s*(?:export\\s+)?${key}\\s*=`,
            "gm"
        );

        if ([...source.matchAll(pattern)].length > 1) {
            throw new Error(`Duplicate environment key: ${key}`);
        }
    }

    return {
        url,
        source,
        values: dotenv.parse(source),
    };
}

function sharedValue(files, key, fallback) {
    const existing = files
        .map((file) => file.values[key])
        .filter((value) => value !== undefined);

    if (new Set(existing).size > 1) {
        throw new Error(`${key} differs between services`);
    }

    return existing[0] ?? fallback;
}

function quoteValue(value) {
    if (/[\r\n]/.test(value)) {
        throw new Error("Transport values must use one line");
    }

    for (const quote of ["'", "`"]) {
        if (!value.includes(quote)) {
            return `${quote}${value}${quote}`;
        }
    }

    throw new Error("Transport value cannot be safely appended");
}

async function main() {
    const shipment = await load(
        new URL("../.env", import.meta.url)
    );

    const gateway = await load(
        new URL("../../api-gateway/.env", import.meta.url)
    );

    const files = [shipment, gateway];

    const token = sharedValue(
        files,
        TOKEN_KEY,
        randomBytes(32).toString("hex")
    );

    const address = sharedValue(
        files,
        ADDRESS_KEY,
        "127.0.0.1:50055"
    );

    if (token.length < 32) {
        throw new Error(
            "SHIPMENT_GATEWAY_TOKEN must contain at least 32 characters"
        );
    }

    if (
        address.length === 0 ||
        /\s/.test(address)
    ) {
        throw new Error("Invalid SHIPMENT_GRPC_ADDRESS");
    }

    const existingTokens = [
        "CUSTOMER_GATEWAY_TOKEN",
        "PRODUCT_GATEWAY_TOKEN",
        "ORDER_GATEWAY_TOKEN",
    ].map((key) => {
        const value = gateway.values[key];

        if (typeof value !== "string" || value.length < 32) {
            throw new Error(`Invalid Gateway token configuration: ${key}`);
        }

        return value;
    });

    if (new Set([...existingTokens, token]).size !== 4) {
        throw new Error("Gateway service tokens must be distinct");
    }

    const configured = {
        [TOKEN_KEY]: token,
        [ADDRESS_KEY]: address,
    };

    // Prepare both updates before writing either file.
    const updates = files.map((file) => {
        const newline = file.source.includes("\r\n")
            ? "\r\n"
            : "\n";

        const additions = TARGET_KEYS
            .filter((key) => file.values[key] === undefined)
            .map((key) => `${key}=${quoteValue(configured[key])}`);

        if (additions.length === 0) {
            return { ...file, updated: file.source };
        }

        const separator =
            file.source.length > 0 &&
                !file.source.endsWith("\n")
                ? newline
                : "";

        return {
            ...file,
            updated:
                file.source +
                separator +
                additions.join(newline) +
                newline,
        };
    });

    for (const file of updates) {
        if (file.updated !== file.source) {
            await writeFile(file.url, file.updated, "utf8");
        }
    }

    for (const file of files) {
        const saved = dotenv.parse(
            await readFile(file.url, "utf8")
        );

        for (const key of TARGET_KEYS) {
            if (saved[key] !== configured[key]) {
                throw new Error(`Saved configuration mismatch: ${key}`);
            }
        }
    }

    console.log("Gateway to Shipment shared token configured: OK");
    console.log("Shipment gRPC address configured: OK");
    console.log("Existing Gateway service tokens remain distinct: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Shipment transport setup failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
}