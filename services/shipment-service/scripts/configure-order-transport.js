import { readFile, writeFile } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import dotenv from "dotenv";

const TOKEN_KEY = "SHIPMENT_ORDER_TOKEN";
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
    const files = await Promise.all([
        load(new URL("../.env", import.meta.url)),
        load(new URL("../../order-service/.env", import.meta.url)),
    ]);

    const shipmentAddress = files[0].values[ADDRESS_KEY];

    if (
        typeof shipmentAddress !== "string" ||
        shipmentAddress.length === 0 ||
        /\s/.test(shipmentAddress)
    ) {
        throw new Error("Shipment gRPC address is not configured");
    }

    const token = sharedValue(
        files,
        TOKEN_KEY,
        randomBytes(32).toString("hex")
    );

    const address = sharedValue(
        files,
        ADDRESS_KEY,
        shipmentAddress
    );

    if (token.length < 32 || /[\r\n]/.test(token)) {
        throw new Error("Invalid SHIPMENT_ORDER_TOKEN");
    }

    const otherTokens = files.flatMap((file) =>
        Object.entries(file.values)
            .filter(([key]) =>
                key.endsWith("_TOKEN") &&
                key !== TOKEN_KEY
            )
            .map(([, value]) => value)
    );

    if (otherTokens.includes(token)) {
        throw new Error(
            "Shipment Order token must differ from existing service tokens"
        );
    }

    const configured = {
        [TOKEN_KEY]: token,
        [ADDRESS_KEY]: address,
    };

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

    console.log("Order to Shipment shared token configured: OK");
    console.log("Order and Shipment gRPC address binding: OK");
    console.log(
        "Shipment Order token differs from existing service tokens: OK"
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Order Shipment transport setup failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
}