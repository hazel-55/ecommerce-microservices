import { readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

const require = createRequire(import.meta.url);
const packagePath = require.resolve("kafkajs/package.json");
const packageInfo = JSON.parse(readFileSync(packagePath, "utf8"));

if (packageInfo.version !== "2.2.4") {
    throw new Error(
        "KafkaJS version changed; review the timer patch before applying"
    );
}

const sourcePath = join(
    dirname(packagePath),
    "src/network/requestQueue/index.js"
);

const source = readFileSync(sourcePath, "utf8");
const newline = source.includes("\r\n") ? "\r\n" : "\n";

const original = [
    "    let scheduleAt = this.throttledUntil - Date.now()",
    "    if (!this.throttleCheckTimeoutId) {",
].join(newline);

const patched = [
    "    let scheduleAt = this.throttledUntil - Date.now()",
    "    // Local compatibility fix: do not schedule an idle, expired timer.",
    "    if (this.pending.length === 0 && scheduleAt <= 0) {",
    "      return",
    "    }",
    "    if (!this.throttleCheckTimeoutId) {",
].join(newline);

if (source.includes(patched)) {
    console.log("KafkaJS timer patch already applied: OK");
} else {
    if (source.split(original).length !== 2) {
        throw new Error(
            "Unexpected KafkaJS source; timer patch was not applied"
        );
    }

    writeFileSync(
        sourcePath,
        source.replace(original, patched),
        "utf8"
    );

    console.log("KafkaJS timer patch applied: OK");
}