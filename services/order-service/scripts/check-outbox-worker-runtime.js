import assert from "node:assert/strict";
import {
    outboxWorkerConfig,
} from "../src/infrastructure/config/env.js";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    checkOutboxReadiness,
} from "../src/infrastructure/outbox-readiness.js";

try {
    assert.ok(
        Number.isInteger(outboxWorkerConfig.intervalMs)
    );
    assert.ok(outboxWorkerConfig.intervalMs >= 1);
    assert.ok(outboxWorkerConfig.intervalMs <= 60000);

    assert.ok(
        Number.isInteger(outboxWorkerConfig.batchSize)
    );
    assert.ok(outboxWorkerConfig.batchSize >= 1);
    assert.ok(outboxWorkerConfig.batchSize <= 100);

    console.log("Outbox worker runtime configuration: OK");

    const readiness = await checkOutboxReadiness();

    console.log("Outbox database and required schema: OK");
    console.log("Outbox Kafka topic readiness: OK");
    console.log("Outbox pending event scan: OK");

    console.log(
        `Pending candidate sample (limit ${readiness.candidateLimit}):`,
        readiness.candidateCount
    );
} catch (error) {
    console.error(
        "Outbox runtime check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    await pool.end();
}