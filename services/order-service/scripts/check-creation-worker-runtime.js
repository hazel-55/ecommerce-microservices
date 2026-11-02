import assert from "node:assert/strict";
import {
    creationWorkerConfig,
} from "../src/infrastructure/config/env.js";
import {
    pool,
} from "../src/infrastructure/database/pool.js";
import {
    customerClient,
} from "../src/infrastructure/grpc/customer.client.js";
import {
    productClient,
} from "../src/infrastructure/grpc/product.client.js";
import {
    orderCreationRepository,
} from "../src/infrastructure/repositories/order-creation.repository.js";
import {
    validateCreationOperationId,
} from "../src/domain/order-creation.rules.js";

async function main() {
    assert.ok(
        Number.isInteger(creationWorkerConfig.intervalMs),
    );
    assert.ok(creationWorkerConfig.intervalMs >= 1);
    assert.ok(creationWorkerConfig.intervalMs <= 60_000);

    assert.ok(
        Number.isInteger(creationWorkerConfig.batchSize),
    );
    assert.ok(creationWorkerConfig.batchSize >= 1);
    assert.ok(creationWorkerConfig.batchSize <= 100);

    console.log("Worker runtime configuration: OK");

    await pool.query(
        `
        SELECT
            operation_id,
            uid,
            actor_role,
            items,
            score,
            status,
            failure_code,
            attempts,
            last_error_code,
            next_attempt_at
        FROM order_creation_tasks
        LIMIT 0
        `,
    );

    console.log(
        "Order worker database and task schema: OK",
    );

    const ids =
        await orderCreationRepository.listDueOperationIds({
            limit: creationWorkerConfig.batchSize,
        });

    assert.ok(Array.isArray(ids));
    assert.ok(ids.length <= creationWorkerConfig.batchSize);
    assert.equal(new Set(ids).size, ids.length);

    for (const id of ids) {
        validateCreationOperationId(id);
    }

    console.log("Worker pending task scan: OK");
    console.log("Pending task candidates:", ids.length);
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Creation worker runtime check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    customerClient.close();
    productClient.close();
    await pool.end();
}