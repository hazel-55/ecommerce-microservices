import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    createUserRepository,
} from "../src/infrastructure/repositories/user.repository.js";
import {
    createProvisioningRepository,
} from "../src/infrastructure/repositories/provisioning.repository.js";
import {
    hashPassword,
} from "../src/infrastructure/security/password.js";

let client;
let transactionActive = false;

try {
    const username = `task_${randomUUID().replaceAll("-", "")}`;
    const passwordHash = await hashPassword("P@ss123");

    client = await pool.connect();

    const users = createUserRepository(client);
    const tasks = createProvisioningRepository(client);

    await client.query("BEGIN");
    transactionActive = true;

    const user = await users.createCustomerUser({
        username,
        passwordHash,
        email: `${username}@example.com`,
        phone: "0900000000",
    });

    const initial = await tasks.findByUid(user.uid);

    assert.equal(initial.attempts, 0);
    assert.equal(initial.completed_at, null);

    const failed = await tasks.markFailed(
        user.uid,
        "SERVICE_UNAVAILABLE"
    );

    assert.equal(failed.attempts, 1);
    assert.equal(failed.completed_at, null);
    assert.equal(failed.last_error, "SERVICE_UNAVAILABLE");

    const schedule = await client.query(
        `
      SELECT next_attempt_at > CURRENT_TIMESTAMP AS scheduled
      FROM customer_provisioning_tasks
      WHERE uid = $1
    `,
        [user.uid]
    );

    assert.equal(schedule.rows[0].scheduled, true);

    assert.equal(
        (await tasks.findPending()).some((task) => task.uid === user.uid),
        false
    );

    const completed = await tasks.markCompleted(user.uid);

    assert.ok(completed.completed_at);
    assert.equal(completed.attempts, 2);
    assert.equal(completed.last_error, null);

    const repeated = await tasks.markCompleted(user.uid);
    assert.equal(repeated.attempts, 2);

    assert.equal(
        await tasks.markFailed(user.uid, "SERVICE_UNAVAILABLE"),
        null
    );

    assert.equal(
        (await tasks.findPending()).some((task) => task.uid === user.uid),
        false
    );

    await client.query("ROLLBACK");
    transactionActive = false;

    assert.equal(await tasks.findByUid(user.uid), null);
    assert.equal(await users.findByUsername(username), null);

    console.log("Provisioning task repository: OK");
    console.log("Failure and retry scheduling: OK");
    console.log("Completion is idempotent: OK");
    console.log("Rollback removes test data: OK");
} catch (error) {
    console.error("Task check failed:", error.code ?? error.message);
    process.exitCode = 1;
} finally {
    try {
        if (client && transactionActive) {
            await client.query("ROLLBACK");
        }
    } finally {
        client?.release();
        await pool.end();
    }
}