import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    createUserRepository,
} from "../src/infrastructure/repositories/user.repository.js";
import {
    hashPassword,
    verifyPassword,
} from "../src/infrastructure/security/password.js";

let client;
let transactionActive = false;

try {
    const username = `repo_${randomUUID().replaceAll("-", "")}`;
    const email = `${username}@example.com`;
    const passwordHash = await hashPassword("P@ss123");

    client = await pool.connect();
    const repository = createUserRepository(client);

    await client.query("BEGIN");
    transactionActive = true;

    assert.equal(await repository.findByUsername(username), null);

    const created = await repository.createCustomerUser({
        username,
        passwordHash,
        email,
        phone: "0900000000",
    });

    assert.equal(created.role, "CUSTOMER");

    const task = await client.query(
        `
    SELECT uid, fullname, completed_at, attempts
    FROM customer_provisioning_tasks
    WHERE uid = $1
  `,
        [created.uid]
    );

    assert.deepEqual(task.rows, [
        {
            uid: created.uid,
            fullname: username,
            completed_at: null,
            attempts: 0,
        },
    ]);

    const loginRecord = await repository.findByUsername(username);

    assert.equal(loginRecord.password, passwordHash);
    assert.equal(
        await verifyPassword("P@ss123", loginRecord.password),
        true
    );

    const profile = await repository.findByUid(created.uid);

    assert.equal(profile.email, email);
    assert.equal(Object.hasOwn(profile, "password"), false);

    await client.query("SAVEPOINT duplicate_username");

    await assert.rejects(
        () => repository.createCustomerUser({
            username,
            passwordHash,
            email: `other_${email}`,
            phone: "0900000000",
        }),
        (error) => error.code === "ALREADY_EXISTS"
    );

    await client.query("ROLLBACK TO SAVEPOINT duplicate_username");
    await client.query("RELEASE SAVEPOINT duplicate_username");

    await client.query("SAVEPOINT duplicate_email");

    await assert.rejects(
        () => repository.createCustomerUser({
            username: `${username}_other`,
            passwordHash,
            email,
            phone: "0900000000",
        }),
        (error) => error.code === "ALREADY_EXISTS"
    );

    await client.query("ROLLBACK TO SAVEPOINT duplicate_email");
    await client.query("RELEASE SAVEPOINT duplicate_email");

    await client.query("ROLLBACK");
    transactionActive = false;

    assert.equal(await repository.findByUsername(username), null);

    const rolledBackTask = await client.query(
        "SELECT uid FROM customer_provisioning_tasks WHERE uid = $1",
        [created.uid]
    );

    assert.equal(rolledBackTask.rowCount, 0);

    console.log("User and provisioning task stored together: OK");

    console.log("Auth user repository: OK");
    console.log("Hash storage and default role: OK");
    console.log("Duplicate username/email rejected: OK");
    console.log("Rollback removes test data: OK");
} catch (error) {
    console.error("Repository check failed:", error.code ?? error.message);
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