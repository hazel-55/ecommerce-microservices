import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    createUserRepository,
} from "../src/infrastructure/repositories/user.repository.js";
import {
    createProvisioningRepository,
} from "../src/infrastructure/repositories/provisioning.repository.js";
import {
    createProvisioningApplication,
} from "../src/application/provisioning.application.js";
import {
    customerClient,
} from "../src/infrastructure/grpc/customer.client.js";
import {
    hashPassword,
} from "../src/infrastructure/security/password.js";

let databaseClient;
let transactionActive = false;

try {
    const username = `retry_${randomUUID().replaceAll("-", "")}`;
    const passwordHash = await hashPassword("P@ss123");

    databaseClient = await pool.connect();

    const users = createUserRepository(databaseClient);
    const tasks = createProvisioningRepository(databaseClient);

    await databaseClient.query("BEGIN");
    transactionActive = true;

    const user = await users.createCustomerUser({
        username,
        passwordHash,
        email: `${username}@example.com`,
        phone: "0900000000",
    });

    let calls = 0;

    const fakeCustomer = {
        async createProfile(input) {
            calls += 1;

            assert.deepEqual(input, {
                uid: user.uid,
                fullname: username,
            });

            if (calls === 1) {
                throw new DomainError(
                    "SERVICE_UNAVAILABLE",
                    "Simulated connection failure"
                );
            }

            return {
                uid: input.uid,
                fullname: input.fullname,
                mid: 1,
                mname: "Standard",
                score: 0,
            };
        },
    };

    const application = createProvisioningApplication({
        repository: tasks,
        client: fakeCustomer,
    });

    await assert.rejects(
        () => application.provisionCustomer(user.uid),
        (error) => error.code === "SERVICE_UNAVAILABLE"
    );

    const failed = await tasks.findByUid(user.uid);

    assert.equal(failed.completed_at, null);
    assert.equal(failed.attempts, 1);
    assert.equal(failed.last_error, "SERVICE_UNAVAILABLE");

    const retried = await application.provisionCustomer(user.uid);

    assert.deepEqual(retried, { uid: user.uid, ready: true });

    const completed = await tasks.findByUid(user.uid);

    assert.ok(completed.completed_at);
    assert.equal(completed.attempts, 2);

    await application.provisionCustomer(user.uid);
    assert.equal(calls, 2);

    await databaseClient.query("ROLLBACK");
    transactionActive = false;

    assert.equal(await tasks.findByUid(user.uid), null);
    assert.equal(await users.findByUsername(username), null);

    console.log("Provisioning application: OK");
    console.log("Failure keeps task pending: OK");
    console.log("Retry and completed-task idempotence: OK");
    console.log("Rollback removes test data: OK");
} catch (error) {
    console.error("Provisioning check failed:", error.code ?? error.message);
    process.exitCode = 1;
} finally {
    try {
        if (databaseClient && transactionActive) {
            await databaseClient.query("ROLLBACK");
        }
    } finally {
        databaseClient?.release();
        customerClient.close();
        await pool.end();
    }
}