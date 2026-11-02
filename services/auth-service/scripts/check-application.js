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
    createAuthApplication,
} from "../src/application/auth.application.js";
import {
    verifyAccessToken,
} from "../src/infrastructure/security/token.js";
import {
    customerClient,
} from "../src/infrastructure/grpc/customer.client.js";

async function expectError(action, code) {
    await assert.rejects(action, (error) => error.code === code);
}

let client;
let transactionActive = false;

try {
    const username = `auth_${randomUUID().replaceAll("-", "")}`;

    const input = {
        username,
        password: "P@ss123",
        email: `${username}@example.com`,
        phone: "0900000000",
        role: "ADMIN",
    };

    client = await pool.connect();

    const users = createUserRepository(client);
    const tasks = createProvisioningRepository(client);

    let customerCalls = 0;

    const provisioning = createProvisioningApplication({
        repository: tasks,
        client: {
            async createProfile(request) {
                customerCalls += 1;

                if (customerCalls === 1) {
                    throw new DomainError(
                        "SERVICE_UNAVAILABLE",
                        "Simulated Customer outage"
                    );
                }

                return {
                    uid: request.uid,
                    fullname: request.fullname,
                    mid: 1,
                    mname: "Standard",
                    score: 0,
                };
            },
        },
    });

    const application = createAuthApplication({
        users,
        tasks,
        provisioning,
    });

    await client.query("BEGIN");
    transactionActive = true;

    await expectError(
        () => application.register(input),
        "SERVICE_UNAVAILABLE"
    );

    const pendingUser = await users.findByUsername(username);
    assert.ok(pendingUser);
    assert.equal(pendingUser.role, "CUSTOMER");

    const pendingTask = await tasks.findByUid(pendingUser.uid);
    assert.equal(pendingTask.completed_at, null);

    await expectError(
        () => application.login(input),
        "SERVICE_UNAVAILABLE"
    );

    await expectError(
        () => application.register({
            ...input,
            password: "WrongPassword",
        }),
        "ALREADY_EXISTS"
    );

    const registered = await application.register(input);

    assert.deepEqual(registered, {
        message: "User registered successfully",
        uid: pendingUser.uid,
    });

    assert.equal(customerCalls, 2);

    const loggedIn = await application.login(input);
    const payload = verifyAccessToken(loggedIn.accessToken);

    assert.equal(payload.sub, String(pendingUser.uid));
    assert.equal(payload.role, "CUSTOMER");
    assert.equal(loggedIn.tokenType, "Bearer");
    assert.equal(loggedIn.expiresIn, 3600);

    await expectError(
        () => application.register(input),
        "ALREADY_EXISTS"
    );

    await expectError(
        () => application.login({
            username,
            password: "WrongPassword",
        }),
        "UNAUTHENTICATED"
    );

    await expectError(
        () => application.login({
            username: `${username}_missing`,
            password: "P@ss123",
        }),
        "UNAUTHENTICATED"
    );

    await client.query("ROLLBACK");
    transactionActive = false;

    assert.equal(await users.findByUsername(username), null);
    assert.equal(await tasks.findByUid(pendingUser.uid), null);

    console.log("Auth register and login application: OK");
    console.log("Pending profile blocks login: OK");
    console.log("Registration recovery and duplicate checks: OK");
    console.log("Login JWT contains correct subject and role: OK");
    console.log("Rollback removes test data: OK");
} catch (error) {
    console.error("Auth application check failed:", error.code ?? error.message);
    process.exitCode = 1;
} finally {
    try {
        if (client && transactionActive) {
            await client.query("ROLLBACK");
        }
    } finally {
        client?.release();
        customerClient.close();
        await pool.end();
    }
}