import assert from "node:assert/strict";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    createCustomerRepository,
} from "../src/infrastructure/repositories/customer.repository.js";
import {
    createCustomerApplication,
} from "../src/application/customer.application.js";

async function expectError(action, code) {
    await assert.rejects(action, (error) => error.code === code);
}

let client;
let transactionActive = false;

try {
    client = await pool.connect();

    const repository = createCustomerRepository(client);
    const application = createCustomerApplication(repository);
    const uid = 2147483647;

    const owner = { uid, role: "CUSTOMER" };
    const other = { uid: 1, role: "CUSTOMER" };
    const admin = { uid: 2, role: "ADMIN" };

    await client.query("BEGIN");
    transactionActive = true;

    assert.equal(await repository.findByUid(uid), null);

    await expectError(
        () => application.createCustomer(
            { uid, fullname: "John Doe" },
            { service: "api-gateway" }
        ),
        "FORBIDDEN"
    );

    const created = await application.createCustomer(
        { uid, fullname: "  John Doe  " },
        { service: "auth-service" }
    );

    assert.equal(created.fullname, "John Doe");
    assert.equal(created.mid, 1);

    const ownProfile = await application.getCustomer(uid, owner);
    assert.equal(ownProfile.uid, uid);

    await expectError(
        () => application.getCustomer(uid, other),
        "FORBIDDEN"
    );

    await expectError(
        () => application.getCustomer(2147483646, admin),
        "NOT_FOUND"
    );

    await expectError(
        () => application.updateCustomer(
            { uid, fullname: "John Smith", mid: 2 },
            owner
        ),
        "FORBIDDEN"
    );

    assert.equal((await repository.findByUid(uid)).mid, 1);

    await expectError(
        () => application.updateCustomer(
            { uid, fullname: "John Smith", mid: 2147483647 },
            admin
        ),
        "INVALID_ARGUMENT"
    );

    const upgraded = await application.updateCustomer(
        { uid, fullname: "John Smith", mid: 2 },
        admin
    );

    assert.equal(upgraded.mid, 2);
    assert.equal(upgraded.score, 100);

    const renamed = await application.updateCustomer(
        { uid, fullname: "John Doe", mid: 2 },
        owner
    );

    assert.equal(renamed.fullname, "John Doe");
    assert.equal(renamed.mid, 2);

    await client.query("ROLLBACK");
    transactionActive = false;

    assert.equal(await repository.findByUid(uid), null);

    console.log("Customer application: OK");
    console.log("Profile ownership enforcement: OK");
    console.log("Membership permission enforcement: OK");
    console.log("Rollback removes test data: OK");
} catch (error) {
    console.error("Application check failed:", error.code ?? error.message);
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