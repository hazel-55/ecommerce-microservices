import assert from "node:assert/strict";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    createCustomerRepository,
} from "../src/infrastructure/repositories/customer.repository.js";

let client;
let transactionActive = false;

try {
    client = await pool.connect();
    const repository = createCustomerRepository(client);
    const testUid = 2147483647;

    await client.query("BEGIN");
    transactionActive = true;

    assert.equal(
        await repository.findByUid(testUid),
        null,
        "UID thu da ton tai; dung kiem tra, khong ghi de"
    );

    const created = await repository.createDefault({
        uid: testUid,
        fullname: "O'Brien",
    });

    assert.deepEqual(created, {
        uid: testUid,
        fullname: "O'Brien",
        mid: 1,
        mname: "Standard",
        score: 0,
    });

    const retried = await repository.createDefault({
        uid: testUid,
        fullname: "Changed",
    });

    assert.deepEqual(retried, created);

    const upgraded = await repository.update(testUid, {
        fullname: "John Smith",
        mid: 2,
    });

    assert.deepEqual(upgraded, {
        uid: testUid,
        fullname: "John Smith",
        mid: 2,
        mname: "Silver",
        score: 100,
    });

    const renamed = await repository.update(testUid, {
        fullname: "John Doe",
    });

    assert.deepEqual(renamed, {
        uid: testUid,
        fullname: "John Doe",
        mid: 2,
        mname: "Silver",
        score: 100,
    });

    await client.query("ROLLBACK");
    transactionActive = false;

    assert.equal(await repository.findByUid(testUid), null);

    console.log("Customer write repository: OK");
    console.log("Create retry preserves existing profile: OK");
    console.log("Update without mid preserves membership: OK");
    console.log("Rollback removes test data: OK");
} catch (error) {
    console.error("Write check failed:", error.code ?? error.message);
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