import assert from "node:assert/strict";
import { pool } from "../src/infrastructure/database/pool.js";
import { customerRepository } from "../src/infrastructure/repositories/customer.repository.js";

try {
    const standard = await customerRepository.findMembershipById(1);

    assert.deepEqual(standard, {
        mid: 1,
        mname: "Standard",
        score: 0,
    });

    const silver = await customerRepository.findMembershipById(2);

    assert.deepEqual(silver, {
        mid: 2,
        mname: "Silver",
        score: 100,
    });

    const missingMembership =
        await customerRepository.findMembershipById(2147483647);

    assert.equal(missingMembership, null);

    const count = await pool.query(
        "SELECT COUNT(*) AS customer_count FROM customers"
    );

    assert.equal(
        Number(count.rows[0].customer_count),
        0,
        "Buoc kiem tra nay du kien customers dang trong"
    );

    const missingCustomer = await customerRepository.findByUid(1);

    assert.equal(missingCustomer, null);

    console.log("Customer read repository: OK");
    console.log("Membership lookup: OK");
    console.log("Missing records return null: OK");
} catch (error) {
    console.error("Repository check failed:", error.code ?? error.message);
    process.exitCode = 1;
} finally {
    await pool.end();
}