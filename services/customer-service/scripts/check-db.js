import assert from "node:assert/strict";
import { pool } from "../src/infrastructure/database/pool.js";

try {
    const connection = await pool.query(`
    SELECT
      current_database() AS database,
      current_user AS username
  `);

    assert.equal(connection.rows[0].database, "customer_db");
    assert.equal(connection.rows[0].username, "customer_user");

    const memberships = await pool.query(`
    SELECT mid, mname, score
    FROM memberships
    ORDER BY mid
  `);

    assert.deepEqual(memberships.rows, [
        { mid: 1, mname: "Standard", score: 0 },
        { mid: 2, mname: "Silver", score: 100 },
        { mid: 3, mname: "Gold", score: 500 },
    ]);

    console.log("Customer DB connection: OK");
    console.log("Database:", connection.rows[0].database);
    console.log("User:", connection.rows[0].username);
    console.log("Membership seed: OK");
} catch (error) {
    console.error("Customer DB check failed:", error.code ?? error.message);
    process.exitCode = 1;
} finally {
    await pool.end();
}