import assert from "node:assert/strict";
import { pool } from "../src/infrastructure/database/pool.js";

try {
    const connection = await pool.query(`
    SELECT
      current_database() AS database,
      current_user AS username
  `);

    assert.equal(connection.rows[0].database, "auth_db");
    assert.equal(connection.rows[0].username, "auth_user");

    const roles = await pool.query(`
    SELECT roleid, rolename
    FROM roles
    ORDER BY roleid
  `);

    assert.deepEqual(roles.rows, [
        { roleid: 1, rolename: "CUSTOMER" },
        { roleid: 2, rolename: "ADMIN" },
        { roleid: 3, rolename: "SHIPMENT_STAFF" },
    ]);

    const users = await pool.query(
        "SELECT COUNT(*) AS user_count FROM users"
    );

    assert.equal(Number(users.rows[0].user_count), 0);

    console.log("Auth DB connection: OK");
    console.log("Database:", connection.rows[0].database);
    console.log("User:", connection.rows[0].username);
    console.log("Role seed: OK");
    console.log("User count: 0");
} catch (error) {
    console.error("Auth DB check failed:", error.code ?? error.message);
    process.exitCode = 1;
} finally {
    await pool.end();
}