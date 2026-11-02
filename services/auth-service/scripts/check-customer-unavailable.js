import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { pool } from "../src/infrastructure/database/pool.js";

const definition = loadSync(
    fileURLToPath(new URL("../../../proto/auth.proto", import.meta.url)),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    }
);

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.auth.v1;

const auth = new proto.AuthService(
    "127.0.0.1:50051",
    grpc.credentials.createInsecure()
);

function invoke(method, request) {
    return new Promise((resolve, reject) => {
        auth[method](
            request,
            { deadline: Date.now() + 10000 },
            (error, response) => {
                if (error) reject(error);
                else resolve(response);
            }
        );
    });
}

try {
    const username = `recovery_${randomUUID().replaceAll("-", "")}`;

    const input = {
        username,
        password: "P@ss123",
        email: `${username}@example.com`,
        phone: "0900000000",
    };

    await assert.rejects(
        () => invoke("Register", input),
        (error) => error.code === grpc.status.UNAVAILABLE
    );

    const stored = await pool.query(
        `
      SELECT u.uid, r.rolename AS role,
             task.completed_at, task.attempts, task.last_error
      FROM users AS u
      JOIN roles AS r ON r.roleid = u.roleid
      JOIN customer_provisioning_tasks AS task ON task.uid = u.uid
      WHERE u.username = $1
    `,
        [username]
    );

    assert.equal(stored.rowCount, 1);

    const pending = stored.rows[0];

    assert.equal(pending.role, "CUSTOMER");
    assert.equal(pending.completed_at, null);
    assert.ok(pending.attempts >= 1);
    assert.equal(pending.last_error, "SERVICE_UNAVAILABLE");

    await assert.rejects(
        () => invoke("Login", {
            username,
            password: input.password,
        }),
        (error) => error.code === grpc.status.UNAVAILABLE
    );

    console.log("Registration failure handled: OK");
    console.log("Pending account and task retained: OK");
    console.log("Login blocked until profile is ready: OK");
    console.log("Recovery uid:", pending.uid);
    console.log("Recovery username:", username);
} catch (error) {
    console.error("Customer outage check failed:", error.message);
    process.exitCode = 1;
} finally {
    auth.close();
    await pool.end();
}