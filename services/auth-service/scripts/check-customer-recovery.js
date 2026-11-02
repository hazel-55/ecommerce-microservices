import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { pool } from "../src/infrastructure/database/pool.js";
import {
    verifyAccessToken,
} from "../src/infrastructure/security/token.js";

const definition = loadSync(
    [
        fileURLToPath(new URL("../../../proto/auth.proto", import.meta.url)),
        fileURLToPath(new URL("../../../proto/customer.proto", import.meta.url)),
    ],
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    }
);

const proto = grpc.loadPackageDefinition(definition).ecommerce;

const auth = new proto.auth.v1.AuthService(
    "127.0.0.1:50051",
    grpc.credentials.createInsecure()
);

const customer = new proto.customer.v1.CustomerService(
    "127.0.0.1:50052",
    grpc.credentials.createInsecure()
);

function invoke(client, method, request, headers = new grpc.Metadata()) {
    return new Promise((resolve, reject) => {
        client[method](
            request,
            headers,
            { deadline: Date.now() + 10000 },
            (error, response) => {
                if (error) reject(error);
                else resolve(response);
            }
        );
    });
}

try {
    const username = process.argv[2];
    const expectedUid = Number(process.argv[3]);

    assert.ok(typeof username === "string" && username.length > 0);
    assert.ok(Number.isInteger(expectedUid) && expectedUid > 0);

    const customerEnv = dotenv.parse(
        readFileSync(new URL("../../customer-service/.env", import.meta.url))
    );

    assert.ok(
        typeof customerEnv.CUSTOMER_GATEWAY_TOKEN === "string" &&
        customerEnv.CUSTOMER_GATEWAY_TOKEN.length >= 32
    );

    const deadline = Date.now() + 120000;
    let nextProgress = Date.now() + 10000;
    let recovered;

    console.log("Waiting for background provisioning...");

    while (Date.now() < deadline) {
        const result = await pool.query(
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

        assert.equal(result.rowCount, 1);
        assert.equal(result.rows[0].uid, expectedUid);

        if (result.rows[0].completed_at) {
            recovered = result.rows[0];
            break;
        }

        if (Date.now() >= nextProgress) {
            console.log("Still waiting for the Auth worker...");
            nextProgress = Date.now() + 10000;
        }

        await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    assert.ok(
        recovered,
        "Provisioning did not complete within 120 seconds"
    );

    assert.equal(recovered.role, "CUSTOMER");
    assert.ok(recovered.attempts >= 2);
    assert.equal(recovered.last_error, null);

    const loggedIn = await invoke(auth, "Login", {
        username,
        password: "P@ss123",
    });

    const payload = verifyAccessToken(loggedIn.accessToken);

    assert.equal(payload.sub, String(expectedUid));
    assert.equal(payload.role, "CUSTOMER");

    const headers = new grpc.Metadata();

    headers.set("x-service-name", "api-gateway");
    headers.set("x-service-token", customerEnv.CUSTOMER_GATEWAY_TOKEN);
    headers.set("x-user-id", payload.sub);
    headers.set("x-user-role", payload.role);

    const profile = await invoke(
        customer,
        "GetCustomer",
        { uid: expectedUid },
        headers
    );

    assert.deepEqual(profile, {
        uid: expectedUid,
        fullname: username,
        mid: 1,
        mname: "Standard",
        score: 0,
    });

    console.log("Background Customer recovery: OK");
    console.log("Same account uid preserved: OK");
    console.log("Login and recovered profile: OK");
    console.log("Recovery uid:", expectedUid);
} catch (error) {
    console.error("Recovery check failed:", error.message);
    process.exitCode = 1;
} finally {
    auth.close();
    customer.close();
    await pool.end();
}