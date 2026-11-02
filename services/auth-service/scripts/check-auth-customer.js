import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
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

async function expectCode(client, method, request, code, headers) {
    await assert.rejects(
        () => invoke(client, method, request, headers),
        (error) => error.code === code
    );
}

try {
    const customerEnv = dotenv.parse(
        readFileSync(
            new URL("../../customer-service/.env", import.meta.url)
        )
    );

    const gatewayToken = customerEnv.CUSTOMER_GATEWAY_TOKEN;

    assert.ok(
        typeof gatewayToken === "string" && gatewayToken.length >= 32,
        "Missing Customer Gateway token"
    );

    function gatewayMetadata(uid, role) {
        const headers = new grpc.Metadata();

        headers.set("x-service-name", "api-gateway");
        headers.set("x-service-token", gatewayToken);
        headers.set("x-user-id", String(uid));
        headers.set("x-user-role", role);

        return headers;
    }

    const username = `integration_${randomUUID().replaceAll("-", "")}`;

    const input = {
        username,
        password: "P@ss123",
        email: `${username}@example.com`,
        phone: "0900000000",
    };

    const registered = await invoke(auth, "Register", input);

    assert.equal(registered.message, "User registered successfully");
    assert.ok(registered.uid > 0);

    const loggedIn = await invoke(auth, "Login", {
        username,
        password: input.password,
    });

    assert.equal(loggedIn.tokenType, "Bearer");
    assert.equal(loggedIn.expiresIn, 3600);

    const payload = verifyAccessToken(loggedIn.accessToken);

    assert.equal(payload.sub, String(registered.uid));
    assert.equal(payload.role, "CUSTOMER");

    await expectCode(
        auth,
        "Register",
        input,
        grpc.status.ALREADY_EXISTS
    );

    await expectCode(
        auth,
        "Register",
        { ...input, username: `${username}_other` },
        grpc.status.ALREADY_EXISTS
    );

    await expectCode(
        auth,
        "Login",
        { username, password: "WrongPassword" },
        grpc.status.UNAUTHENTICATED
    );

    const headers = gatewayMetadata(payload.sub, payload.role);

    async function getOwnProfile() {
        return invoke(
            customer,
            "GetCustomer",
            { uid: registered.uid },
            headers
        );
    }

    const profile = await getOwnProfile();

    assert.deepEqual(profile, {
        uid: registered.uid,
        fullname: username,
        mid: 1,
        mname: "Standard",
        score: 0,
    });

    const updated = await invoke(
        customer,
        "UpdateCustomer",
        { uid: registered.uid, fullname: "John Doe" },
        headers
    );

    assert.equal(updated.message, "Customer updated successfully");
    assert.equal(updated.uid, registered.uid);

    const afterUpdate = await getOwnProfile();

    assert.equal(afterUpdate.fullname, "John Doe");
    assert.equal(afterUpdate.mid, 1);

    await expectCode(
        customer,
        "UpdateCustomer",
        { uid: registered.uid, fullname: "John Doe", mid: 2 },
        grpc.status.PERMISSION_DENIED,
        headers
    );

    assert.equal((await getOwnProfile()).mid, 1);

    const otherUid = registered.uid === 1 ? 2 : 1;

    await expectCode(
        customer,
        "GetCustomer",
        { uid: registered.uid },
        grpc.status.PERMISSION_DENIED,
        gatewayMetadata(otherUid, "CUSTOMER")
    );

    const stored = await pool.query(
        `
      SELECT u.password, r.rolename AS role, task.completed_at
      FROM users AS u
      JOIN roles AS r ON r.roleid = u.roleid
      JOIN customer_provisioning_tasks AS task ON task.uid = u.uid
      WHERE u.uid = $1
    `,
        [registered.uid]
    );

    assert.equal(stored.rowCount, 1);
    assert.notEqual(stored.rows[0].password, input.password);
    assert.equal(stored.rows[0].role, "CUSTOMER");
    assert.ok(stored.rows[0].completed_at);

    console.log("Live registration and login: OK");
    console.log("Customer profile creation and update: OK");
    console.log("Duplicate and authorization checks: OK");
    console.log("Stored hash and completed provisioning: OK");
    console.log("Test uid:", registered.uid);
    console.log("Test username:", username);
} catch (error) {
    console.error("Integration check failed:", error.message);
    process.exitCode = 1;
} finally {
    auth.close();
    customer.close();
    await pool.end();
}