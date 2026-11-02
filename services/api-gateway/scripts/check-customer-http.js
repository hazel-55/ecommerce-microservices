import assert from "node:assert/strict";
import { gatewayConfig } from "../src/config/env.js";

const baseUrl = `http://127.0.0.1:${gatewayConfig.port}`;

async function request(path, {
    method = "GET",
    token,
    body,
} = {}) {
    const headers = {};

    if (token) {
        headers.Authorization = `Bearer ${token}`;
    }

    if (body !== undefined) {
        headers["Content-Type"] = "application/json";
    }

    const response = await fetch(`${baseUrl}${path}`, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(15_000),
    });

    if (response.status === 429) {
        throw new Error(
            `Rate limit reached. Retry after ${response.headers.get("Retry-After") ?? "60"
            } seconds.`,
        );
    }

    return {
        status: response.status,
        body: await response.json(),
    };
}

try {
    const unauthenticated = await request("/api/customers/13");
    assert.equal(unauthenticated.status, 401);

    console.log("Customer route requires JWT: OK");

    const login = await request("/api/auth/login", {
        method: "POST",
        body: {
            username: "http_8678dd4139734fe18d85dee359c76237",
            password: "P@ss123",
        },
    });

    assert.equal(login.status, 200);
    assert.equal(typeof login.body.accessToken, "string");

    const token = login.body.accessToken;

    const ownProfile = await request("/api/customers/13", { token });

    assert.equal(ownProfile.status, 200);
    assert.equal(ownProfile.body.uid, 13);
    assert.equal(typeof ownProfile.body.fullname, "string");
    assert.ok(Number.isInteger(ownProfile.body.mid));

    assert.deepEqual(
        Object.keys(ownProfile.body).sort(),
        ["fullname", "mid", "uid"],
    );

    console.log("HTTP own customer profile and response: OK");

    const forbiddenRead = await request("/api/customers/12", { token });
    assert.equal(forbiddenRead.status, 403);

    const forbiddenUpdate = await request("/api/customers/12", {
        method: "PUT",
        token,
        body: { fullname: "Must not be applied" },
    });
    assert.equal(forbiddenUpdate.status, 403);

    console.log("Cross-customer GET and PUT forbidden: OK");

    const invalidId = await request("/api/customers/0", { token });
    assert.equal(invalidId.status, 400);

    const invalidUpdate = await request("/api/customers/13", {
        method: "PUT",
        token,
        body: { fullname: 123 },
    });
    assert.equal(invalidUpdate.status, 400);

    console.log("Customer ID and PUT input validation: OK");
} catch (error) {
    console.error("Customer HTTP check failed:", error.message);
    process.exitCode = 1;
}