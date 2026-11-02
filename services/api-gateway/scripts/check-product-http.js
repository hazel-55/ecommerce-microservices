import assert from "node:assert/strict";
import { gatewayConfig } from "../src/config/env.js";

const baseUrl =
    `http://${gatewayConfig.host}:${gatewayConfig.port}`;

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
        body: body === undefined
            ? undefined
            : JSON.stringify(body),
        signal: AbortSignal.timeout(15000),
    });

    return {
        status: response.status,
        data: await response.json(),
    };
}

function assertProduct(product) {
    assert.deepEqual(
        Object.keys(product).sort(),
        ["pid", "pname", "price", "quantity"].sort()
    );

    assert.ok(Number.isInteger(product.pid));
    assert.ok(product.pid > 0);
    assert.equal(typeof product.pname, "string");
    assert.equal(typeof product.price, "number");
    assert.ok(Number.isFinite(product.price));
    assert.ok(product.price >= 0);
    assert.ok(Number.isInteger(product.quantity));
    assert.ok(product.quantity >= 0);
}

async function main() {
    for (const path of [
        "/api/products",
        "/api/products/1",
    ]) {
        const result = await request(path);
        assert.equal(result.status, 401);
    }

    console.log("Product routes require JWT: OK");

    const login = await request("/api/auth/login", {
        method: "POST",
        body: {
            username: "http_8678dd4139734fe18d85dee359c76237",
            password: "P@ss123",
        },
    });

    assert.equal(login.status, 200);
    assert.equal(typeof login.data.accessToken, "string");

    const token = login.data.accessToken;

    const list = await request("/api/products", { token });

    assert.equal(list.status, 200);
    assert.deepEqual(Object.keys(list.data), ["items"]);
    assert.ok(Array.isArray(list.data.items));

    for (const product of list.data.items) {
        assertProduct(product);
    }

    if (list.data.items.length > 0) {
        const pid = list.data.items[0].pid;

        const detail = await request(
            `/api/products/${pid}`,
            { token }
        );

        assert.equal(detail.status, 200);
        assertProduct(detail.data);
        assert.equal(detail.data.pid, pid);

        console.log("HTTP existing product detail: OK");
    } else {
        console.log(
            "HTTP existing product detail: SKIPPED (no products)"
        );
    }

    console.log("HTTP product list and response fields: OK");

    for (const pid of [
        "0",
        "abc",
        "2147483648",
        "01",
    ]) {
        const result = await request(
            `/api/products/${pid}`,
            { token }
        );

        assert.equal(result.status, 400);
    }

    console.log("HTTP product ID validation: OK");

    const productBody = {
        pname: "Must not be created",
        price: 25,
        quantity: 1,
        role: "ADMIN",
    };

    const create = await request("/api/products", {
        method: "POST",
        token,
        body: productBody,
    });

    assert.equal(create.status, 403);

    const update = await request("/api/products/1", {
        method: "PUT",
        token,
        body: productBody,
    });

    assert.equal(update.status, 403);

    const remove = await request("/api/products/1", {
        method: "DELETE",
        token,
    });

    assert.equal(remove.status, 403);

    console.log("Customer product mutations forbidden: OK");

    const existingIds = new Set(
        list.data.items.map((product) => product.pid)
    );

    let missingPid = 2147483647;

    while (existingIds.has(missingPid)) {
        missingPid--;
    }

    assert.ok(missingPid > 0);

    const missing = await request(
        `/api/products/${missingPid}`,
        { token }
    );

    assert.equal(missing.status, 404);
    assert.equal(missing.data.message, "Product not found");

    console.log("HTTP product NOT_FOUND mapping: OK");
}

main().catch((error) => {
    console.error(
        "Product HTTP check failed:",
        error.message
    );

    process.exitCode = 1;
});