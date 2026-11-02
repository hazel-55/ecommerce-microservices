import assert from "node:assert/strict";
import { gatewayConfig } from "../src/config/env.js";
import { redisClient } from "../src/infrastructure/redis.js";
import {
    customerCacheKeys,
    CUSTOMER_CACHE_TTL_SECONDS,
} from "../src/infrastructure/cache/customer.cache.js";

const baseUrl = `http://127.0.0.1:${gatewayConfig.port}`;
const uid = 13;
const keys = customerCacheKeys(uid);

let token;
let originalFullname;
let restoreRequired = false;

async function request(path, {
    method = "GET",
    accessToken = token,
    body,
} = {}) {
    const headers = {};

    if (accessToken) {
        headers.Authorization = `Bearer ${accessToken}`;
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
        headers: response.headers,
        body: await response.json(),
    };
}

try {
    await redisClient.connect();

    const login = await request("/api/auth/login", {
        method: "POST",
        body: {
            username: "http_8678dd4139734fe18d85dee359c76237",
            password: "P@ss123",
        },
    });

    assert.equal(login.status, 200);
    assert.equal(typeof login.body.accessToken, "string");
    token = login.body.accessToken;

    // Chỉ xóa cache dữ liệu của tài khoản kiểm tra.
    await redisClient.del(keys.data);

    const first = await request(`/api/customers/${uid}`);

    assert.equal(first.status, 200);
    assert.equal(first.headers.get("X-Cache"), "MISS");
    assert.equal(first.headers.get("Cache-Control"), "no-store");

    originalFullname = first.body.fullname;

    assert.equal(typeof originalFullname, "string");
    assert.ok(
        originalFullname.trim().length > 0,
        "Original fullname is empty; stop before changing the profile",
    );

    const cached = await redisClient.get(keys.data);
    assert.notEqual(cached, null);
    const cachedCustomer = JSON.parse(cached);

    assert.deepEqual(
        Object.keys(first.body).sort(),
        ["fullname", "mid", "uid"],
    );

    assert.deepEqual(
        {
            uid: cachedCustomer.uid,
            fullname: cachedCustomer.fullname,
            mid: cachedCustomer.mid,
        },
        first.body,
    );

    // Dữ liệu nội bộ vẫn giữ thông tin membership.
    assert.equal(typeof cachedCustomer.mname, "string");
    assert.ok(Number.isInteger(cachedCustomer.score));

    const ttl = await redisClient.ttl(keys.data);
    assert.ok(ttl > 0 && ttl <= CUSTOMER_CACHE_TTL_SECONDS);

    console.log("GET cache MISS, Redis data and TTL: OK");

    const second = await request(`/api/customers/${uid}`);

    assert.equal(second.status, 200);
    assert.equal(second.headers.get("X-Cache"), "HIT");
    assert.deepEqual(second.body, first.body);

    console.log("Repeated GET cache HIT: OK");

    const otherLogin = await request("/api/auth/login", {
        method: "POST",
        accessToken: undefined,
        body: {
            username: "recovery_019cd44745364961bad7a1b684ce8a96",
            password: "P@ss123",
        },
    });

    assert.equal(otherLogin.status, 200);
    assert.equal(typeof otherLogin.body.accessToken, "string");

    const forbidden = await request(`/api/customers/${uid}`, {
        accessToken: otherLogin.body.accessToken,
    });

    assert.equal(forbidden.status, 403);
    assert.equal(forbidden.headers.get("X-Cache"), null);

    console.log("Cached profile remains protected: OK");

    const newFullname = originalFullname === "HTTP Cache Test"
        ? "HTTP Cache Test Two"
        : "HTTP Cache Test";

    // Chuẩn bị khôi phục kể cả khi request cập nhật bị lỗi giữa chừng.
    restoreRequired = true;

    const update = await request(`/api/customers/${uid}`, {
        method: "PUT",
        body: { fullname: newFullname },
    });

    assert.equal(update.status, 200);
    assert.equal(update.body.uid, uid);
    assert.equal(typeof update.body.message, "string");
    assert.deepEqual(
        Object.keys(update.body).sort(),
        ["message", "uid"],
    );
    assert.equal(await redisClient.get(keys.data), null);

    console.log("HTTP PUT success and cache invalidation: OK");

    const refreshed = await request(`/api/customers/${uid}`);

    assert.equal(refreshed.status, 200);
    assert.equal(refreshed.headers.get("X-Cache"), "MISS");
    assert.equal(refreshed.body.fullname, newFullname);
    assert.equal(refreshed.body.mid, first.body.mid);
    assert.deepEqual(
        Object.keys(refreshed.body).sort(),
        ["fullname", "mid", "uid"],
    );

    const refreshedCache = JSON.parse(
        await redisClient.get(keys.data),
    );

    assert.equal(refreshedCache.mname, cachedCustomer.mname);
    assert.equal(refreshedCache.score, cachedCustomer.score);


    console.log("GET after PUT returns updated profile: OK");
} catch (error) {
    console.error("Customer cache HTTP check failed:", error.message);
    process.exitCode = 1;
} finally {
    try {
        if (restoreRequired) {
            const restored = await request(`/api/customers/${uid}`, {
                method: "PUT",
                body: { fullname: originalFullname },
            });

            assert.equal(restored.status, 200);

            const profile = await request(`/api/customers/${uid}`);
            assert.equal(profile.status, 200);
            assert.equal(profile.body.fullname, originalFullname);

            console.log("Original fullname restored: OK");
        }
    } catch (error) {
        console.error(
            "Profile restoration failed; check uid 13:",
            error.message,
        );
        process.exitCode = 1;
    } finally {
        if (redisClient.isOpen) {
            await redisClient.close();
        }
    }
}