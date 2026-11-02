import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { redisClient } from "../src/infrastructure/redis.js";
import {
    consumeRateLimit,
    createRateLimiter,
} from "../src/middleware/rate-limit.middleware.js";

const key = `gateway:rate:check:${randomUUID()}`;

try {
    await redisClient.connect();

    const options = {
        limit: 3,
        windowSeconds: 60,
    };

    const results = await Promise.all(
        Array.from({ length: 20 }, () => consumeRateLimit(key, options)),
    );

    assert.deepEqual(
        results.map((result) => result.count).sort((a, b) => a - b),
        Array.from({ length: 20 }, (_, index) => index + 1),
    );
    console.log("Redis atomic rate counter: OK");

    assert.equal(results.filter((result) => result.allowed).length, 3);
    assert.equal(results.filter((result) => !result.allowed).length, 17);
    console.log("Concurrent request limit: OK");

    const ttl = await redisClient.pTTL(key);
    assert.ok(ttl > 0 && ttl <= 60_000);
    console.log("Rate window TTL: OK");

    const middleware = createRateLimiter({
        scope: "check",
        ...options,
        consume: (_requestKey, settings) => consumeRateLimit(key, settings),
    });

    const headers = new Map();
    let nextCalled = false;
    let receivedError;

    await middleware(
        { ip: "127.0.0.1" },
        {
            setHeader(name, value) {
                headers.set(name, value);
            },
        },
        (error) => {
            nextCalled = true;
            receivedError = error;
        },
    );

    assert.equal(nextCalled, true);
    assert.equal(receivedError?.status, 429);
    assert.equal(headers.get("X-RateLimit-Limit"), "3");
    assert.equal(headers.get("X-RateLimit-Remaining"), "0");
    assert.ok(Number(headers.get("Retry-After")) >= 1);
    console.log("429 and Retry-After: OK");
} catch (error) {
    console.error("Rate limiting check failed:", error.message);
    process.exitCode = 1;
} finally {
    try {
        if (redisClient.isReady) {
            await redisClient.del(key);
        }
    } finally {
        if (redisClient.isOpen) {
            await redisClient.close();
        }
    }
}