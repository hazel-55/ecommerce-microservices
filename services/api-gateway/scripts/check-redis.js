import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { redisClient } from "../src/infrastructure/redis.js";

const key = `gateway:check:${randomUUID()}`;

try {
    await redisClient.connect();

    assert.equal(await redisClient.ping(), "PONG");

    const value = JSON.stringify({
        uid: 12,
        fullname: "Redis test",
    });

    await redisClient.set(key, value, { EX: 30 });

    const cached = await redisClient.get(key);
    assert.equal(cached, value);

    const ttl = await redisClient.ttl(key);
    assert.ok(ttl > 0 && ttl <= 30);

    await redisClient.del(key);
    assert.equal(await redisClient.get(key), null);

    console.log("Gateway Redis connection: OK");
    console.log("Cache write and read: OK");
    console.log("Cache TTL: OK");
    console.log("Test key removed: OK");
} catch (error) {
    console.error("Redis check failed:", error.code ?? error.message);
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