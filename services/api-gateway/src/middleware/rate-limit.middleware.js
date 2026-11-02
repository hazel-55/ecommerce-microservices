import { createHash } from "node:crypto";
import { rateLimitConfig } from "../config/env.js";
import { HttpError } from "../errors/http-error.js";
import { redisClient } from "../infrastructure/redis.js";

const COUNTER_SCRIPT = `
local count = redis.call('INCR', KEYS[1])
local ttl = redis.call('PTTL', KEYS[1])

if count == 1 or ttl < 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
  ttl = tonumber(ARGV[1])
end

return {count, ttl}
`;

export async function consumeRateLimit(key, { limit, windowSeconds }) {
    if (!redisClient.isReady) {
        throw new HttpError(503, "Rate limiting service unavailable");
    }

    let timer;

    try {
        const result = await Promise.race([
            redisClient.eval(COUNTER_SCRIPT, {
                keys: [key],
                arguments: [String(windowSeconds * 1000)],
            }),
            new Promise((_, reject) => {
                timer = setTimeout(() => {
                    reject(new Error("Redis deadline exceeded"));
                }, 2000);
            }),
        ]);

        const count = Number(result[0]);
        const ttl = Number(result[1]);

        if (!Number.isSafeInteger(count) || count < 1 ||
            !Number.isFinite(ttl) || ttl < 0) {
            throw new Error("Invalid Redis counter response");
        }

        return {
            count,
            allowed: count <= limit,
            remaining: Math.max(0, limit - count),
            retryAfter: Math.max(1, Math.ceil(ttl / 1000)),
        };
    } catch {
        throw new HttpError(503, "Rate limiting service unavailable");
    } finally {
        clearTimeout(timer);
    }
}

export function createRateLimiter({
    scope = "global",
    limit = rateLimitConfig.maxRequests,
    windowSeconds = rateLimitConfig.windowSeconds,
    consume = consumeRateLimit,
} = {}) {
    if (typeof scope !== "string" || scope.length === 0 ||
        !Number.isSafeInteger(limit) || limit < 1 ||
        !Number.isSafeInteger(windowSeconds) || windowSeconds < 1) {
        throw new TypeError("Invalid rate limit configuration");
    }

    return async function rateLimiter(req, res, next) {
        try {
            const identity = req.ip ?? req.socket?.remoteAddress ?? "unknown";
            const digest = createHash("sha256")
                .update(String(identity))
                .digest("hex");

            const key = `gateway:rate:${scope}:${digest}`;
            const result = await consume(key, { limit, windowSeconds });

            res.setHeader("X-RateLimit-Limit", String(limit));
            res.setHeader("X-RateLimit-Remaining", String(result.remaining));

            if (!result.allowed) {
                res.setHeader("Retry-After", String(result.retryAfter));
                return next(new HttpError(429, "Too many requests"));
            }

            return next();
        } catch (error) {
            return next(error);
        }
    };
}