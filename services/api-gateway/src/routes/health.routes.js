import { Router } from "express";
import {
    redisClient,
} from "../infrastructure/redis.js";
import {
    createRedisHealthCheck,
} from "../infrastructure/health/redis.health.js";

export function createHealthRouter({
    redis = redisClient,
    timeoutMs = 1000,
} = {}) {
    const router = Router();

    const checkRedis = createRedisHealthCheck({
        redis,
        timeoutMs,
    });

    router.get("/", async (req, res) => {
        const healthy = await checkRedis();

        res.setHeader("Cache-Control", "no-store");

        res.status(healthy ? 200 : 503).json(
            healthy
                ? {
                    status: "UP",
                    message: "E-commerce API is running",
                }
                : {
                    status: "DOWN",
                    message: "E-commerce API is not ready",
                },
        );
    });

    return router;
}

export const healthRouter = createHealthRouter();