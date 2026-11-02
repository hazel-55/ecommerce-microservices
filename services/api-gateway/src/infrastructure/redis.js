import { createClient } from "redis";
import pino from "pino";
import { gatewayConfig } from "../config/env.js";

const logger = pino();

export const redisClient = createClient({
    url: gatewayConfig.redisUrl,
    disableOfflineQueue: true,
    socket: {
        connectTimeout: 5000,
    },
});

redisClient.on("error", (error) => {
    logger.error(
        { code: error?.code ?? "REDIS_ERROR" },
        "Gateway Redis connection error"
    );
});