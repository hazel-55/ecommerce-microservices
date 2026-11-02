import pino from "pino";
import { app } from "./app.js";
import { gatewayConfig } from "./config/env.js";
import { redisClient } from "./infrastructure/redis.js";
import { authClient } from "./infrastructure/grpc/auth.client.js";
import { customerClient } from "./infrastructure/grpc/customer.client.js";
import { productClient } from "./infrastructure/grpc/product.client.js";
import {
    orderClient,
} from "./infrastructure/grpc/order.client.js";
import {
    shipmentClient,
} from "./infrastructure/grpc/shipment.client.js";
const logger = pino();

let server;
let stopping = false;

async function shutdown(signal) {
    if (stopping) {
        return;
    }

    stopping = true;
    logger.info({ signal }, "Gateway stopping");

    const timer = setTimeout(() => {
        logger.error("Gateway shutdown timed out");
        process.exit(1);
    }, 10_000);

    try {
        if (server?.listening) {
            await new Promise((resolve, reject) => {
                server.close((error) => {
                    if (error) {
                        reject(error);
                        return;
                    }

                    resolve();
                });
            });
        }

        authClient.close();
        customerClient.close();
        productClient.close();
        orderClient.close();
        shipmentClient.close();
        if (redisClient.isOpen) {
            await redisClient.close();
        }

        clearTimeout(timer);
        logger.info("Gateway stopped");
    } catch (error) {
        clearTimeout(timer);
        logger.error({
            errorName: error.name,
            code: error.code ?? "UNKNOWN",
            syscall: error.syscall,
        }, "Gateway shutdown failed");
        process.exitCode = 1;
    }
}

async function main() {
    await redisClient.connect();
    await redisClient.ping();

    server = app.listen(gatewayConfig.port, gatewayConfig.host);

    await new Promise((resolve, reject) => {
        server.once("listening", resolve);
        server.once("error", reject);
    });

    server.on("error", () => {
        logger.error("Gateway HTTP server error");
        process.exitCode = 1;
        void shutdown("SERVER_ERROR");
    });

    logger.info({
        host: gatewayConfig.host,
        port: gatewayConfig.port,
    }, "API Gateway started");

    process.once("SIGINT", () => void shutdown("SIGINT"));
    process.once("SIGTERM", () => void shutdown("SIGTERM"));
}

main().catch(async (error) => {
    logger.error({
        errorName: error.name,
        code: error.code ?? "UNKNOWN",
        syscall: error.syscall,
    }, "Gateway startup failed");

    process.exitCode = 1;
    await shutdown("STARTUP_FAILURE");
});