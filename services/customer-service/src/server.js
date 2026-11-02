import grpc from "@grpc/grpc-js";
import pino from "pino";
import { pool } from "./infrastructure/database/pool.js";
import { createGrpcServer } from "./interfaces/grpc/server.js";

const logger = pino();
const address =
    process.env.CUSTOMER_GRPC_ADDRESS ?? "127.0.0.1:50052";

let server;

try {
    await pool.query("SELECT 1");

    server = createGrpcServer(logger);

    await new Promise((resolve, reject) => {
        server.bindAsync(
            address,
            grpc.ServerCredentials.createInsecure(),
            (error) => {
                if (error) {
                    reject(error);
                } else {
                    resolve();
                }
            }
        );
    });

    logger.info({ address }, "Customer gRPC server started");

    let stopping = false;

    function shutdown() {
        if (stopping) return;
        stopping = true;

        logger.info("Stopping Customer Service");

        const timer = setTimeout(() => {
            server.forceShutdown();
        }, 10000);

        timer.unref();

        server.tryShutdown(async () => {
            try {
                await pool.end();
                logger.info("Customer Service stopped");
            } catch {
                logger.error("Failed to close database pool");
                process.exitCode = 1;
            } finally {
                clearTimeout(timer);
            }
        });
    }

    process.once("SIGINT", shutdown);
    process.once("SIGTERM", shutdown);
} catch (error) {
    logger.error(
        { code: error.code ?? "UNKNOWN", message: error.message },
        "Customer Service startup failed"
    );

    server?.forceShutdown();
    await pool.end();
    process.exitCode = 1;
}