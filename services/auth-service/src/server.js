import grpc from "@grpc/grpc-js";
import pino from "pino";
import { pool } from "./infrastructure/database/pool.js";
import { customerClient } from "./infrastructure/grpc/customer.client.js";
import { createGrpcServer } from "./interfaces/grpc/server.js";
import {
    createProvisioningWorker,
} from "./infrastructure/workers/customer-provisioning.worker.js";

const logger = pino();
const address = process.env.AUTH_GRPC_ADDRESS ?? "127.0.0.1:50051";

let server;
let worker;

try {
    await pool.query("SELECT 1");

    server = createGrpcServer(logger);
    worker = createProvisioningWorker({ logger });

    await new Promise((resolve, reject) => {
        server.bindAsync(
            address,
            grpc.ServerCredentials.createInsecure(),
            (error) => {
                if (error) reject(error);
                else resolve();
            }
        );
    });

    worker.start();

    logger.info({ address }, "Auth gRPC server started");
    logger.info("Customer provisioning worker started");

    let stopping = false;

    function shutdown() {
        if (stopping) return;
        stopping = true;

        logger.info("Stopping Auth Service");

        const timer = setTimeout(() => {
            server.forceShutdown();
        }, 10000);

        timer.unref();

        server.tryShutdown(async () => {
            try {
                await worker.stop();
                customerClient.close();
                await pool.end();

                logger.info("Auth Service stopped");
            } catch {
                logger.error("Failed to stop Auth Service cleanly");
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
        { code: error?.code ?? "UNKNOWN", message: error.message },
        "Auth Service startup failed"
    );

    server?.forceShutdown();
    await worker?.stop();
    customerClient.close();
    await pool.end();
    process.exitCode = 1;
}