import grpc from "@grpc/grpc-js";
import pino from "pino";

import {
    shipmentServerConfig,
} from "./infrastructure/config/env.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "./infrastructure/database/mongoose.js";
import {
    prepareShipmentDatabase,
} from "./infrastructure/runtime/consumer-readiness.js";
import {
    createGrpcServer,
} from "./interfaces/grpc/server.js";

const logger = pino();

let server;
let bound = false;
let stopping = false;

let startupPromise = Promise.resolve();
let shutdownPromise;

function errorFields(error) {
    return {
        errorName: error?.name ?? "Error",
        code: error?.code ?? "INTERNAL",
    };
}

function shutdown(signal) {
    if (shutdownPromise) {
        return shutdownPromise;
    }

    stopping = true;

    logger.info(
        { signal },
        "Shipment service stopping"
    );

    shutdownPromise = (async () => {
        const timer = setTimeout(() => {
            server?.forceShutdown();

            logger.error("Shipment shutdown timed out");
            process.exit(1);
        }, 30_000);

        try {
            // Avoid closing resources while startup is using them.
            await startupPromise.catch(() => { });

            if (server && bound) {
                await new Promise((resolve, reject) => {
                    server.tryShutdown((error) => {
                        if (error) {
                            reject(error);
                            return;
                        }

                        resolve();
                    });
                });
            } else {
                server?.forceShutdown();
            }

            // Active RPCs have drained before MongoDB is closed.
            await disconnectDatabase();

            logger.info("Shipment service stopped");
        } catch (error) {
            server?.forceShutdown();

            logger.error(
                errorFields(error),
                "Shipment shutdown failed"
            );

            process.exit(1);
        } finally {
            clearTimeout(timer);

            process.off("SIGINT", onSigint);
            process.off("SIGTERM", onSigterm);
        }
    })();

    return shutdownPromise;
}

function onSigint() {
    void shutdown("SIGINT");
}

function onSigterm() {
    void shutdown("SIGTERM");
}

async function main() {
    const connection = await connectDatabase();

    if (stopping) {
        return;
    }

    await prepareShipmentDatabase(connection);

    if (stopping) {
        return;
    }

    server = createGrpcServer(logger);

    await new Promise((resolve, reject) => {
        server.bindAsync(
            shipmentServerConfig.grpcAddress,
            grpc.ServerCredentials.createInsecure(),
            (error) => {
                if (error) {
                    reject(error);
                    return;
                }

                bound = true;
                resolve();
            }
        );
    });

    if (stopping) {
        return;
    }

    logger.info(
        {
            address: shipmentServerConfig.grpcAddress,
        },
        "Shipment gRPC server started"
    );
}

process.once("SIGINT", onSigint);
process.once("SIGTERM", onSigterm);

startupPromise = main();

void startupPromise.catch((error) => {
    logger.error(
        errorFields(error),
        "Shipment startup failed"
    );

    process.exitCode = 1;
    void shutdown("STARTUP_FAILURE");
});