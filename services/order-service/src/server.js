import grpc from "@grpc/grpc-js";
import pino from "pino";
import {
    orderServerConfig,
} from "./infrastructure/config/env.js";
import {
    pool,
} from "./infrastructure/database/pool.js";
import {
    customerClient,
} from "./infrastructure/grpc/customer.client.js";
import {
    productClient,
} from "./infrastructure/grpc/product.client.js";
import {
    shipmentClient,
} from "./infrastructure/grpc/shipment.client.js";
import {
    createGrpcServer,
} from "./interfaces/grpc/server.js";

const logger = pino();

let server;
let bound = false;
let stopping = false;
let startupPromise;
let shutdownPromise;

async function performShutdown(signal) {
    logger.info(
        { signal },
        "Order service stopping",
    );

    const timer = setTimeout(() => {
        server?.forceShutdown();

        logger.error("Order shutdown timed out");
        process.exit(1);
    }, 30_000);

    const failures = [];

    try {
        await startupPromise?.catch(() => { });

        try {
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
        } catch (error) {
            failures.push(error);
            server?.forceShutdown();
        }

        try {
            if (server) {
                await server.waitForIdle();
            }
        } catch (error) {
            failures.push(error);
        }

        for (const client of [
            customerClient,
            productClient,
            shipmentClient,
        ]) {
            try {
                client.close();
            } catch (error) {
                failures.push(error);
            }
        }

        try {
            await pool.end();
        } catch (error) {
            failures.push(error);
        }

        if (failures.length > 0) {
            process.exitCode = 1;

            logger.error(
                {
                    errorName: failures[0]?.name ?? "Error",
                    code: failures[0]?.code ?? "INTERNAL",
                },
                "Order shutdown failed",
            );
        } else {
            logger.info("Order service stopped");
        }
    } finally {
        clearTimeout(timer);
    }
}

function shutdown(signal) {
    if (shutdownPromise) {
        return shutdownPromise;
    }

    stopping = true;
    shutdownPromise = performShutdown(signal);

    return shutdownPromise;
}

process.once("SIGINT", () => {
    void shutdown("SIGINT");
});

process.once("SIGTERM", () => {
    void shutdown("SIGTERM");
});

async function main() {
    const readinessQueries = [
        `
        SELECT
            o.oid,
            o.uid,
            o.discount,
            o.operation_id,
            d.pid,
            d.qty,
            d.unit_price
        FROM orders o
        LEFT JOIN order_details d ON d.oid = o.oid
        LIMIT 0
        `,
        `
        SELECT
            operation_id,
            uid,
            actor_role,
            items,
            status,
            score,
            failure_code,
            next_attempt_at
        FROM order_creation_tasks
        LIMIT 0
        `,
        `
        SELECT
            event_id,
            event_type,
            oid,
            payload,
            published_at,
            next_attempt_at
        FROM outbox_events
        LIMIT 0
        `,
    ];

    for (const query of readinessQueries) {
        if (stopping) {
            return;
        }

        await pool.query(query);
    }

    if (stopping) {
        return;
    }

    server = createGrpcServer(logger);

    await new Promise((resolve, reject) => {
        server.bindAsync(
            orderServerConfig.grpcAddress,
            grpc.ServerCredentials.createInsecure(),
            (error) => {
                if (error) {
                    reject(error);
                    return;
                }

                bound = true;
                resolve();
            },
        );
    });

    if (stopping) {
        return;
    }

    logger.info(
        {
            address: orderServerConfig.grpcAddress,
        },
        "Order gRPC server started",
    );
}

startupPromise = main();

void startupPromise.catch(async (error) => {
    if (stopping) {
        return;
    }

    process.exitCode = 1;

    logger.error(
        {
            errorName: error?.name ?? "Error",
            code: error?.code ?? "INTERNAL",
        },
        "Order startup failed",
    );

    await shutdown("STARTUP_FAILURE");
});