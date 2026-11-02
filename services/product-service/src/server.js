import * as grpc from "@grpc/grpc-js";
import pino from "pino";
import { productConfig } from "./infrastructure/config/env.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "./infrastructure/database/mongoose.js";
import { ProductModel } from "./infrastructure/database/models/product.model.js";
import { CounterModel } from "./infrastructure/database/models/counter.model.js";
import { createGrpcServer } from "./interfaces/grpc/server.js";
import {
    StockReservationModel,
} from "./infrastructure/database/models/stock-reservation.model.js";
const logger = pino();

let server;
let bound = false;
let stopping = false;

async function ensureCollection(model) {
    try {
        await model.createCollection();
    } catch (error) {
        if (error.code !== 48) {
            throw error;
        }
    }
}

async function shutdown(signal) {
    if (stopping) {
        return;
    }

    stopping = true;
    logger.info({ signal }, "Product service stopping");

    const timer = setTimeout(() => {
        server?.forceShutdown();
        logger.error("Product shutdown timed out");
        process.exit(1);
    }, 10_000);

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

        await disconnectDatabase();

        clearTimeout(timer);
        logger.info("Product service stopped");
    } catch (error) {
        server?.forceShutdown();
        clearTimeout(timer);

        logger.error({
            errorName: error.name,
            code: error.code ?? "UNKNOWN",
        }, "Product shutdown failed");

        process.exitCode = 1;

        await disconnectDatabase().catch(() => { });
    }
}

async function main() {
    const connection = await connectDatabase();

    await connection.db.command({ ping: 1 }, { maxTimeMS: 5000 });

    await ensureCollection(ProductModel);
    await ensureCollection(CounterModel);
    await ensureCollection(StockReservationModel);

    await ProductModel.createIndexes();
    await StockReservationModel.createIndexes();

    server = createGrpcServer(logger);

    await new Promise((resolve, reject) => {
        server.bindAsync(
            productConfig.grpcAddress,
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

    logger.info({
        address: productConfig.grpcAddress,
    }, "Product gRPC server started");

    process.once("SIGINT", () => void shutdown("SIGINT"));
    process.once("SIGTERM", () => void shutdown("SIGTERM"));
}

main().catch(async (error) => {
    logger.error({
        errorName: error.name,
        code: error.code ?? "UNKNOWN",
    }, "Product startup failed");

    process.exitCode = 1;
    await shutdown("STARTUP_FAILURE");
});