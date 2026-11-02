import mongoose from "mongoose";
import pino from "pino";
import {
    shipmentConfig,
} from "../config/env.js";

const logger = pino();

mongoose.set("bufferCommands", false);

mongoose.connection.on("error", (error) => {
    logger.error(
        {
            code: error?.code ?? "MONGODB_ERROR",
        },
        "Shipment MongoDB connection error"
    );
});

export async function connectDatabase() {
    await mongoose.connect(
        shipmentConfig.mongodbUri,
        {
            serverSelectionTimeoutMS: 5000,
            connectTimeoutMS: 5000,
            maxPoolSize: 5,
        }
    );

    return mongoose.connection;
}

export async function disconnectDatabase() {
    await mongoose.disconnect();
}