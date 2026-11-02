import mongoose from "mongoose";
import { productConfig } from "../config/env.js";

mongoose.set("bufferCommands", false);

export async function connectDatabase() {
    await mongoose.connect(productConfig.mongodbUri, {
        serverSelectionTimeoutMS: 5000,
        connectTimeoutMS: 5000,
        autoIndex: false,
        autoCreate: false,
    });

    return mongoose.connection;
}

export async function disconnectDatabase() {
    await mongoose.disconnect();
}