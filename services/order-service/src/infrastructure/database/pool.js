import pg from "pg";
import pino from "pino";
import { dbConfig } from "../config/env.js";

const { Pool } = pg;
const logger = pino();

export const pool = new Pool({
    ...dbConfig,
    max: 5,
    connectionTimeoutMillis: 5000,
    idleTimeoutMillis: 30000,
    statement_timeout: 5000,
    lock_timeout: 2000,
});

pool.on("error", (error) => {
    logger.error(
        { code: error.code ?? "POSTGRES_ERROR" },
        "Order database pool error"
    );
});