import pg from "pg";
import { dbConfig } from "../config/env.js";

const { Pool } = pg;

export const pool = new Pool({
    ...dbConfig,
    max: 5,
    connectionTimeoutMillis: 5000,
    idleTimeoutMillis: 30000,
});

pool.on("error", (error) => {
    console.error("PostgreSQL pool error:", error.code ?? "UNKNOWN");
});