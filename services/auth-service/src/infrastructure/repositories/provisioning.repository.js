import { pool } from "../database/pool.js";

export function createProvisioningRepository(database = pool) {
    async function findByUid(uid) {
        const result = await database.query(
            "SELECT * FROM customer_provisioning_tasks WHERE uid = $1",
            [uid]
        );

        return result.rows[0] ?? null;
    }

    async function findPending(limit = 10) {
        const result = await database.query(
            `
        SELECT *
        FROM customer_provisioning_tasks
        WHERE completed_at IS NULL
          AND next_attempt_at <= CURRENT_TIMESTAMP
        ORDER BY next_attempt_at, created_at
        LIMIT $1
      `,
            [limit]
        );

        return result.rows;
    }

    async function markFailed(uid, errorCode) {
        const result = await database.query(
            `
        UPDATE customer_provisioning_tasks
        SET attempts = attempts + 1,
            last_error = $2,
            next_attempt_at = CURRENT_TIMESTAMP + INTERVAL '5 seconds'
        WHERE uid = $1 AND completed_at IS NULL
        RETURNING *
      `,
            [uid, errorCode]
        );

        return result.rows[0] ?? null;
    }

    async function markCompleted(uid) {
        const result = await database.query(
            `
        UPDATE customer_provisioning_tasks
        SET completed_at = CURRENT_TIMESTAMP,
            attempts = attempts + 1,
            last_error = NULL
        WHERE uid = $1 AND completed_at IS NULL
        RETURNING *
      `,
            [uid]
        );

        return result.rows[0] ?? await findByUid(uid);
    }

    return { findByUid, findPending, markFailed, markCompleted };
}

export const provisioningRepository = createProvisioningRepository();