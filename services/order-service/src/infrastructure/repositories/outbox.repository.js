import { pool } from "../database/pool.js";
import { DomainError } from "../../domain/errors.js";

export function createOutboxRepository({
    database = pool,
} = {}) {
    return {
        async listDueEventIds({ limit = 20 } = {}) {
            if (
                !Number.isInteger(limit) ||
                limit < 1 ||
                limit > 100
            ) {
                throw new DomainError(
                    "INVALID_ARGUMENT",
                    "Outbox batch limit must be between 1 and 100"
                );
            }

            const result = await database.query(
                `
                SELECT e.event_id
                FROM outbox_events e
                JOIN orders o
                    ON o.oid = e.oid
                JOIN order_creation_tasks t
                    ON t.operation_id = o.operation_id
                   AND t.uid = o.uid
                WHERE e.published_at IS NULL
                  AND e.event_type = 'OrderCreated'
                  AND e.next_attempt_at <= CURRENT_TIMESTAMP
                  AND t.status = 'COMPLETED'
                  AND e.event_id = t.operation_id
                ORDER BY
                    e.next_attempt_at,
                    e.created_at,
                    e.event_id
                LIMIT $1
                `,
                [limit]
            );

            return result.rows.map((row) => row.event_id);
        },
    };
}

export const outboxRepository = createOutboxRepository();