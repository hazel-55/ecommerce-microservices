import { pool } from "../database/pool.js";
import { DomainError } from "../../domain/errors.js";
import {
    validateCreationOperationId,
    normalizeCreationTaskInput,
    assertSameCreationTask,
} from "../../domain/order-creation.rules.js";

export function createOrderCreationRepository({
    database = pool,
} = {}) {
    async function findByOperationId(operationId) {
        validateCreationOperationId(operationId);

        const result = await database.query(
            `
            SELECT *
            FROM order_creation_tasks
            WHERE operation_id = $1
            `,
            [operationId]
        );

        return result.rows[0] ?? null;
    }

    return {
        findByOperationId,
        async listDueOperationIds({ limit = 20 } = {}) {
            if (
                !Number.isInteger(limit) ||
                limit < 1 ||
                limit > 100
            ) {
                throw new DomainError(
                    "INVALID_ARGUMENT",
                    "Pending task limit must be between 1 and 100",
                );
            }

            const result = await database.query(
                `
                SELECT operation_id
                FROM order_creation_tasks
                WHERE status IN (
                    'PROCESSING',
                    'CONFIRM_PENDING',
                    'RELEASE_PENDING'
                )
                  AND next_attempt_at <= CURRENT_TIMESTAMP
                ORDER BY
                    next_attempt_at ASC,
                    created_at ASC,
                    operation_id ASC
                LIMIT $1::integer
                `,
                [limit],
            );

            return result.rows.map(
                (row) => row.operation_id,
            );
        },

        async createTask(input, { nextAttemptAt = null } = {}) {
            const request = normalizeCreationTaskInput(input);

            if (
                nextAttemptAt !== null &&
                (
                    !(nextAttemptAt instanceof Date) ||
                    !Number.isFinite(nextAttemptAt.getTime())
                )
            ) {
                throw new DomainError(
                    "INVALID_ARGUMENT",
                    "Invalid next attempt time"
                );
            }

            const inserted = await database.query(
                `
                INSERT INTO order_creation_tasks (
                    operation_id,
                    uid,
                    actor_role,
                    items,
                    next_attempt_at
                )
                VALUES (
                    $1,
                    $2,
                    $3,
                    $4::jsonb,
                    COALESCE($5::timestamptz, CURRENT_TIMESTAMP)
                )
                ON CONFLICT (operation_id) DO NOTHING
                RETURNING *
                `,
                [
                    request.operationId,
                    request.uid,
                    request.actorRole,
                    JSON.stringify(request.items),
                    nextAttemptAt,
                ]
            );

            if (inserted.rowCount === 1) {
                return inserted.rows[0];
            }

            const existing = await findByOperationId(
                request.operationId
            );

            if (!existing) {
                throw new DomainError(
                    "SERVICE_UNAVAILABLE",
                    "Creation task could not be loaded"
                );
            }

            assertSameCreationTask(existing, request);

            return existing;
        },
    };
}

export const orderCreationRepository =
    createOrderCreationRepository();