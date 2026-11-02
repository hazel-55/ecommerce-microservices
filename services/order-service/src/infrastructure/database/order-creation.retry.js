import { DomainError } from "../../domain/errors.js";
import {
    creationRetryDelaySeconds,
    validateCreationRetryCode,
} from "../../domain/order-creation-retry.rules.js";

const MAX_ATTEMPTS = 2147483647;

const PENDING_STATES = new Set([
    "PROCESSING",
    "CONFIRM_PENDING",
    "RELEASE_PENDING",
]);

export async function scheduleCreationRetry(
    client,
    task,
    errorCode,
    { now = null } = {},
) {
    validateCreationRetryCode(errorCode);

    if (
        now !== null &&
        (
            !(now instanceof Date) ||
            !Number.isFinite(now.getTime())
        )
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid retry scheduling time",
        );
    }

    if (!PENDING_STATES.has(task.status)) {
        throw new DomainError(
            "CONFLICT",
            "Cannot retry a closed creation task",
        );
    }

    if (
        !Number.isInteger(task.attempts) ||
        task.attempts < 0 ||
        task.attempts > MAX_ATTEMPTS
    ) {
        throw new DomainError(
            "INTERNAL",
            "Invalid stored retry attempts",
        );
    }

    const attempts = Math.min(
        MAX_ATTEMPTS,
        task.attempts + 1,
    );

    const delaySeconds = creationRetryDelaySeconds(
        attempts,
    );

    const updated = await client.query(
        `
        UPDATE order_creation_tasks
        SET
            attempts = $2::integer,
            last_error_code = $3::text,
            next_attempt_at =
                COALESCE(
                    $4::timestamptz,
                    CURRENT_TIMESTAMP
                )
                + ($5::integer * INTERVAL '1 second'),
            updated_at = CURRENT_TIMESTAMP
        WHERE operation_id = $1::uuid
          AND status = $6::text
        RETURNING *
        `,
        [
            task.operation_id,
            attempts,
            errorCode,
            now,
            delaySeconds,
            task.status,
        ],
    );

    if (updated.rowCount !== 1) {
        throw new DomainError(
            "CONFLICT",
            "Creation retry could not be scheduled",
        );
    }

    return updated.rows[0];
}