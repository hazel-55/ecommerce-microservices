import { DomainError } from "../../domain/errors.js";
import { normalizeOrderInput } from "../../domain/order.rules.js";
import { findStoredOrder } from "./order-creation.persistence.js";

const PERMANENT_FAILURES = new Set([
    "INVALID_ARGUMENT",
    "NOT_FOUND",
    "CONFLICT",
    "FORBIDDEN",
]);

function assertAcknowledgment(task, reservation) {
    if (
        !reservation ||
        reservation.operationId !== task.operation_id ||
        !Array.isArray(reservation.items)
    ) {
        throw new DomainError(
            "CONFLICT",
            "Reservation acknowledgment does not match task"
        );
    }

    if (reservation.uid !== task.uid) {
        throw new DomainError(
            "FORBIDDEN",
            "Reservation owner does not match task"
        );
    }
}

function pricedItems(items) {
    const normalized = normalizeOrderInput({ items });

    return normalized.items.map((item, index) => {
        const price = items[index].unit_price;

        if (
            typeof price !== "string" ||
            !/^(0|[1-9]\d{0,7})\.\d{2}$/.test(price)
        ) {
            throw new DomainError(
                "INVALID_ARGUMENT",
                "Invalid acknowledgment price"
            );
        }

        return {
            ...item,
            unit_price: price,
        };
    }).sort((first, second) => first.pid - second.pid);
}

async function changeStatus(
    client,
    task,
    status,
    failureCode,
    nextAttemptAt = null,
) {
    const result = await client.query(
        `
        UPDATE order_creation_tasks
        SET
            status = $2::text,
            failure_code = $3::text,
            last_error_code = NULL,
            next_attempt_at = CASE
                WHEN $2::text = 'RELEASE_PENDING'
                    THEN COALESCE(
                        $4::timestamptz,
                        CURRENT_TIMESTAMP
                    )
                ELSE next_attempt_at
            END,
            updated_at = CURRENT_TIMESTAMP
        WHERE operation_id = $1::uuid
          AND status = $5::text
        RETURNING *
        `,
        [
            task.operation_id,
            status,
            failureCode,
            nextAttemptAt,
            task.status,
        ],
    );

    if (result.rowCount !== 1) {
        throw new DomainError(
            "CONFLICT",
            "Creation task state changed",
        );
    }

    return result.rows[0];
}

export async function completeConfirmedOrder(
    client,
    task,
    reservation
) {
    assertAcknowledgment(task, reservation);

    if (
        reservation.status !== "CONFIRMED" ||
        !["CONFIRM_PENDING", "COMPLETED"].includes(task.status)
    ) {
        throw new DomainError(
            "CONFLICT",
            "Task is not ready for completion"
        );
    }

    const order = await findStoredOrder(
        client,
        task.operation_id
    );

    if (
        !order ||
        order.uid !== task.uid ||
        task.score === null
    ) {
        throw new DomainError(
            "INTERNAL",
            "Stored order state is inconsistent"
        );
    }

    if (
        JSON.stringify(pricedItems(reservation.items)) !==
        JSON.stringify(pricedItems(order.items))
    ) {
        throw new DomainError(
            "CONFLICT",
            "Confirmed stock does not match stored order"
        );
    }

    if (task.status === "COMPLETED") {
        return { task, order };
    }

    const updated = await changeStatus(
        client,
        task,
        "COMPLETED",
        null
    );

    return { task: updated, order };
}

export async function requestStockRelease(
    client,
    task,
    failureCode,
    { nextAttemptAt = null } = {}
) {
    if (!PERMANENT_FAILURES.has(failureCode)) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Release requires a permanent failure code"
        );
    }

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

    const order = await findStoredOrder(
        client,
        task.operation_id
    );

    if (order) {
        throw new DomainError(
            "CONFLICT",
            "Stored order must be confirmed"
        );
    }

    if (["RELEASE_PENDING", "FAILED"].includes(task.status)) {
        if (!PERMANENT_FAILURES.has(task.failure_code)) {
            throw new DomainError(
                "INTERNAL",
                "Stored failure decision is inconsistent"
            );
        }

        return task;
    }

    if (task.status !== "PROCESSING") {
        throw new DomainError(
            "CONFLICT",
            "Task cannot enter release state"
        );
    }

    return changeStatus(
        client,
        task,
        "RELEASE_PENDING",
        failureCode,
        nextAttemptAt
    );
}

export async function finishStockRelease(
    client,
    task,
    reservation
) {
    assertAcknowledgment(task, reservation);

    if (reservation.status !== "RELEASED") {
        throw new DomainError(
            "CONFLICT",
            "Stock release has not been acknowledged"
        );
    }

    const order = await findStoredOrder(
        client,
        task.operation_id
    );

    if (order) {
        throw new DomainError(
            "CONFLICT",
            "Stored order cannot enter failed release state"
        );
    }

    if (
        !["RELEASE_PENDING", "FAILED"].includes(task.status) ||
        !PERMANENT_FAILURES.has(task.failure_code)
    ) {
        throw new DomainError(
            "CONFLICT",
            "Task has no valid release decision"
        );
    }

    if (task.status === "FAILED") {
        return task;
    }

    return changeStatus(
        client,
        task,
        "FAILED",
        task.failure_code
    );
}