import { DomainError } from "../../domain/errors.js";
import { normalizeOrderInput } from "../../domain/order.rules.js";
import {
    calculateOrderPricing,
} from "../../domain/order.pricing.js";

function canonicalItems(items) {
    return normalizeOrderInput({ items }).items.sort(
        (first, second) => first.pid - second.pid
    );
}

export async function findStoredOrder(client, operationId) {
    const result = await client.query(
        `
        SELECT oid, uid, discount
        FROM orders
        WHERE operation_id = $1
        `,
        [operationId]
    );

    if (result.rowCount === 0) {
        return null;
    }

    const order = result.rows[0];

    const details = await client.query(
        `
        SELECT pid, qty, unit_price
        FROM order_details
        WHERE oid = $1
        ORDER BY pid
        `,
        [order.oid]
    );

    return {
        ...order,
        items: details.rows,
    };
}

// Call only inside the transaction that already locks this task.
export async function persistReservedOrder(
    client,
    task,
    reservation,
    { nextAttemptAt = null } = {}
) {
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

    if (
        !reservation ||
        reservation.operationId !== task.operation_id
    ) {
        throw new DomainError(
            "CONFLICT",
            "Reservation operation does not match task"
        );
    }

    if (reservation.uid !== task.uid) {
        throw new DomainError(
            "FORBIDDEN",
            "Reservation owner does not match task"
        );
    }

    if (
        JSON.stringify(canonicalItems(reservation.items)) !==
        JSON.stringify(canonicalItems(task.items))
    ) {
        throw new DomainError(
            "CONFLICT",
            "Reservation items do not match task"
        );
    }

    const existing = await findStoredOrder(
        client,
        task.operation_id
    );

    if (existing) {
        if (
            existing.uid !== task.uid ||
            !["CONFIRM_PENDING", "COMPLETED"].includes(task.status) ||
            !["RESERVED", "CONFIRMED"].includes(reservation.status)
        ) {
            throw new DomainError(
                "INTERNAL",
                "Stored order state is inconsistent"
            );
        }

        return { order: existing, task };
    }

    if (
        task.status !== "PROCESSING" ||
        task.score === null
    ) {
        throw new DomainError(
            "CONFLICT",
            "Task is not ready to create an order"
        );
    }

    if (reservation.status === "CONFIRMED") {
        throw new DomainError(
            "INTERNAL",
            "Confirmed stock has no stored order"
        );
    }

    if (reservation.status !== "RESERVED") {
        throw new DomainError(
            "CONFLICT",
            "Stock reservation is not active"
        );
    }

    const pricing = calculateOrderPricing(
        [...reservation.items].sort(
            (first, second) => first.pid - second.pid
        ),
        task.score
    );

    const inserted = await client.query(
        `
        INSERT INTO orders (uid, discount, operation_id)
        VALUES ($1, $2, $3)
        RETURNING oid, uid, discount
        `,
        [task.uid, pricing.discount, task.operation_id]
    );

    const order = {
        ...inserted.rows[0],
        items: [],
    };

    for (const item of pricing.items) {
        const detail = await client.query(
            `
            INSERT INTO order_details (oid, pid, qty, unit_price)
            VALUES ($1, $2, $3, $4)
            RETURNING pid, qty, unit_price
            `,
            [order.oid, item.pid, item.qty, item.unit_price]
        );

        order.items.push(detail.rows[0]);
    }

    const advanced = await client.query(
        `
        UPDATE order_creation_tasks
        SET status = 'CONFIRM_PENDING',
            failure_code = NULL,
            last_error_code = NULL,
            next_attempt_at = COALESCE(
                $2::timestamptz,
                CURRENT_TIMESTAMP
            ),
            updated_at = CURRENT_TIMESTAMP
        WHERE operation_id = $1
          AND status = 'PROCESSING'
          AND score IS NOT NULL
        RETURNING *
        `,
        [task.operation_id, nextAttemptAt]
    );

    if (advanced.rowCount !== 1) {
        throw new DomainError(
            "CONFLICT",
            "Task state could not advance"
        );
    }

    const payload = {
        eventId: task.operation_id,
        eventType: "OrderCreated",
        version: 1,
        oid: order.oid,
        uid: order.uid,
        discount: order.discount,
        items: order.items,
    };

    await client.query(
        `
        INSERT INTO outbox_events (
            event_id,
            event_type,
            oid,
            payload,
            next_attempt_at
        )
        VALUES (
            $1, $2, $3, $4::jsonb,
            COALESCE($5::timestamptz, CURRENT_TIMESTAMP)
        )
        `,
        [
            task.operation_id,
            "OrderCreated",
            order.oid,
            JSON.stringify(payload),
            nextAttemptAt,
        ]
    );

    return {
        order,
        task: advanced.rows[0],
    };
}