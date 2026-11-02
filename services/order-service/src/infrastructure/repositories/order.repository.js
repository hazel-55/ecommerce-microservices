import { randomUUID } from "node:crypto";
import { pool } from "../database/pool.js";
import {
    findStoredOrder,
} from "../database/order-creation.persistence.js";
import {
    validateCreationOperationId,
} from "../../domain/order-creation.rules.js";

export function createOrderRepository({
    database = pool,
} = {}) {
    return {
        async findByOperationId(operationId) {
            validateCreationOperationId(operationId);

            return findStoredOrder(database, operationId);
        },
        async listByUid(uid) {
            const result = await database.query(
                `
                SELECT oid, uid, discount
                FROM orders
                WHERE uid = $1
                ORDER BY oid DESC
                `,
                [uid]
            );

            return result.rows;
        },

        async findById(oid) {
            const result = await database.query(
                `
                SELECT
                    o.oid,
                    o.uid,
                    o.discount,
                    d.pid,
                    d.qty,
                    d.unit_price
                FROM orders o
                LEFT JOIN order_details d ON d.oid = o.oid
                WHERE o.oid = $1
                ORDER BY d.pid
                `,
                [oid]
            );

            if (result.rowCount === 0) {
                return null;
            }

            const first = result.rows[0];

            return {
                oid: first.oid,
                uid: first.uid,
                discount: first.discount,
                items: result.rows
                    .filter((row) => row.pid !== null)
                    .map((row) => ({
                        pid: row.pid,
                        qty: row.qty,
                        unit_price: row.unit_price,
                    })),
            };
        },
        async createOrder({
            uid,
            discount,
            items,
            eventId = randomUUID(),
        }) {
            const snapshots = items.map((item) => ({
                pid: item.pid,
                qty: item.qty,
                unit_price: item.unit_price,
            }));

            const client = await database.connect();

            let transactionStarted = false;
            let releaseError;

            try {
                await client.query("BEGIN");
                transactionStarted = true;

                const insertedOrder = await client.query(
                    `
                    INSERT INTO orders (uid, discount)
                    VALUES ($1, $2)
                    RETURNING oid, uid, discount
                    `,
                    [uid, discount]
                );

                const order = insertedOrder.rows[0];
                const storedItems = [];

                for (const item of snapshots) {
                    const insertedDetail = await client.query(
                        `
                        INSERT INTO order_details (
                            oid,
                            pid,
                            qty,
                            unit_price
                        )
                        VALUES ($1, $2, $3, $4)
                        RETURNING pid, qty, unit_price
                        `,
                        [
                            order.oid,
                            item.pid,
                            item.qty,
                            item.unit_price,
                        ]
                    );

                    storedItems.push(insertedDetail.rows[0]);
                }

                const payload = {
                    eventId,
                    eventType: "OrderCreated",
                    version: 1,
                    oid: order.oid,
                    uid: order.uid,
                    discount: order.discount,
                    items: storedItems,
                };

                await client.query(
                    `
                    INSERT INTO outbox_events (
                        event_id,
                        event_type,
                        oid,
                        payload
                    )
                    VALUES ($1, $2, $3, $4::jsonb)
                    `,
                    [
                        eventId,
                        "OrderCreated",
                        order.oid,
                        JSON.stringify(payload),
                    ]
                );

                await client.query("COMMIT");
                transactionStarted = false;

                return {
                    ...order,
                    items: storedItems,
                    eventId,
                };
            } catch (error) {
                if (transactionStarted) {
                    try {
                        await client.query("ROLLBACK");
                    } catch (rollbackError) {
                        releaseError = rollbackError;
                    }
                }

                throw error;
            } finally {
                client.release(releaseError);
            }
        },
    };
}

export const orderRepository = createOrderRepository();