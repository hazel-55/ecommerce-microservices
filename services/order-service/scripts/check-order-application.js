import assert from "node:assert/strict";
import { DomainError } from "../src/domain/errors.js";
import {
    pool,
} from "../src/infrastructure/database/pool.js";
import {
    createOrderRepository,
} from "../src/infrastructure/repositories/order.repository.js";
import {
    createOrderApplication,
} from "../src/application/order.application.js";

let client;
let transactionStarted = false;

const customer = { uid: 13, role: "CUSTOMER" };
const otherCustomer = { uid: 14, role: "CUSTOMER" };
const admin = { uid: 99, role: "ADMIN" };
const shipmentStaff = { uid: 13, role: "SHIPMENT_STAFF" };

async function expectDomainError(operation, code) {
    await assert.rejects(
        operation,
        (error) =>
            error instanceof DomainError &&
            error.code === code,
    );
}

async function insertOrder(uid, discount, items) {
    const inserted = await client.query(
        `
        INSERT INTO orders (uid, discount)
        VALUES ($1, $2)
        RETURNING oid
        `,
        [uid, discount],
    );

    const oid = inserted.rows[0].oid;

    for (const item of items) {
        await client.query(
            `
            INSERT INTO order_details (
                oid,
                pid,
                qty,
                unit_price
            )
            VALUES ($1, $2, $3, $4)
            `,
            [
                oid,
                item.pid,
                item.qty,
                item.unit_price,
            ],
        );
    }

    return oid;
}

async function main() {
    client = await pool.connect();

    await client.query("BEGIN");
    transactionStarted = true;

    // Bảng tạm chỉ tồn tại trên kết nối kiểm tra.
    await client.query(
        `
        CREATE TEMP TABLE orders (
            LIKE public.orders INCLUDING ALL
        )
        ON COMMIT DROP
        `,
    );

    await client.query(
        `
        CREATE TEMP TABLE order_details (
            LIKE public.order_details INCLUDING ALL
        )
        ON COMMIT DROP
        `,
    );

    const repository = createOrderRepository({
        database: client,
    });

    const application = createOrderApplication({
        repository,
    });

    const firstId = await insertOrder(
        13,
        "3.05",
        [
            { pid: 2, qty: 1, unit_price: "10.00" },
            { pid: 1, qty: 2, unit_price: "25.50" },
        ],
    );

    const otherId = await insertOrder(
        14,
        "0.00",
        [{ pid: 3, qty: 1, unit_price: "9.99" }],
    );

    const newestId = await insertOrder(
        13,
        "0.00",
        [{ pid: 4, qty: 1, unit_price: "1.00" }],
    );

    assert.deepEqual(
        await application.listOrders(customer),
        {
            items: [
                {
                    oid: newestId,
                    uid: 13,
                    discount: "0.00",
                },
                {
                    oid: firstId,
                    uid: 13,
                    discount: "3.05",
                },
            ],
        },
    );

    assert.deepEqual(
        await application.listOrders(otherCustomer),
        {
            items: [{
                oid: otherId,
                uid: 14,
                discount: "0.00",
            }],
        },
    );

    assert.deepEqual(
        await application.listOrders(admin),
        { items: [] },
    );

    console.log(
        "Order lists scoped to authenticated UID: OK",
    );

    const detail = await application.getOrder(
        firstId,
        customer,
    );

    assert.deepEqual(detail, {
        oid: firstId,
        uid: 13,
        discount: "3.05",
        items: [
            { pid: 1, qty: 2, unit_price: "25.50" },
            { pid: 2, qty: 1, unit_price: "10.00" },
        ],
    });

    assert.deepEqual(
        Object.keys(detail).sort(),
        ["discount", "items", "oid", "uid"],
    );

    for (const item of detail.items) {
        assert.equal(typeof item.unit_price, "string");

        assert.deepEqual(
            Object.keys(item).sort(),
            ["pid", "qty", "unit_price"],
        );
    }

    console.log(
        "Order detail and decimal response fields: OK",
    );

    await expectDomainError(
        () => application.getOrder(otherId, customer),
        "FORBIDDEN",
    );

    await expectDomainError(
        () => application.getOrder(otherId, shipmentStaff),
        "FORBIDDEN",
    );

    assert.equal(
        (
            await application.getOrder(firstId, shipmentStaff)
        ).uid,
        13,
    );

    assert.equal(
        (
            await application.getOrder(otherId, admin)
        ).uid,
        14,
    );

    console.log(
        "Order ownership and admin access enforced: OK",
    );

    let repositoryCalls = 0;

    const guarded = createOrderApplication({
        repository: {
            async listByUid() {
                repositoryCalls++;
                throw new Error("Unexpected repository access");
            },

            async findById() {
                repositoryCalls++;
                throw new Error("Unexpected repository access");
            },
        },
    });

    const invalidActors = [
        {
            actor: null,
            code: "UNAUTHENTICATED",
        },
        {
            actor: { uid: 0, role: "CUSTOMER" },
            code: "UNAUTHENTICATED",
        },
        {
            actor: { uid: 13, role: "UNKNOWN" },
            code: "FORBIDDEN",
        },
    ];

    for (const scenario of invalidActors) {
        await expectDomainError(
            () => guarded.listOrders(scenario.actor),
            scenario.code,
        );

        await expectDomainError(
            () => guarded.getOrder(firstId, scenario.actor),
            scenario.code,
        );
    }

    for (const oid of [
        0,
        -1,
        1.5,
        "1",
        null,
        2147483648,
    ]) {
        await expectDomainError(
            () => guarded.getOrder(oid, customer),
            "INVALID_ARGUMENT",
        );
    }

    assert.equal(repositoryCalls, 0);

    console.log(
        "Invalid actors and order IDs rejected before repository access: OK",
    );

    await expectDomainError(
        () => application.getOrder(
            2147483647,
            customer,
        ),
        "NOT_FOUND",
    );

    const marker = new Error("TEST_REPOSITORY_FAILURE");

    const failing = createOrderApplication({
        repository: {
            async listByUid() {
                throw marker;
            },

            async findById() {
                throw marker;
            },
        },
    });

    await assert.rejects(
        () => failing.listOrders(customer),
        (error) => error === marker,
    );

    await assert.rejects(
        () => failing.getOrder(firstId, customer),
        (error) => error === marker,
    );

    console.log(
        "Missing orders and repository failures handled: OK",
    );
}

try {
    await main();
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Order application check failed:",
        error.code ?? error.name,
        error.message,
    );
} finally {
    try {
        if (client && transactionStarted) {
            await client.query("ROLLBACK");
            transactionStarted = false;

            const remaining = await client.query(
                `
                SELECT
                    to_regclass('pg_temp.orders') AS orders,
                    to_regclass('pg_temp.order_details') AS details
                `,
            );

            assert.equal(remaining.rows[0].orders, null);
            assert.equal(remaining.rows[0].details, null);

            console.log(
                "Temporary Order application fixtures removed: OK",
            );
        }
    } catch (error) {
        process.exitCode = 1;

        console.error(
            "Order application cleanup failed:",
            error.code ?? error.name,
            error.message,
        );
    } finally {
        client?.release();
        await pool.end();
    }
}