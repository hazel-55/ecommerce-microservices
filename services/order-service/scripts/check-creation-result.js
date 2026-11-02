import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import {
    createOrderRepository,
} from "../src/infrastructure/repositories/order.repository.js";
import {
    createOrderCreationResultReader,
} from "../src/application/order-creation.result.js";
import {
    shipmentClient,
} from "../src/infrastructure/grpc/shipment.client.js";

const operationId = randomUUID();
const actor = { uid: 13, role: "CUSTOMER" };

const originalTask = {
    operation_id: operationId,
    uid: 13,
    actor_role: "CUSTOMER",
    status: "COMPLETED",
    score: 100,
    failure_code: null,
    items: [{ pid: 7, qty: 2 }],
};

const originalOrder = {
    oid: 41,
    uid: 13,
    discount: "2.55",
    items: [
        { pid: 7, qty: 2, unit_price: "25.50" },
    ],
};

let task;
let order;
let shipment;
let taskError;
let orderError;
let shipmentError;
let calls;

function reset() {
    task = structuredClone(originalTask);
    order = structuredClone(originalOrder);
    shipment = {
        shipid: 61,
        oid: 41,
        status: "PENDING",
    };

    taskError = null;
    orderError = null;
    shipmentError = null;

    calls = {
        tasks: [],
        orders: [],
        shipments: [],
    };
}

const reader = createOrderCreationResultReader({
    tasks: {
        async findByOperationId(id) {
            calls.tasks.push(id);

            if (taskError) {
                throw taskError;
            }

            return structuredClone(task);
        },
    },
    orders: {
        async findByOperationId(id) {
            calls.orders.push(id);

            if (orderError) {
                throw orderError;
            }

            return structuredClone(order);
        },
    },
    shipments: {
        async getShipmentBySourceEvent(...args) {
            calls.shipments.push(structuredClone(args));

            if (shipmentError) {
                throw shipmentError;
            }

            return structuredClone(shipment);
        },
    },
});

async function expectCode(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    // Kiểm tra phương thức repository mới bằng query client giả lập.
    const queries = [];
    let missing = false;

    const repository = createOrderRepository({
        database: {
            async query(sql, parameters) {
                queries.push({ sql, parameters });

                if (sql.includes("FROM orders")) {
                    assert.match(sql, /operation_id\s*=\s*\$1/);
                    assert.deepEqual(parameters, [operationId]);

                    return {
                        rowCount: missing ? 0 : 1,
                        rows: missing ? [] : [{
                            oid: 41,
                            uid: 13,
                            discount: "2.55",
                        }],
                    };
                }

                assert.match(sql, /FROM order_details/);
                assert.deepEqual(parameters, [41]);

                return {
                    rowCount: 1,
                    rows: structuredClone(originalOrder.items),
                };
            },
        },
    });

    assert.deepEqual(
        await repository.findByOperationId(operationId),
        originalOrder,
    );

    missing = true;

    assert.equal(
        await repository.findByOperationId(operationId),
        null,
    );

    const queryCount = queries.length;

    await expectCode(
        () => repository.findByOperationId("invalid"),
        "INVALID_ARGUMENT",
    );

    assert.equal(queries.length, queryCount);

    console.log("Order repository operation lookup: OK");

    reset();

    await expectCode(
        () => reader.readResult(operationId, null),
        "UNAUTHENTICATED",
    );

    await expectCode(
        () => reader.readResult("invalid", actor),
        "INVALID_ARGUMENT",
    );

    assert.equal(calls.tasks.length, 0);

    await expectCode(
        () => reader.readResult(operationId, {
            uid: 14,
            role: "ADMIN",
        }),
        "FORBIDDEN",
    );

    assert.equal(calls.orders.length, 0);
    assert.equal(calls.shipments.length, 0);

    task = null;

    await expectCode(
        () => reader.readResult(operationId, actor),
        "NOT_FOUND",
    );

    console.log("Result input and operation ownership: OK");

    for (const status of [
        "PROCESSING",
        "CONFIRM_PENDING",
        "RELEASE_PENDING",
    ]) {
        reset();
        task.status = status;

        assert.equal(
            await reader.readResult(operationId, actor),
            null,
        );

        assert.equal(calls.orders.length, 0);
        assert.equal(calls.shipments.length, 0);
    }

    for (const code of [
        "INVALID_ARGUMENT",
        "NOT_FOUND",
        "CONFLICT",
        "FORBIDDEN",
    ]) {
        reset();
        task.status = "FAILED";
        task.failure_code = code;

        await expectCode(
            () => reader.readResult(operationId, actor),
            code,
        );

        assert.equal(calls.orders.length, 0);
        assert.equal(calls.shipments.length, 0);
    }

    console.log("Pending tasks and durable failures: OK");

    reset();

    const before = structuredClone({ task, order, shipment });

    const result = await reader.readResult(
        operationId,
        actor,
    );

    assert.deepEqual(result, {
        ...originalOrder,
        shipment: {
            shipid: 61,
            oid: 41,
            status: "PENDING",
        },
    });

    assert.deepEqual(calls.orders, [operationId]);

    assert.deepEqual(calls.shipments, [
        [operationId, 41, actor, {}],
    ]);

    assert.deepEqual(
        { task, order, shipment },
        before,
    );

    result.items[0].unit_price = "0.00";

    assert.equal(order.items[0].unit_price, "25.50");

    shipment.status = "SHIPPING";

    assert.equal(
        (await reader.readResult(operationId, actor))
            .shipment.status,
        "SHIPPING",
    );

    console.log("Completed Order and Shipment snapshot response: OK");

    reset();

    shipmentError = new DomainError(
        "NOT_FOUND",
        "Shipment not found",
    );

    assert.equal(
        await reader.readResult(operationId, actor),
        null,
    );

    assert.deepEqual(task, originalTask);
    assert.deepEqual(order, originalOrder);

    console.log("Pending Shipment preserves completed Order: OK");

    for (const code of [
        "SERVICE_UNAVAILABLE",
        "FORBIDDEN",
        "INTERNAL",
    ]) {
        reset();

        const expected = new DomainError(code, "Test failure");
        shipmentError = expected;

        await assert.rejects(
            () => reader.readResult(operationId, actor),
            (error) => error === expected,
        );

        assert.deepEqual(task, originalTask);
    }

    reset();
    orderError = new Error("Injected database failure");

    await assert.rejects(
        () => reader.readResult(operationId, actor),
        (error) => error === orderError,
    );

    assert.equal(calls.shipments.length, 0);

    reset();
    taskError = new Error("Injected task read failure");

    await assert.rejects(
        () => reader.readResult(operationId, actor),
        (error) => error === taskError,
    );

    assert.equal(calls.orders.length, 0);

    console.log("Service and repository failures propagated: OK");

    const corruptions = [
        () => { task.operation_id = randomUUID(); },
        () => { task.status = "UNKNOWN"; },
        () => {
            task.status = "FAILED";
            task.failure_code = "UNKNOWN";
        },
        () => { order = null; },
        () => { order.uid = 14; },
        () => { order.oid = 0; },
        () => { order.items = []; },
        () => { shipment = null; },
        () => { shipment.shipid = 0; },
        () => { shipment.oid = 99; },
        () => { shipment.status = "UNKNOWN"; },
    ];

    for (const corrupt of corruptions) {
        reset();
        corrupt();

        await expectCode(
            () => reader.readResult(operationId, actor),
            "INTERNAL",
        );
    }

    assert.throws(
        () => createOrderCreationResultReader({
            tasks: {},
        }),
        TypeError,
    );

    console.log("Inconsistent creation results rejected: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Creation result check failed:",
        error.code ?? error.name,
    );

    process.exitCode = 1;
} finally {
    shipmentClient.close();
}