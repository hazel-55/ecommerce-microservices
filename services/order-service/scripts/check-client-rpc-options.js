import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { getEventListeners } from "node:events";
import { fileURLToPath } from "node:url";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { DomainError } from "../src/domain/errors.js";

import {
    createCustomerClient,
    customerClient,
} from "../src/infrastructure/grpc/customer.client.js";
import {
    createProductClient,
    productClient,
} from "../src/infrastructure/grpc/product.client.js";
import {
    createShipmentClient,
    shipmentClient,
} from "../src/infrastructure/grpc/shipment.client.js";

function readProto(name) {
    const definition = loadSync(
        fileURLToPath(
            new URL(
                `../../../proto/${name}.proto`,
                import.meta.url,
            ),
        ),
        {
            keepCase: false,
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true,
        },
    );

    return grpc.loadPackageDefinition(definition)
        .ecommerce[name].v1;
}

const customerProto = readProto("customer");
const productProto = readProto("product");
const shipmentProto = readProto("shipment");

const actor = { uid: 13, role: "CUSTOMER" };
const operationId = randomUUID();
const items = [{ pid: 7, qty: 2 }];

// Token thử nghiệm, không lấy hoặc in secret thật.
const tokens = {
    customer: "customer-fixture-token-".padEnd(40, "c"),
    product: "product-fixture-token-".padEnd(40, "p"),
    shipment: "shipment-fixture-token-".padEnd(40, "s"),
};

const server = new grpc.Server();
const clients = [];
const gates = new Map();

let bound = false;
let requests = 0;
let mode = "success";

function deferred() {
    let resolve;

    const promise = new Promise((done) => {
        resolve = done;
    });

    return { promise, resolve };
}

function within(promise, milliseconds = 3000) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            reject(new Error("RPC test wait timed out"));
        }, milliseconds);

        promise.then(
            (value) => {
                clearTimeout(timer);
                resolve(value);
            },
            (error) => {
                clearTimeout(timer);
                reject(error);
            },
        );
    });
}

function stockResponse(status) {
    return (request) => ({
        operationId: request.operationId,
        uid: request.uid,
        status,
        items: [{
            pid: 7,
            qty: 2,
            unitPrice: "25.50",
        }],
    });
}

function route(name, token, buildResponse) {
    return (call, callback) => {
        requests += 1;

        try {
            assert.deepEqual(
                call.metadata.get("x-service-name"),
                ["order-service"],
            );

            assert.deepEqual(
                call.metadata.get("x-service-token"),
                [token],
            );

            assert.deepEqual(
                call.metadata.get("x-user-id"),
                ["13"],
            );

            assert.deepEqual(
                call.metadata.get("x-user-role"),
                ["CUSTOMER"],
            );

            if (name === "shipment") {
                assert.equal(
                    call.request.sourceEventId,
                    operationId,
                );
            } else {
                assert.equal(call.request.uid, 13);
            }

            if (
                name !== "customer" &&
                name !== "shipment"
            ) {
                assert.equal(
                    call.request.operationId,
                    operationId,
                );
            }

            if (name === "reserve") {
                assert.deepEqual(call.request.items, items);
            }
        } catch {
            callback({
                code: grpc.status.UNAUTHENTICATED,
                details: "Invalid fixture binding",
            });

            return;
        }

        if (mode === "pending") {
            const gate = gates.get(name);

            gate.deadline = Number(call.getDeadline());

            call.once("cancelled", () => {
                gate.cancelled.resolve();
            });

            gate.started.resolve();
            return;
        }

        if (mode === "missing" || mode === "auth") {
            callback({
                code: mode === "missing"
                    ? grpc.status.NOT_FOUND
                    : grpc.status.UNAUTHENTICATED,
                details: "Private fixture error",
            });

            return;
        }

        const response = buildResponse(call.request);

        if (mode === "malformed") {
            if (name === "shipment") {
                response.oid = 99;
            } else if (name !== "customer") {
                response.items[0].unitPrice = "invalid";
            }
        }

        callback(null, response);
    };
}

server.addService(customerProto.CustomerService.service, {
    GetCustomer: route(
        "customer",
        tokens.customer,
        (request) => ({
            uid: request.uid,
            fullname: "RPC fixture",
            score: 100,
        }),
    ),
});

server.addService(productProto.ProductService.service, {
    ReserveStock: route(
        "reserve",
        tokens.product,
        stockResponse("RESERVED"),
    ),
    ConfirmStock: route(
        "confirm",
        tokens.product,
        stockResponse("CONFIRMED"),
    ),
    ReleaseStock: route(
        "release",
        tokens.product,
        stockResponse("RELEASED"),
    ),
    GetStockReservation: route(
        "lookup",
        tokens.product,
        stockResponse("RESERVED"),
    ),
});

server.addService(shipmentProto.ShipmentService.service, {
    GetShipmentBySourceEvent: route(
        "shipment",
        tokens.shipment,
        () => ({
            shipid: 61,
            oid: 41,
            status: "PENDING",
        }),
    ),
});

async function expectCode(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    const port = await new Promise((resolve, reject) => {
        server.bindAsync(
            "127.0.0.1:0",
            grpc.ServerCredentials.createInsecure(),
            (error, assignedPort) => {
                if (error) {
                    reject(error);
                    return;
                }

                bound = true;
                resolve(assignedPort);
            },
        );
    });

    const address = `127.0.0.1:${port}`;

    const customers = createCustomerClient({
        address,
        serviceToken: tokens.customer,
    });

    const products = createProductClient({
        address,
        serviceToken: tokens.product,
    });

    const shipments = createShipmentClient({
        address,
        serviceToken: tokens.shipment,
    });

    clients.push(customers, products, shipments);

    const operations = [
        {
            name: "customer",
            run: (options) =>
                customers.getCustomer(13, actor, options),
        },
        {
            name: "reserve",
            run: (options) =>
                products.reserveStock(
                    operationId,
                    items,
                    actor,
                    options,
                ),
        },
        {
            name: "confirm",
            run: (options) =>
                products.confirmStock(
                    operationId,
                    actor,
                    options,
                ),
        },
        {
            name: "release",
            run: (options) =>
                products.releaseStock(
                    operationId,
                    actor,
                    options,
                ),
        },
        {
            name: "lookup",
            run: (options) =>
                products.getStockReservation(
                    operationId,
                    actor,
                    options,
                ),
        },
        {
            name: "shipment",
            run: (options) =>
                shipments.getShipmentBySourceEvent(
                    operationId,
                    41,
                    actor,
                    options,
                ),
        },
    ];

    for (const operation of operations) {
        const response = await operation.run();

        if (operation.name === "customer") {
            assert.equal(response.uid, 13);
            assert.equal(response.score, 100);
        } else if (operation.name === "shipment") {
            assert.deepEqual(response, {
                shipid: 61,
                oid: 41,
                status: "PENDING",
            });
        } else {
            assert.equal(response.operationId, operationId);
            assert.equal(response.uid, 13);

            assert.deepEqual(response.items, [{
                pid: 7,
                qty: 2,
                unit_price: "25.50",
            }]);

            const expectedStatus = {
                reserve: "RESERVED",
                confirm: "CONFIRMED",
                release: "RELEASED",
                lookup: "RESERVED",
            }[operation.name];

            assert.equal(response.status, expectedStatus);
        }
    }

    console.log("Six RPC methods preserve metadata and responses: OK");

    let before = requests;

    for (const operation of operations) {
        await expectCode(
            () => operation.run({
                deadline: new Date(Date.now() - 1000),
            }),
            "SERVICE_UNAVAILABLE",
        );
    }

    assert.equal(requests, before);

    console.log("Expired deadlines rejected before RPC: OK");

    const preAborted = new AbortController();
    preAborted.abort();

    for (const operation of operations) {
        await expectCode(
            () => operation.run({
                signal: preAborted.signal,
            }),
            "SERVICE_UNAVAILABLE",
        );
    }

    assert.equal(requests, before);

    console.log("Pre-aborted signals rejected before RPC: OK");

    mode = "pending";

    for (const operation of operations) {
        const gate = {
            started: deferred(),
            cancelled: deferred(),
            deadline: null,
        };

        gates.set(operation.name, gate);

        const deadline = new Date(Date.now() + 300);

        const result = expectCode(
            () => operation.run({ deadline }),
            "SERVICE_UNAVAILABLE",
        );

        await within(gate.started.promise);

        // Phải dùng deadline truyền vào, không rơi về mặc định 10s.
        assert.ok(
            gate.deadline <= deadline.getTime() + 50,
        );

        await within(result);
        await within(gate.cancelled.promise);
    }

    // Kiểm tra timeout riêng ngắn hơn deadline của request.
    const capGate = {
        started: deferred(),
        cancelled: deferred(),
        deadline: null,
    };

    gates.set("customer", capGate);

    const capStartedAt = Date.now();

    const capResult = expectCode(
        () => operations[0].run({
            timeoutMs: 300,
            deadline: new Date(Date.now() + 60_000),
        }),
        "SERVICE_UNAVAILABLE",
    );

    await within(capGate.started.promise);

    assert.ok(
        capGate.deadline <= capStartedAt + 350,
    );

    await within(capResult);
    await within(capGate.cancelled.promise);

    console.log("Live deadlines and timeout caps reach all clients: OK");

    for (const operation of operations) {
        const controller = new AbortController();

        const gate = {
            started: deferred(),
            cancelled: deferred(),
            deadline: null,
        };

        gates.set(operation.name, gate);

        const result = expectCode(
            () => operation.run({
                timeoutMs: 5000,
                signal: controller.signal,
            }),
            "SERVICE_UNAVAILABLE",
        );

        await within(gate.started.promise);

        controller.abort();

        // Phải hủy ngay, không đợi timeout 5s.
        await within(result, 1000);
        await within(gate.cancelled.promise, 1000);

        assert.equal(
            getEventListeners(controller.signal, "abort").length,
            0,
        );
    }

    mode = "success";

    // Các channel vẫn dùng lại được sau khi hủy RPC.
    for (const operation of operations) {
        await operation.run();
    }

    console.log("Live cancellation maps safely and channels remain reusable: OK");

    before = requests;

    await expectCode(
        () => customers.getCustomer(0, actor),
        "INVALID_ARGUMENT",
    );

    await expectCode(
        () => products.reserveStock("invalid", items, actor),
        "INVALID_ARGUMENT",
    );

    await expectCode(
        () => shipments.getShipmentBySourceEvent(
            operationId,
            0,
            actor,
        ),
        "INVALID_ARGUMENT",
    );

    assert.equal(requests, before);

    for (const operation of operations) {
        mode = "missing";

        await expectCode(
            () => operation.run(),
            "NOT_FOUND",
        );

        mode = "auth";

        await expectCode(
            () => operation.run(),
            "INTERNAL",
        );
    }

    mode = "malformed";

    for (const operation of operations) {
        if (operation.name !== "customer") {
            await expectCode(
                () => operation.run(),
                "INTERNAL",
            );
        }
    }

    console.log("Existing validation, response guards and error mappings preserved: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Client RPC options check failed:",
        error.code ?? error.name,
    );

    process.exitCode = 1;
} finally {
    for (const client of clients) {
        client.close();
    }

    customerClient.close();
    productClient.close();
    shipmentClient.close();

    try {
        if (bound) {
            await new Promise((resolve, reject) => {
                const timer = setTimeout(() => {
                    server.forceShutdown();
                    reject(new Error("Test server shutdown timed out"));
                }, 5000);

                server.tryShutdown((error) => {
                    clearTimeout(timer);

                    if (error) {
                        reject(error);
                        return;
                    }

                    resolve();
                });
            });
        } else {
            server.forceShutdown();
        }

        console.log("Client RPC test connections closed: OK");
    } catch (error) {
        server.forceShutdown();

        console.error(
            "Client RPC test shutdown failed:",
            error.code ?? error.name,
        );

        process.exitCode = 1;
    }
}