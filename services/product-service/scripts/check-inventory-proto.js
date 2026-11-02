import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";

const definition = loadSync(
    fileURLToPath(
        new URL("../../../proto/product.proto", import.meta.url)
    ),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    }
);

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.product.v1;

const service = proto.ProductService.service;

assert.deepEqual(
    Object.keys(service).sort(),
    [
        "ListProducts",
        "GetProduct",
        "CreateProduct",
        "UpdateProduct",
        "DeleteProduct",
        "ReserveStock",
        "ConfirmStock",
        "ReleaseStock",
        "GetStockReservation",
    ].sort()
);

const product = service.GetProduct.responseDeserialize(
    service.GetProduct.responseSerialize({
        pid: 1,
        pname: "Product proto fixture",
        price: "25.50",
        quantity: 10,
        reservedQuantity: 2,
    })
);

assert.deepEqual(product, {
    pid: 1,
    pname: "Product proto fixture",
    price: "25.50",
    quantity: 10,
});

console.log("Product CRUD and inventory RPC contract: OK");

const operationId = randomUUID();

const reserveRequest = {
    operationId,
    uid: 13,
    items: [
        { pid: 1, qty: 2 },
        { pid: 2, qty: 1 },
    ],
};

assert.deepEqual(
    service.ReserveStock.requestDeserialize(
        service.ReserveStock.requestSerialize(reserveRequest)
    ),
    reserveRequest
);

console.log("Reserve stock request fields: OK");

const command = {
    operationId,
    uid: 13,
};

for (const method of [
    "ConfirmStock",
    "ReleaseStock",
    "GetStockReservation",
]) {
    assert.deepEqual(
        service[method].requestDeserialize(
            service[method].requestSerialize(command)
        ),
        command
    );
}

console.log("Stock command request fields: OK");

for (const method of [
    "ReserveStock",
    "ConfirmStock",
    "ReleaseStock",
    "GetStockReservation",
]) {
    for (const status of [
        "RESERVED",
        "CONFIRMED",
        "RELEASED",
    ]) {
        const response = {
            operationId,
            uid: 13,
            status,
            items: [
                { pid: 1, qty: 2, unitPrice: "25.50" },
                { pid: 2, qty: 1, unitPrice: "0.00" },
            ],
        };

        assert.deepEqual(
            service[method].responseDeserialize(
                service[method].responseSerialize(response)
            ),
            response
        );
    }
}

const emptyReleased = {
    operationId,
    uid: 13,
    status: "RELEASED",
    items: [],
};

assert.deepEqual(
    service.ReleaseStock.responseDeserialize(
        service.ReleaseStock.responseSerialize(emptyReleased)
    ),
    emptyReleased
);

console.log("Stock reservation response fields and decimal prices: OK");