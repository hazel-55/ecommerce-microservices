import assert from "node:assert/strict";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { loadSync } from "@grpc/proto-loader";

const protoPath = fileURLToPath(
    new URL("../../../proto/order.proto", import.meta.url)
);

const definition = loadSync(protoPath, {
    includeDirs: [dirname(protoPath)],
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const service = definition["ecommerce.order.v1.OrderService"];

assert.ok(service, "Khong tim thay OrderService");

const methods = Object.keys(service).sort();

assert.deepEqual(
    methods,
    ["CreateOrder", "GetOrder", "ListOrders"],
    "OrderService co danh sach RPC khong dung"
);

assert.ok(
    definition["ecommerce.shipment.v1.Shipment"],
    "Khong doc duoc Shipment tu file import"
);

console.log("Order proto: OK");
console.log("RPC methods:", methods.join(", "));
console.log("Shipment import: OK");