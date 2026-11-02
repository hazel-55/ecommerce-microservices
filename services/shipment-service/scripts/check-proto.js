import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { loadSync } from "@grpc/proto-loader";

const protoPath = fileURLToPath(
    new URL("../../../proto/shipment.proto", import.meta.url)
);

const definition = loadSync(protoPath, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const service = definition["ecommerce.shipment.v1.ShipmentService"];

assert.ok(service, "Khong tim thay ShipmentService");

const methods = Object.keys(service).sort();

assert.deepEqual(
    methods,
    ["GetShipment", "UpdateShipmentStatus"],
    "ShipmentService co danh sach RPC khong dung"
);

console.log("Shipment proto: OK");
console.log("RPC methods:", methods.join(", "));