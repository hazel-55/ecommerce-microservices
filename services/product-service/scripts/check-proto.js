import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { loadSync } from "@grpc/proto-loader";

const protoPath = fileURLToPath(
    new URL("../../../proto/product.proto", import.meta.url)
);

const definition = loadSync(protoPath, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const service = definition["ecommerce.product.v1.ProductService"];

assert.ok(service, "Khong tim thay ProductService");

const methods = Object.keys(service).sort();

assert.deepEqual(
    methods,
    [
        "CreateProduct",
        "DeleteProduct",
        "GetProduct",
        "ListProducts",
        "UpdateProduct",
    ],
    "ProductService co danh sach RPC khong dung"
);

console.log("Product proto: OK");
console.log("RPC methods:", methods.join(", "));