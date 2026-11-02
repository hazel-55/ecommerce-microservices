import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { loadSync } from "@grpc/proto-loader";

const protoPath = fileURLToPath(
    new URL("../../../proto/auth.proto", import.meta.url)
);

const definition = loadSync(protoPath, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const service = definition["ecommerce.auth.v1.AuthService"];

assert.ok(service, "Khong tim thay AuthService");

const methods = Object.keys(service).sort();

assert.deepEqual(
    methods,
    ["Login", "Register"],
    "AuthService phai co dung hai RPC Login va Register"
);

console.log("Auth proto: OK");
console.log("RPC methods:", methods.join(", "));