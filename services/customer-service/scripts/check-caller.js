import assert from "node:assert/strict";
import grpc from "@grpc/grpc-js";
import {
    customerTransportConfig,
} from "../src/infrastructure/config/env.js";
import {
    readGatewayActor,
    readAuthCaller,
} from "../src/interfaces/grpc/caller.js";

function makeMetadata(service, token) {
    const metadata = new grpc.Metadata();

    metadata.set("x-service-name", service);
    metadata.set("x-service-token", token);

    return metadata;
}

function expectRejected(action) {
    assert.throws(
        action,
        (error) => error.code === "UNAUTHENTICATED"
    );
}

const gateway = makeMetadata(
    "api-gateway",
    customerTransportConfig.gatewayToken
);

gateway.set("x-user-id", "1");
gateway.set("x-user-role", "CUSTOMER");

assert.deepEqual(readGatewayActor(gateway), {
    uid: 1,
    role: "CUSTOMER",
});

const auth = makeMetadata(
    "auth-service",
    customerTransportConfig.authToken
);

assert.deepEqual(readAuthCaller(auth), {
    service: "auth-service",
});

expectRejected(() => readGatewayActor(new grpc.Metadata()));

expectRejected(() => readGatewayActor(
    makeMetadata("api-gateway", "wrong-token")
));

expectRejected(() => readGatewayActor(
    makeMetadata("api-gateway", customerTransportConfig.authToken)
));

gateway.add("x-user-id", "2");
expectRejected(() => readGatewayActor(gateway));

console.log("Customer caller authentication: OK");
console.log("Gateway and Auth identities: OK");
console.log("Invalid and ambiguous metadata rejected: OK");