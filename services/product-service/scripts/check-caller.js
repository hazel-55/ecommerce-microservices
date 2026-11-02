import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import { DomainError } from "../src/domain/errors.js";
import { productTransportConfig } from "../src/infrastructure/config/env.js";
import { readGatewayActor } from "../src/interfaces/grpc/caller.js";

function makeMetadata({
    service = "api-gateway",
    token = productTransportConfig.gatewayToken,
    uid = "13",
    role = "CUSTOMER",
} = {}) {
    const metadata = new grpc.Metadata();

    metadata.set("x-service-name", service);
    metadata.set("x-service-token", token);
    metadata.set("x-user-id", uid);
    metadata.set("x-user-role", role);

    return metadata;
}

function expectRejected(metadata) {
    assert.throws(
        () => readGatewayActor(metadata),
        (error) => {
            assert.ok(error instanceof DomainError);
            assert.equal(error.code, "UNAUTHENTICATED");
            return true;
        },
    );
}

try {
    assert.deepEqual(
        readGatewayActor(makeMetadata()),
        { uid: 13, role: "CUSTOMER" },
    );

    expectRejected(makeMetadata({ token: "incorrect-token" }));
    expectRejected(makeMetadata({ service: "auth-service" }));

    console.log("Gateway caller credentials: OK");

    assert.deepEqual(
        readGatewayActor(makeMetadata({ uid: "1", role: "ADMIN" })),
        { uid: 1, role: "ADMIN" },
    );

    assert.deepEqual(
        readGatewayActor(makeMetadata({
            uid: "2",
            role: "SHIPMENT_STAFF",
        })),
        { uid: 2, role: "SHIPMENT_STAFF" },
    );

    console.log("Verified actor metadata: OK");

    const missingToken = makeMetadata();
    missingToken.remove("x-service-token");
    expectRejected(missingToken);

    const duplicateUid = makeMetadata();
    duplicateUid.add("x-user-id", "14");
    expectRejected(duplicateUid);

    for (const uid of ["0", "01", "2147483648"]) {
        expectRejected(makeMetadata({ uid }));
    }

    expectRejected(makeMetadata({ role: "UNKNOWN" }));

    console.log("Invalid and duplicate caller data rejected: OK");
} catch (error) {
    console.error("Product caller check failed:", error.message);
    process.exitCode = 1;
}