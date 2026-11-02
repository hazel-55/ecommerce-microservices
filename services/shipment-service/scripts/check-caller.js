import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import grpc from "@grpc/grpc-js";
import dotenv from "dotenv";

import { DomainError } from "../src/domain/errors.js";
import {
    shipmentTransportConfig,
    shipmentServerConfig,
} from "../src/infrastructure/config/env.js";
import {
    readGatewayActor,
} from "../src/interfaces/grpc/caller.js";

function callerMetadata({
    service = "api-gateway",
    token = shipmentTransportConfig.gatewayToken,
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

function expectUnauthenticated(metadata) {
    assert.throws(
        () => readGatewayActor(metadata),
        (error) => {
            assert.ok(error instanceof DomainError);
            assert.equal(error.code, "UNAUTHENTICATED");
            return true;
        }
    );
}

try {
    const [shipmentText, gatewayText] = await Promise.all([
        readFile(
            new URL("../.env", import.meta.url),
            "utf8"
        ),
        readFile(
            new URL("../../api-gateway/.env", import.meta.url),
            "utf8"
        ),
    ]);

    const shipmentEnv = dotenv.parse(shipmentText);
    const gatewayEnv = dotenv.parse(gatewayText);

    assert.ok(
        shipmentEnv.SHIPMENT_GATEWAY_TOKEN ===
        gatewayEnv.SHIPMENT_GATEWAY_TOKEN
    );

    assert.ok(
        shipmentEnv.SHIPMENT_GATEWAY_TOKEN ===
        shipmentTransportConfig.gatewayToken
    );

    assert.ok(
        shipmentEnv.SHIPMENT_GRPC_ADDRESS ===
        gatewayEnv.SHIPMENT_GRPC_ADDRESS
    );

    assert.ok(
        shipmentEnv.SHIPMENT_GRPC_ADDRESS ===
        shipmentServerConfig.grpcAddress
    );

    const tokens = [
        gatewayEnv.CUSTOMER_GATEWAY_TOKEN,
        gatewayEnv.PRODUCT_GATEWAY_TOKEN,
        gatewayEnv.ORDER_GATEWAY_TOKEN,
        gatewayEnv.SHIPMENT_GATEWAY_TOKEN,
    ];

    for (const token of tokens) {
        assert.equal(typeof token, "string");
        assert.ok(token.length >= 32);
    }

    assert.equal(new Set(tokens).size, 4);

    console.log(
        "Gateway to Shipment shared transport binding: OK"
    );

    assert.deepEqual(
        readGatewayActor(callerMetadata()),
        { uid: 13, role: "CUSTOMER" }
    );

    expectUnauthenticated(
        callerMetadata({ service: "order-service" })
    );

    expectUnauthenticated(
        callerMetadata({ token: "invalid" })
    );

    const validToken = shipmentTransportConfig.gatewayToken;
    const wrongSameLengthToken =
        validToken.slice(0, -1) +
        (validToken.endsWith("a") ? "b" : "a");

    expectUnauthenticated(
        callerMetadata({ token: wrongSameLengthToken })
    );

    const missingToken = callerMetadata();
    missingToken.remove("x-service-token");

    expectUnauthenticated(missingToken);

    for (const token of tokens.slice(0, 3)) {
        expectUnauthenticated(callerMetadata({ token }));
    }

    console.log("Gateway caller credentials: OK");

    for (const role of [
        "CUSTOMER",
        "ADMIN",
        "SHIPMENT_STAFF",
    ]) {
        assert.deepEqual(
            readGatewayActor(callerMetadata({ role })),
            { uid: 13, role }
        );
    }

    for (const uid of [1, 2147483647]) {
        assert.deepEqual(
            readGatewayActor(
                callerMetadata({ uid: String(uid) })
            ),
            { uid, role: "CUSTOMER" }
        );
    }

    console.log("Verified actor metadata: OK");

    for (const uid of [
        "",
        "0",
        "-1",
        "01",
        "1.0",
        "1e2",
        "2147483648",
        "9007199254740993",
        "abc",
    ]) {
        expectUnauthenticated(callerMetadata({ uid }));
    }

    for (const role of ["", "customer", "ROOT"]) {
        expectUnauthenticated(callerMetadata({ role }));
    }

    for (const name of [
        "x-service-name",
        "x-service-token",
        "x-user-id",
        "x-user-role",
    ]) {
        const missing = callerMetadata();
        missing.remove(name);
        expectUnauthenticated(missing);

        const duplicated = callerMetadata();
        duplicated.add(name, "duplicate");
        expectUnauthenticated(duplicated);
    }

    expectUnauthenticated(null);
    expectUnauthenticated({});
    expectUnauthenticated({ get: () => [] });

    const original = callerMetadata();

    expectUnauthenticated({
        get(name) {
            if (name === "x-service-token") {
                return [Buffer.from("binary-token")];
            }

            return original.get(name);
        },
    });

    console.log(
        "Invalid and duplicate caller data rejected: OK"
    );
} catch (error) {
    // Do not print assertion values that may contain credentials.
    console.error(
        "Shipment caller check failed:",
        error.code ?? error.name
    );

    process.exitCode = 1;
}