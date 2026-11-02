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
    readOrderActor,
} from "../src/interfaces/grpc/caller.js";

function metadata({
    service = "order-service",
    token = shipmentTransportConfig.orderToken,
    uid = "13",
    role = "CUSTOMER",
} = {}) {
    const result = new grpc.Metadata();

    result.set("x-service-name", service);
    result.set("x-service-token", token);
    result.set("x-user-id", uid);
    result.set("x-user-role", role);

    return result;
}

function expectUnauthenticated(reader, value) {
    assert.throws(
        () => reader(value),
        (error) => {
            assert.ok(error instanceof DomainError);
            assert.equal(error.code, "UNAUTHENTICATED");
            return true;
        }
    );
}

try {
    const texts = await Promise.all([
        readFile(
            new URL("../.env", import.meta.url),
            "utf8"
        ),
        readFile(
            new URL("../../order-service/.env", import.meta.url),
            "utf8"
        ),
    ]);

    const files = texts.map((text) => dotenv.parse(text));
    const orderToken = shipmentTransportConfig.orderToken;

    assert.equal(typeof orderToken, "string");
    assert.ok(orderToken.length >= 32);

    for (const file of files) {
        assert.ok(file.SHIPMENT_ORDER_TOKEN === orderToken);

        assert.ok(
            file.SHIPMENT_GRPC_ADDRESS ===
            shipmentServerConfig.grpcAddress
        );
    }

    const otherTokens = files.flatMap((file) =>
        Object.entries(file)
            .filter(([key]) =>
                key.endsWith("_TOKEN") &&
                key !== "SHIPMENT_ORDER_TOKEN"
            )
            .map(([, value]) => value)
    );

    assert.equal(otherTokens.includes(orderToken), false);

    console.log(
        "Order to Shipment shared transport binding: OK"
    );

    for (const role of [
        "CUSTOMER",
        "ADMIN",
        "SHIPMENT_STAFF",
    ]) {
        assert.deepEqual(
            readOrderActor(metadata({ role })),
            { uid: 13, role }
        );
    }

    for (const uid of [1, 2147483647]) {
        assert.deepEqual(
            readOrderActor(metadata({ uid: String(uid) })),
            { uid, role: "CUSTOMER" }
        );
    }

    console.log(
        "Order caller credentials and actor metadata: OK"
    );

    const gatewayMetadata = metadata({
        service: "api-gateway",
        token: shipmentTransportConfig.gatewayToken,
    });

    assert.deepEqual(
        readGatewayActor(gatewayMetadata),
        { uid: 13, role: "CUSTOMER" }
    );

    expectUnauthenticated(readGatewayActor, metadata());
    expectUnauthenticated(readOrderActor, gatewayMetadata);

    expectUnauthenticated(
        readOrderActor,
        metadata({
            service: "order-service",
            token: shipmentTransportConfig.gatewayToken,
        })
    );

    expectUnauthenticated(
        readGatewayActor,
        metadata({
            service: "api-gateway",
            token: orderToken,
        })
    );

    console.log(
        "Gateway and Order credentials remain separated: OK"
    );

    expectUnauthenticated(
        readOrderActor,
        metadata({ service: "unknown-service" })
    );

    expectUnauthenticated(
        readOrderActor,
        metadata({ token: "invalid-token" })
    );

    const wrongSameLengthToken =
        orderToken.slice(0, -1) +
        (orderToken.endsWith("a") ? "b" : "a");

    expectUnauthenticated(
        readOrderActor,
        metadata({ token: wrongSameLengthToken })
    );

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
        expectUnauthenticated(
            readOrderActor,
            metadata({ uid })
        );
    }

    for (const role of ["", "customer", "ROOT"]) {
        expectUnauthenticated(
            readOrderActor,
            metadata({ role })
        );
    }

    for (const name of [
        "x-service-name",
        "x-service-token",
        "x-user-id",
        "x-user-role",
    ]) {
        const missing = metadata();
        missing.remove(name);

        expectUnauthenticated(readOrderActor, missing);

        const duplicated = metadata();
        duplicated.add(name, "duplicate");

        expectUnauthenticated(readOrderActor, duplicated);
    }

    expectUnauthenticated(readOrderActor, null);
    expectUnauthenticated(readOrderActor, {});
    expectUnauthenticated(readOrderActor, { get: () => [] });

    const original = metadata();

    expectUnauthenticated(readOrderActor, {
        get(name) {
            if (name === "x-user-id") {
                return [Buffer.from("13")];
            }

            return original.get(name);
        },
    });

    console.log(
        "Invalid and duplicate Order caller metadata rejected: OK"
    );
} catch (error) {
    // Avoid printing credential values from assertion failures.
    console.error(
        "Shipment Order caller check failed:",
        error.code ?? error.name
    );

    process.exitCode = 1;
}