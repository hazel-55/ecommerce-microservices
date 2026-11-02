import assert from "node:assert/strict";
import grpc from "@grpc/grpc-js";
import { DomainError } from "../src/domain/errors.js";
import {
    orderTransportConfig,
} from "../src/infrastructure/config/env.js";
import {
    readGatewayActor,
} from "../src/interfaces/grpc/caller.js";

function metadata(overrides = {}) {
    const values = {
        "x-service-name": "api-gateway",
        "x-service-token": orderTransportConfig.gatewayToken,
        "x-user-id": "13",
        "x-user-role": "CUSTOMER",
        ...overrides,
    };

    const result = new grpc.Metadata();

    for (const [name, value] of Object.entries(values)) {
        if (value === null) {
            continue;
        }

        for (const item of (
            Array.isArray(value) ? value : [value]
        )) {
            result.add(name, item);
        }
    }

    return result;
}

function expectRejected(value) {
    assert.throws(
        () => readGatewayActor(value),
        (error) =>
            error instanceof DomainError &&
            error.code === "UNAUTHENTICATED",
    );
}

try {
    assert.deepEqual(
        readGatewayActor(metadata()),
        { uid: 13, role: "CUSTOMER" },
    );

    const token = orderTransportConfig.gatewayToken;

    const wrongSameLengthToken =
        token.slice(0, -1) +
        (token.endsWith("a") ? "b" : "a");

    for (const wrongToken of [
        "wrong",
        wrongSameLengthToken,
    ]) {
        expectRejected(metadata({
            "x-service-token": wrongToken,
        }));
    }

    for (const service of [
        "auth-service",
        "order-service",
        "API-GATEWAY",
        "",
    ]) {
        expectRejected(metadata({
            "x-service-name": service,
        }));
    }

    console.log("Gateway caller credentials: OK");

    for (const role of [
        "CUSTOMER",
        "ADMIN",
        "SHIPMENT_STAFF",
    ]) {
        assert.deepEqual(
            readGatewayActor(metadata({
                "x-user-role": role,
            })),
            { uid: 13, role },
        );
    }

    assert.deepEqual(
        readGatewayActor(metadata({
            "x-user-id": "2147483647",
        })),
        {
            uid: 2147483647,
            role: "CUSTOMER",
        },
    );

    console.log("Verified actor metadata: OK");

    for (const uid of [
        "",
        "0",
        "-1",
        "01",
        "1.5",
        "1e2",
        " 13",
        "13 ",
        "abc",
        "2147483648",
        "9007199254740993",
    ]) {
        expectRejected(metadata({
            "x-user-id": uid,
        }));
    }

    for (const role of [
        "",
        "customer",
        "ROOT",
    ]) {
        expectRejected(metadata({
            "x-user-role": role,
        }));
    }

    const validValues = {
        "x-service-name": "api-gateway",
        "x-service-token": token,
        "x-user-id": "13",
        "x-user-role": "CUSTOMER",
    };

    for (const [name, value] of Object.entries(validValues)) {
        expectRejected(metadata({
            [name]: null,
        }));

        expectRejected(metadata({
            [name]: [value, value],
        }));
    }

    expectRejected(undefined);
    expectRejected({});
    expectRejected({ get: () => [] });

    const valid = metadata();

    expectRejected({
        get(name) {
            return name === "x-user-id"
                ? [Buffer.from("13")]
                : valid.get(name);
        },
    });

    console.log(
        "Invalid and duplicate caller data rejected: OK",
    );
} catch (error) {
    process.exitCode = 1;

    console.error(
        "Order caller check failed:",
        error.code ?? error.name,
        error.message,
    );
}