import assert from "node:assert/strict";
import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import {
    customerTransportConfig,
} from "../src/infrastructure/config/env.js";
import {
    readOrderActor,
} from "../src/interfaces/grpc/caller.js";

const protoPath = fileURLToPath(
    new URL("../../../proto/customer.proto", import.meta.url)
);

const definition = loadSync(protoPath, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.customer.v1;

const client = new proto.CustomerService(
    "127.0.0.1:50052",
    grpc.credentials.createInsecure()
);

function metadata({
    token = customerTransportConfig.orderToken,
    uid = "13",
    role = "CUSTOMER",
} = {}) {
    const result = new grpc.Metadata();

    result.set("x-service-name", "order-service");
    result.set("x-service-token", token);
    result.set("x-user-id", uid);
    result.set("x-user-role", role);

    return result;
}

function invoke(method, input, callerMetadata) {
    return new Promise((resolve, reject) => {
        client[method](
            input,
            callerMetadata,
            { deadline: new Date(Date.now() + 10000) },
            (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }

                resolve(response);
            }
        );
    });
}

async function expectGrpcError(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.equal(error.code, code);
        return true;
    });
}

async function main() {
    assert.deepEqual(readOrderActor(metadata()), {
        uid: 13,
        role: "CUSTOMER",
    });

    console.log("Order caller actor metadata: OK");

    const customer = await invoke(
        "getCustomer",
        { uid: 13 },
        metadata()
    );

    assert.deepEqual(
        Object.keys(customer).sort(),
        ["uid", "fullname", "mid", "mname", "score"].sort()
    );

    assert.equal(customer.uid, 13);
    assert.equal(typeof customer.fullname, "string");
    assert.ok(customer.fullname.length > 0);
    assert.ok(Number.isInteger(customer.mid));
    assert.ok(customer.mid > 0);
    assert.equal(typeof customer.mname, "string");
    assert.ok(Number.isInteger(customer.score));
    assert.ok(customer.score >= 0);

    console.log("Order caller live customer profile and score: OK");

    const duplicateUid = metadata();
    duplicateUid.add("x-user-id", "13");

    for (const invalidMetadata of [
        metadata({ token: "invalid-order-token" }),
        metadata({ role: "UNKNOWN" }),
        duplicateUid,
    ]) {
        await expectGrpcError(
            () => invoke(
                "getCustomer",
                { uid: 13 },
                invalidMetadata
            ),
            grpc.status.UNAUTHENTICATED
        );
    }

    console.log("Invalid Order caller credentials rejected: OK");

    await expectGrpcError(
        () => invoke(
            "getCustomer",
            { uid: 14 },
            metadata()
        ),
        grpc.status.PERMISSION_DENIED
    );

    console.log("Order caller cross-customer access forbidden: OK");

    await expectGrpcError(
        () => invoke(
            "createCustomer",
            { uid: 0, fullname: "Order caller test" },
            metadata()
        ),
        grpc.status.UNAUTHENTICATED
    );

    await expectGrpcError(
        () => invoke(
            "updateCustomer",
            { uid: 0, fullname: "Order caller test" },
            metadata()
        ),
        grpc.status.UNAUTHENTICATED
    );

    console.log("Order caller cannot create or update customers: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Order caller check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    client.close();
}