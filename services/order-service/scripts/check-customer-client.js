import assert from "node:assert/strict";
import { DomainError } from "../src/domain/errors.js";
import {
    customerClient,
    createCustomerClient,
} from "../src/infrastructure/grpc/customer.client.js";

const actor = {
    uid: 13,
    role: "CUSTOMER",
};

const invalidCredentialClient = createCustomerClient({
    serviceToken: "invalid-order-service-token",
});

async function expectDomainError(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);

        return true;
    });
}

async function main() {
    await expectDomainError(
        () => customerClient.getCustomer(0, actor),
        "INVALID_ARGUMENT"
    );

    await expectDomainError(
        () => customerClient.getCustomer(13, {
            uid: 0,
            role: "CUSTOMER",
        }),
        "UNAUTHENTICATED"
    );

    console.log("Order customer client input validation: OK");

    const customer = await customerClient.getCustomer(
        actor.uid,
        actor
    );

    assert.deepEqual(
        Object.keys(customer).sort(),
        ["uid", "fullname", "mid", "mname", "score"].sort()
    );

    assert.equal(customer.uid, actor.uid);
    assert.equal(typeof customer.fullname, "string");
    assert.ok(customer.fullname.length > 0);
    assert.ok(Number.isInteger(customer.mid));
    assert.ok(customer.mid > 0);
    assert.equal(typeof customer.mname, "string");
    assert.ok(Number.isInteger(customer.score));
    assert.ok(customer.score >= 0);

    console.log("Order to Customer live profile and score: OK");

    await expectDomainError(
        () => customerClient.getCustomer(14, actor),
        "FORBIDDEN"
    );

    // Kiểm tra hồ sơ không tồn tại bằng actor fixture cùng UID.
    const missingUid = 2147483647;

    await expectDomainError(
        () => customerClient.getCustomer(missingUid, {
            uid: missingUid,
            role: "CUSTOMER",
        }),
        "NOT_FOUND"
    );

    console.log("Order customer permission and NOT_FOUND mapping: OK");

    await expectDomainError(
        () => invalidCredentialClient.getCustomer(13, actor),
        "INTERNAL"
    );

    console.log("Order customer credential error mapping: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Order Customer client check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    customerClient.close();
    invalidCredentialClient.close();
}