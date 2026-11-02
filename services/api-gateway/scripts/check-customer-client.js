import assert from "node:assert/strict";
import jwt from "jsonwebtoken";
import { gatewayConfig } from "../src/config/env.js";
import { authClient } from "../src/infrastructure/grpc/auth.client.js";
import { customerClient } from "../src/infrastructure/grpc/customer.client.js";

async function expectHttpError(operation, expectedStatus) {
    await assert.rejects(operation, (error) => {
        assert.equal(error.status, expectedStatus);
        return true;
    });
}

try {
    const login = await authClient.login({
        username: "http_8678dd4139734fe18d85dee359c76237",
        password: "P@ss123",
    });

    const payload = jwt.verify(
        login.accessToken,
        gatewayConfig.jwtSecret,
        {
            algorithms: ["HS256"],
            issuer: gatewayConfig.jwtIssuer,
            audience: gatewayConfig.jwtAudience,
        },
    );

    assert.equal(payload.sub, "13");
    assert.equal(payload.role, "CUSTOMER");

    const actor = {
        uid: Number(payload.sub),
        role: payload.role,
    };

    console.log("Customer test actor from verified JWT: OK");

    await expectHttpError(
        () => customerClient.getCustomer(0, actor),
        400,
    );

    await expectHttpError(
        () => customerClient.getCustomer(actor.uid, null),
        401,
    );

    await expectHttpError(
        () => customerClient.updateCustomer(actor.uid, null, actor),
        400,
    );

    await expectHttpError(
        () => customerClient.updateCustomer(
            actor.uid,
            { fullname: "Test", mid: "1" },
            actor,
        ),
        400,
    );

    console.log("Customer client input validation: OK");

    const customer = await customerClient.getCustomer(actor.uid, actor);

    assert.equal(customer.uid, actor.uid);
    assert.equal(typeof customer.fullname, "string");
    assert.equal(typeof customer.mname, "string");
    assert.ok(Number.isInteger(customer.mid));
    assert.ok(Number.isInteger(customer.score));

    console.log("Gateway to Customer live read: OK");
    console.log("Customer response fields: OK");
    console.log(`Test uid: ${customer.uid}`);
} catch (error) {
    console.error("Customer client check failed:", error.message);
    process.exitCode = 1;
} finally {
    authClient.close();
    customerClient.close();
}