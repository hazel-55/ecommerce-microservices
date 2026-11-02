import assert from "node:assert/strict";
import jwt from "jsonwebtoken";
import { gatewayConfig } from "../src/config/env.js";
import {
    authenticate,
    requireRoles,
} from "../src/middleware/auth.middleware.js";

function run(middleware, request) {
    let called = false;
    let result;

    middleware(request, {}, (error) => {
        called = true;
        result = error;
    });

    assert.equal(called, true);
    return result;
}

const options = {
    algorithm: "HS256",
    subject: "1",
    issuer: gatewayConfig.jwtIssuer,
    audience: gatewayConfig.jwtAudience,
    expiresIn: 3600,
};

function requestFor(token) {
    return {
        headers: { authorization: `Bearer ${token}` },
    };
}

function expectUnauthorized(request) {
    assert.equal(run(authenticate, request).status, 401);
    assert.equal(request.auth, undefined);
}

try {
    const customerToken = jwt.sign(
        { role: "CUSTOMER" },
        gatewayConfig.jwtSecret,
        options
    );

    const customerRequest = requestFor(customerToken);

    assert.equal(run(authenticate, customerRequest), undefined);
    assert.deepEqual(customerRequest.auth, {
        uid: 1,
        role: "CUSTOMER",
    });

    expectUnauthorized({ headers: {} });
    expectUnauthorized(requestFor("invalid-token"));

    expectUnauthorized(requestFor(jwt.sign(
        { role: "CUSTOMER" },
        `${gatewayConfig.jwtSecret}_wrong`,
        options
    )));

    expectUnauthorized(requestFor(jwt.sign(
        { role: "CUSTOMER" },
        gatewayConfig.jwtSecret,
        { ...options, expiresIn: -1 }
    )));

    expectUnauthorized(requestFor(jwt.sign(
        { role: "CUSTOMER" },
        null,
        { ...options, algorithm: "none" }
    )));

    const { expiresIn, ...withoutExpiry } = options;

    expectUnauthorized(requestFor(jwt.sign(
        { role: "CUSTOMER" },
        gatewayConfig.jwtSecret,
        withoutExpiry
    )));

    assert.equal(
        run(requireRoles("ADMIN"), customerRequest).status,
        403
    );

    const adminToken = jwt.sign(
        { role: "ADMIN" },
        gatewayConfig.jwtSecret,
        { ...options, subject: "2" }
    );

    const adminRequest = requestFor(adminToken);

    assert.equal(run(authenticate, adminRequest), undefined);
    assert.equal(run(requireRoles("ADMIN"), adminRequest), undefined);

    assert.equal(
        run(requireRoles("ADMIN"), { headers: {} }).status,
        401
    );

    console.log("Gateway JWT validation: OK");
    console.log("Invalid, expired and unsigned tokens rejected: OK");
    console.log("Role authorization: OK");
} catch (error) {
    console.error("Middleware check failed:", error.message);
    process.exitCode = 1;
}