import assert from "node:assert/strict";
import jwt from "jsonwebtoken";
import { jwtConfig } from "../src/infrastructure/config/env.js";
import {
    issueAccessToken,
    verifyAccessToken,
} from "../src/infrastructure/security/token.js";

function expectRejected(token) {
    assert.throws(
        () => verifyAccessToken(token),
        (error) => error.code === "UNAUTHENTICATED"
    );
}

try {
    const response = issueAccessToken({
        uid: 1,
        role: "CUSTOMER",
    });

    assert.equal(response.tokenType, "Bearer");
    assert.equal(response.expiresIn, 3600);

    const payload = verifyAccessToken(response.accessToken);

    assert.equal(payload.sub, "1");
    assert.equal(payload.role, "CUSTOMER");
    assert.equal(payload.exp - payload.iat, 3600);

    const options = {
        algorithm: "HS256",
        subject: "1",
        issuer: jwtConfig.issuer,
        audience: jwtConfig.audience,
    };

    const wrongSignature = jwt.sign(
        { role: "CUSTOMER" },
        `${jwtConfig.secret}_wrong`,
        { ...options, expiresIn: 3600 }
    );

    expectRejected(wrongSignature);

    const expired = jwt.sign(
        { role: "CUSTOMER" },
        jwtConfig.secret,
        { ...options, expiresIn: -1 }
    );

    expectRejected(expired);

    const unsigned = jwt.sign(
        { role: "CUSTOMER" },
        null,
        { ...options, algorithm: "none", expiresIn: 3600 }
    );

    expectRejected(unsigned);

    console.log("JWT issuance: OK");
    console.log("Subject, role and expiry: OK");
    console.log("Invalid, expired and unsigned tokens rejected: OK");
} catch (error) {
    console.error("JWT check failed:", error.code ?? error.message);
    process.exitCode = 1;
}