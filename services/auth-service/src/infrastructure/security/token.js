import jwt from "jsonwebtoken";
import { DomainError } from "../../domain/errors.js";
import { jwtConfig } from "../config/env.js";

const ROLES = new Set(["CUSTOMER", "ADMIN", "SHIPMENT_STAFF"]);

export function issueAccessToken(user) {
    if (
        !Number.isInteger(user.uid) ||
        user.uid < 1 ||
        user.uid > 2147483647 ||
        !ROLES.has(user.role)
    ) {
        throw new DomainError("INVALID_ARGUMENT", "Invalid token subject or role");
    }

    const accessToken = jwt.sign(
        { role: user.role },
        jwtConfig.secret,
        {
            algorithm: "HS256",
            subject: String(user.uid),
            expiresIn: jwtConfig.expiresIn,
            issuer: jwtConfig.issuer,
            audience: jwtConfig.audience,
        }
    );

    return {
        accessToken,
        tokenType: "Bearer",
        expiresIn: jwtConfig.expiresIn,
    };
}

export function verifyAccessToken(token) {
    try {
        const payload = jwt.verify(token, jwtConfig.secret, {
            algorithms: ["HS256"],
            issuer: jwtConfig.issuer,
            audience: jwtConfig.audience,
        });

        if (
            typeof payload !== "object" ||
            typeof payload.sub !== "string" ||
            !/^[1-9]\d*$/.test(payload.sub) ||
            Number(payload.sub) > 2147483647 ||
            !ROLES.has(payload.role)
        ) {
            throw new Error("Invalid claims");
        }

        return payload;
    } catch {
        throw new DomainError("UNAUTHENTICATED", "Invalid or expired token");
    }
}