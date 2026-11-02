import jwt from "jsonwebtoken";
import { gatewayConfig } from "../config/env.js";
import { HttpError } from "../errors/http-error.js";

const ROLES = new Set(["CUSTOMER", "ADMIN", "SHIPMENT_STAFF"]);

export function authenticate(req, res, next) {
    const authorization = req.headers.authorization;

    if (typeof authorization !== "string") {
        return next(new HttpError(401, "Bearer token required"));
    }

    const match = /^Bearer ([^\s]+)$/i.exec(authorization);

    if (!match) {
        return next(new HttpError(401, "Invalid authorization header"));
    }

    let payload;

    try {
        payload = jwt.verify(match[1], gatewayConfig.jwtSecret, {
            algorithms: ["HS256"],
            issuer: gatewayConfig.jwtIssuer,
            audience: gatewayConfig.jwtAudience,
        });

        if (
            typeof payload !== "object" ||
            typeof payload.sub !== "string" ||
            !/^[1-9]\d*$/.test(payload.sub) ||
            Number(payload.sub) > 2147483647 ||
            !ROLES.has(payload.role) ||
            typeof payload.exp !== "number" ||
            typeof payload.iat !== "number"
        ) {
            throw new Error("Invalid claims");
        }
    } catch {
        return next(new HttpError(401, "Invalid or expired token"));
    }

    req.auth = {
        uid: Number(payload.sub),
        role: payload.role,
    };

    next();
}

export function requireRoles(...allowedRoles) {
    return (req, res, next) => {
        if (!req.auth) {
            return next(new HttpError(401, "Authentication required"));
        }

        if (!allowedRoles.includes(req.auth.role)) {
            return next(new HttpError(403, "Insufficient permission"));
        }

        next();
    };
}