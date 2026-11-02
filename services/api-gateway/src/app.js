import { healthRouter } from "./routes/health.routes.js";
import { customerRouter } from "./routes/customer.routes.js";
import express from "express";
import pino from "pino";
import { randomUUID } from "node:crypto";
import { rateLimitConfig } from "./config/env.js";
import { authClient } from "./infrastructure/grpc/auth.client.js";
import { HttpError } from "./errors/http-error.js";
import { createRateLimiter } from "./middleware/rate-limit.middleware.js";
import { productRouter } from "./routes/product.routes.js";
import { orderRouter } from "./routes/order.routes.js";
import { shipmentRouter } from "./routes/shipment.routes.js";
const logger = pino();

export const app = express();

app.disable("x-powered-by");
app.set("trust proxy", false);

app.use((req, res, next) => {
    const requestId = randomUUID();
    res.setHeader("X-Request-Id", requestId);

    const startedAt = performance.now();

    res.on("finish", () => {
        logger.info({
            requestId,
            method: req.method,
            path: req.path,
            status: res.statusCode,
            durationMs: Math.round(performance.now() - startedAt),
        }, "HTTP request completed");
    });

    next();
});

app.use("/api/health", healthRouter);

app.use(createRateLimiter());

const authLimiter = createRateLimiter({
    scope: "auth",
    limit: rateLimitConfig.authMaxRequests,
});

app.use("/api/auth", authLimiter);
app.use(express.json({ limit: "16kb" }));

app.post("/api/auth/register", async (req, res) => {
    const result = await authClient.register(req.body);

    res.status(201).json({
        message: result.message,
        uid: result.uid,
    });
});

app.post("/api/auth/login", async (req, res) => {
    const result = await authClient.login(req.body);

    res.status(200).json({
        accessToken: result.accessToken,
        tokenType: result.tokenType,
        expiresIn: result.expiresIn,
    });
});
app.use("/api/customers", customerRouter);
app.use("/api/products", productRouter);
app.use("/api/orders", orderRouter);
app.use("/api/shipments", shipmentRouter);

app.use((req, res, next) => {
    next(new HttpError(404, "Route not found"));
});

app.use((error, req, res, next) => {
    if (res.headersSent) {
        return next(error);
    }

    let status = 500;
    let message = "Internal server error";

    if (error instanceof HttpError) {
        status = error.status;
        message = error.message;
    } else if (error.type === "entity.parse.failed") {
        status = 400;
        message = "Invalid JSON body";
    } else if (error.type === "entity.too.large") {
        status = 413;
        message = "Request body too large";
    }

    if (status >= 500) {
        logger.error({
            requestId: res.getHeader("X-Request-Id"),
            status,
        }, "HTTP request failed");
    }

    res.status(status).json({ message });
});