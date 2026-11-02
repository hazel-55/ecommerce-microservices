import { Router } from "express";
import {
    authenticate,
    requireRoles,
} from "../middleware/auth.middleware.js";
import { productClient } from "../infrastructure/grpc/product.client.js";
import { HttpError } from "../errors/http-error.js";

export const productRouter = Router();

productRouter.use(authenticate);

function readPid(value) {
    if (
        typeof value !== "string" ||
        !/^[1-9]\d*$/.test(value)
    ) {
        throw new HttpError(400, "Invalid product ID");
    }

    const pid = Number(value);

    if (
        !Number.isInteger(pid) ||
        pid > 2147483647
    ) {
        throw new HttpError(400, "Invalid product ID");
    }

    return pid;
}

function productResponse(product) {
    const price = Number(product.price);

    if (!Number.isFinite(price)) {
        throw new HttpError(502, "Invalid product service response");
    }

    return {
        pid: product.pid,
        pname: product.pname,
        price,
        quantity: product.quantity,
    };
}

productRouter.get("/", async (req, res) => {
    const result = await productClient.listProducts(req.auth);

    res.status(200).json({
        items: result.items.map(productResponse),
    });
});

productRouter.get("/:pid", async (req, res) => {
    const pid = readPid(req.params.pid);

    const product = await productClient.getProduct(
        pid,
        req.auth
    );

    res.status(200).json(productResponse(product));
});

productRouter.post(
    "/",
    requireRoles("ADMIN"),
    async (req, res) => {
        const product = await productClient.createProduct(
            req.body,
            req.auth
        );

        res.status(201).json(productResponse(product));
    }
);

productRouter.put(
    "/:pid",
    requireRoles("ADMIN"),
    async (req, res) => {
        const pid = readPid(req.params.pid);

        const result = await productClient.updateProduct(
            pid,
            req.body,
            req.auth
        );

        res.status(200).json({
            message: result.message,
            pid: result.pid,
        });
    }
);

productRouter.delete(
    "/:pid",
    requireRoles("ADMIN"),
    async (req, res) => {
        const pid = readPid(req.params.pid);

        const result = await productClient.deleteProduct(
            pid,
            req.auth
        );

        res.status(200).json({
            message: result.message,
            pid: result.pid,
        });
    }
);