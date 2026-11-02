import { Router } from "express";
import { randomUUID } from "node:crypto";
import {
    authenticate,
} from "../middleware/auth.middleware.js";
import {
    orderClient,
} from "../infrastructure/grpc/order.client.js";
import { HttpError } from "../errors/http-error.js";

export const orderRouter = Router();

orderRouter.use(authenticate);

const OPERATION_ID_PATTERN =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function readOperationId(req) {
    let headerCount = 0;

    for (
        let index = 0;
        index < req.rawHeaders.length;
        index += 2
    ) {
        if (
            req.rawHeaders[index].toLowerCase() ===
            "idempotency-key"
        ) {
            headerCount += 1;
        }
    }

    if (headerCount > 1) {
        throw new HttpError(
            400,
            "Duplicate Idempotency-Key header",
        );
    }

    const value = req.headers["idempotency-key"];

    if (value === undefined) {
        return randomUUID();
    }

    if (
        typeof value !== "string" ||
        !OPERATION_ID_PATTERN.test(value)
    ) {
        throw new HttpError(
            400,
            "Idempotency-Key must be a lowercase UUID",
        );
    }

    return value;
}

function readOid(value) {
    if (
        typeof value !== "string" ||
        !/^[1-9]\d*$/.test(value)
    ) {
        throw new HttpError(400, "Invalid order ID");
    }

    const oid = Number(value);

    if (
        !Number.isInteger(oid) ||
        oid > 2147483647
    ) {
        throw new HttpError(400, "Invalid order ID");
    }

    return oid;
}

function moneyResponse(value) {
    if (
        typeof value !== "string" ||
        !/^(0|[1-9]\d*)(?:\.\d{1,2})?$/.test(value)
    ) {
        throw new HttpError(
            502,
            "Invalid order service response",
        );
    }

    const amount = Number(value);

    if (!Number.isFinite(amount)) {
        throw new HttpError(
            502,
            "Invalid order service response",
        );
    }

    return amount;
}

function summaryResponse(order) {
    return {
        oid: order.oid,
        uid: order.uid,
        discount: moneyResponse(order.discount),
    };
}

function detailResponse(order) {
    return {
        ...summaryResponse(order),
        items: order.items.map((item) => ({
            pid: item.pid,
            qty: item.qty,
            unit_price: moneyResponse(item.unit_price),
        })),
    };
}

orderRouter.post("/", async (req, res) => {
    const operationId = readOperationId(req);

    res.setHeader("Idempotency-Key", operationId);

    const controller = new AbortController();

    function abortRequest() {
        controller.abort();
    }

    function handleResponseClose() {
        if (!res.writableFinished) {
            abortRequest();
        }
    }

    req.once("aborted", abortRequest);
    res.once("close", handleResponseClose);

    try {
        if (req.aborted || res.destroyed) {
            abortRequest();
            return;
        }

        const order = await orderClient.createOrder(
            operationId,
            req.body,
            req.auth,
            {
                signal: controller.signal,
            },
        );

        if (
            controller.signal.aborted ||
            res.destroyed
        ) {
            return;
        }

        res.status(201).json({
            ...detailResponse(order),
            shipment: {
                shipid: order.shipment.shipid,
                oid: order.shipment.oid,
                status: order.shipment.status,
            },
        });
    } finally {
        req.removeListener(
            "aborted",
            abortRequest,
        );

        res.removeListener(
            "close",
            handleResponseClose,
        );
    }
});

orderRouter.get("/", async (req, res) => {
    const result = await orderClient.listOrders(req.auth);

    res.status(200).json({
        items: result.items.map(summaryResponse),
    });
});

orderRouter.get("/:oid", async (req, res) => {
    const oid = readOid(req.params.oid);

    const order = await orderClient.getOrder(
        oid,
        req.auth,
    );

    res.status(200).json(detailResponse(order));
});