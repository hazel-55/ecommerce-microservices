import { Router } from "express";
import {
    authenticate,
    requireRoles,
} from "../middleware/auth.middleware.js";
import {
    shipmentClient,
} from "../infrastructure/grpc/shipment.client.js";
import { HttpError } from "../errors/http-error.js";

export const shipmentRouter = Router();

shipmentRouter.use(authenticate);

function readShipid(value) {
    if (
        typeof value !== "string" ||
        !/^[1-9]\d*$/.test(value)
    ) {
        throw new HttpError(400, "Invalid shipment ID");
    }

    const shipid = Number(value);

    if (
        !Number.isInteger(shipid) ||
        shipid > 2147483647
    ) {
        throw new HttpError(400, "Invalid shipment ID");
    }

    return shipid;
}

shipmentRouter.get("/:shipid", async (req, res) => {
    const shipid = readShipid(req.params.shipid);

    const shipment = await shipmentClient.getShipment(
        shipid,
        req.auth
    );

    res.status(200).json(shipment);
});

shipmentRouter.patch(
    "/:shipid/status",
    requireRoles("SHIPMENT_STAFF", "ADMIN"),
    async (req, res) => {
        const shipid = readShipid(req.params.shipid);

        const shipment = await shipmentClient.updateShipmentStatus(
            shipid,
            req.body,
            req.auth
        );

        res.status(200).json(shipment);
    }
);