import { Router } from "express";
import { authenticate } from "../middleware/auth.middleware.js";
import { HttpError } from "../errors/http-error.js";
import { customerClient } from "../infrastructure/grpc/customer.client.js";
import {
    getCachedCustomer,
    invalidateCustomerCache,
} from "../infrastructure/cache/customer.cache.js";

export const customerRouter = Router();

customerRouter.use((req, res, next) => {
    res.setHeader("Cache-Control", "no-store");
    next();
});

customerRouter.use(authenticate);

function checkCustomerAccess(req, res, next) {
    const uidText = req.params.uid;

    if (!/^[1-9]\d*$/.test(uidText)) {
        return next(new HttpError(400, "Invalid customer ID"));
    }

    const uid = Number(uidText);

    if (!Number.isInteger(uid) || uid > 2147483647) {
        return next(new HttpError(400, "Invalid customer ID"));
    }

    if (req.auth.role !== "ADMIN" && req.auth.uid !== uid) {
        return next(new HttpError(
            403,
            "Cannot access another customer's profile",
        ));
    }

    req.customerUid = uid;
    next();
}

customerRouter.get("/:uid", checkCustomerAccess, async (req, res, next) => {
    try {
        const { customer, cacheStatus } = await getCachedCustomer(
            req.customerUid,
            req.auth,
        );

        res.setHeader("X-Cache", cacheStatus);

        res.status(200).json({
            uid: customer.uid,
            fullname: customer.fullname,
            mid: customer.mid,
        });
    } catch (error) {
        next(error);
    }
});

customerRouter.put("/:uid", checkCustomerAccess, async (req, res, next) => {
    try {
        const result = await customerClient.updateCustomer(
            req.customerUid,
            req.body,
            req.auth,
        );

        await invalidateCustomerCache(req.customerUid);

        res.status(200).json({
            message: result.message,
            uid: result.uid,
        });
    } catch (error) {
        next(error);
    }
});