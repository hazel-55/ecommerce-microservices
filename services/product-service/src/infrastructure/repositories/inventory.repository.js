import { DomainError } from "../../domain/errors.js";
import { normalizePrice } from "../../domain/product.rules.js";
import {
    normalizeReservationRequest,
    normalizeStockCommand,
} from "../../domain/inventory.rules.js";
import { ProductModel } from "../database/models/product.model.js";
import { StockReservationModel } from "../database/models/stock-reservation.model.js";

const MAX_ID = 2147483647;

function toReservation(document) {
    return {
        operationId: document._id,
        uid: document.uid,
        status: document.status,
        items: document.items.map((item) => ({
            pid: item.pid,
            qty: item.qty,
            unit_price: item.unit_price,
        })),
    };
}

function assertOwner(reservation, uid) {
    if (reservation.uid !== uid) {
        throw new DomainError(
            "FORBIDDEN",
            "Reservation belongs to another customer"
        );
    }
}

function assertSameRequest(reservation, request) {
    const storedItems = reservation.items
        .map((item) => ({
            pid: item.pid,
            qty: item.qty,
        }))
        .sort((first, second) => first.pid - second.pid);

    if (
        JSON.stringify(storedItems) !==
        JSON.stringify(request.items)
    ) {
        throw new DomainError(
            "CONFLICT",
            "Operation ID already used with different items"
        );
    }
}

export function createInventoryRepository({
    productModel = ProductModel,
    reservationModel = StockReservationModel,
} = {}) {
    if (productModel.db !== reservationModel.db) {
        throw new Error(
            "Products and reservations must use the same database connection"
        );
    }
    async function finishReservation(input, targetStatus) {
        const command = normalizeStockCommand(input);
        const session = await productModel.db.startSession();

        try {
            for (let attempt = 0; attempt < 3; attempt++) {
                try {
                    return await session.withTransaction(
                        async () => {
                            const existing = await reservationModel
                                .findById(command.operationId)
                                .session(session)
                                .lean();

                            if (!existing) {
                                if (targetStatus === "CONFIRMED") {
                                    throw new DomainError(
                                        "NOT_FOUND",
                                        "Reservation not found"
                                    );
                                }

                                const [created] =
                                    await reservationModel.create(
                                        [{
                                            _id: command.operationId,
                                            uid: command.uid,
                                            status: "RELEASED",
                                            items: [],
                                        }],
                                        { session }
                                    );

                                return toReservation(created);
                            }

                            assertOwner(existing, command.uid);

                            if (existing.status === targetStatus) {
                                return toReservation(existing);
                            }

                            if (existing.status !== "RESERVED") {
                                throw new DomainError(
                                    "CONFLICT",
                                    "Reservation is already in a terminal state"
                                );
                            }

                            for (const item of existing.items) {
                                const filter = {
                                    pid: item.pid,
                                    reservedQuantity: { $gte: item.qty },
                                };

                                const increments = {
                                    reservedQuantity: -item.qty,
                                };

                                if (targetStatus === "RELEASED") {
                                    filter.quantity = {
                                        $gte: 0,
                                        $lte: MAX_ID - item.qty,
                                    };

                                    increments.quantity = item.qty;
                                }

                                const changed = await productModel.updateOne(
                                    filter,
                                    { $inc: increments },
                                    { session }
                                );

                                if (changed.matchedCount !== 1) {
                                    throw new DomainError(
                                        "INTERNAL",
                                        "Reserved stock is inconsistent"
                                    );
                                }
                            }

                            const updated = await reservationModel
                                .findOneAndUpdate(
                                    {
                                        _id: command.operationId,
                                        uid: command.uid,
                                        status: "RESERVED",
                                    },
                                    { $set: { status: targetStatus } },
                                    {
                                        session,
                                        returnDocument: "after",
                                        runValidators: true,
                                    }
                                )
                                .lean();

                            if (!updated) {
                                throw new DomainError(
                                    "CONFLICT",
                                    "Reservation state changed"
                                );
                            }

                            return toReservation(updated);
                        },
                        {
                            readConcern: { level: "snapshot" },
                            writeConcern: { w: "majority" },
                            readPreference: "primary",
                        }
                    );
                } catch (error) {
                    const duplicateOperation =
                        error.code === 11000 &&
                        error.keyPattern?._id === 1;

                    if (!duplicateOperation) {
                        throw error;
                    }

                    const existing = await reservationModel.exists({
                        _id: command.operationId,
                    });

                    if (!existing) {
                        throw error;
                    }
                }
            }

            throw new DomainError(
                "SERVICE_UNAVAILABLE",
                "Reservation finalization retry limit reached"
            );
        } finally {
            await session.endSession();
        }
    }

    return {
        async confirm(input) {
            return finishReservation(input, "CONFIRMED");
        },

        async release(input) {
            return finishReservation(input, "RELEASED");
        },
        async findByOperationId(input) {
            const command = normalizeStockCommand(input);

            const reservation = await reservationModel
                .findById(command.operationId)
                .lean();

            if (!reservation) {
                return null;
            }

            assertOwner(reservation, command.uid);

            return toReservation(reservation);
        },

        async reserve(input) {
            const request = normalizeReservationRequest(input);
            const session = await productModel.db.startSession();

            try {
                for (let attempt = 0; attempt < 3; attempt++) {
                    try {
                        return await session.withTransaction(
                            async () => {
                                const existing = await reservationModel
                                    .findById(request.operationId)
                                    .session(session)
                                    .lean();

                                if (existing) {
                                    assertOwner(existing, request.uid);

                                    if (existing.status === "RELEASED") {
                                        throw new DomainError(
                                            "CONFLICT",
                                            "Reservation has already been released"
                                        );
                                    }

                                    if (
                                        !["RESERVED", "CONFIRMED"]
                                            .includes(existing.status)
                                    ) {
                                        throw new DomainError(
                                            "INTERNAL",
                                            "Invalid reservation state"
                                        );
                                    }

                                    assertSameRequest(existing, request);

                                    return toReservation(existing);
                                }

                                const snapshots = [];

                                for (const item of request.items) {
                                    const product = await productModel
                                        .findOneAndUpdate(
                                            {
                                                pid: item.pid,
                                                quantity: { $gte: item.qty },
                                                $or: [
                                                    {
                                                        reservedQuantity: {
                                                            $exists: false,
                                                        },
                                                    },
                                                    {
                                                        reservedQuantity: {
                                                            $gte: 0,
                                                            $lte: MAX_ID - item.qty,
                                                        },
                                                    },
                                                ],
                                            },
                                            {
                                                $inc: {
                                                    quantity: -item.qty,
                                                    reservedQuantity: item.qty,
                                                },
                                            },
                                            {
                                                session,
                                                returnDocument: "after",
                                                runValidators: true,
                                                upsert: false,
                                            }
                                        )
                                        .lean();

                                    if (!product) {
                                        const exists = await productModel
                                            .findOne({ pid: item.pid })
                                            .session(session)
                                            .lean();

                                        if (!exists) {
                                            throw new DomainError(
                                                "NOT_FOUND",
                                                "Product not found"
                                            );
                                        }

                                        throw new DomainError(
                                            "CONFLICT",
                                            "Insufficient stock or invalid reserved quantity"
                                        );
                                    }

                                    snapshots.push({
                                        pid: item.pid,
                                        qty: item.qty,
                                        unit_price: normalizePrice(
                                            product.price.toString()
                                        ),
                                    });
                                }

                                const [created] = await reservationModel.create(
                                    [{
                                        _id: request.operationId,
                                        uid: request.uid,
                                        status: "RESERVED",
                                        items: snapshots,
                                    }],
                                    { session }
                                );

                                return toReservation(created);
                            },
                            {
                                readConcern: { level: "snapshot" },
                                writeConcern: { w: "majority" },
                                readPreference: "primary",
                            }
                        );
                    } catch (error) {
                        const duplicateOperation =
                            error.code === 11000 &&
                            error.keyPattern?._id === 1;

                        if (!duplicateOperation) {
                            throw error;
                        }

                        const existing = await reservationModel.exists({
                            _id: request.operationId,
                        });

                        if (!existing) {
                            throw error;
                        }

                        // Transaction trước đã abort.
                        // Transaction mới sẽ đọc reservation đã tồn tại.
                    }
                }

                throw new DomainError(
                    "SERVICE_UNAVAILABLE",
                    "Reservation retry limit reached"
                );
            } finally {
                await session.endSession();
            }
        },
    };
}

export const inventoryRepository = createInventoryRepository();