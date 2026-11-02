import { CounterModel } from "./models/counter.model.js";
import { DomainError } from "../../domain/errors.js";

const MAX_ID = 2147483647;

export function createShipmentIdAllocator({
    model = CounterModel,
    counterId = "shipment",
} = {}) {
    if (
        typeof counterId !== "string" ||
        counterId.length < 1 ||
        counterId.length > 100 ||
        counterId.trim() !== counterId
    ) {
        throw new DomainError(
            "INVALID_ARGUMENT",
            "Invalid shipment counter name"
        );
    }

    return async function allocateShipmentId() {
        try {
            await model.updateOne(
                { _id: counterId },
                {
                    $setOnInsert: {
                        seq: 0,
                    },
                },
                {
                    upsert: true,
                    runValidators: true,
                }
            );
        } catch (error) {
            // Another allocator may have created this counter first.
            if (error.code !== 11000) {
                throw error;
            }
        }

        const counter = await model.findOneAndUpdate(
            {
                _id: counterId,
                seq: {
                    $gte: 0,
                    $lt: MAX_ID,
                },
            },
            {
                $inc: {
                    seq: 1,
                },
            },
            {
                returnDocument: "after",
                upsert: false,
                runValidators: true,
            }
        );

        if (!counter) {
            const current = await model.findById(
                counterId
            ).lean();

            if (current?.seq === MAX_ID) {
                throw new DomainError(
                    "CONFLICT",
                    "Shipment ID limit reached"
                );
            }

            throw new DomainError(
                "INTERNAL",
                "Shipment counter is unavailable"
            );
        }

        if (
            !Number.isInteger(counter.seq) ||
            counter.seq < 1 ||
            counter.seq > MAX_ID
        ) {
            throw new DomainError(
                "INTERNAL",
                "Invalid shipment ID counter"
            );
        }

        return counter.seq;
    };
}

export const nextShipmentId = createShipmentIdAllocator();