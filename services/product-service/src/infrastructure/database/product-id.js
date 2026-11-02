import { CounterModel } from "./models/counter.model.js";

export const MAX_PRODUCT_ID = 2147483647;

function counterError(code, message) {
    const error = new Error(message);
    error.code = code;
    return error;
}

export function createProductIdAllocator({
    model = CounterModel,
    counterId = "product-pid",
} = {}) {
    return async function nextId() {
        // Chỉ đặt value = 0 khi tạo counter lần đầu.
        try {
            await model.updateOne(
                { _id: counterId },
                { $setOnInsert: { value: 0 } },
                {
                    upsert: true,
                    runValidators: true,
                },
            );
        } catch (error) {
            // Hai instance có thể cùng khởi tạo counter.
            if (error.code !== 11000) {
                throw error;
            }

            const existing = await model.exists({ _id: counterId });

            if (!existing) {
                throw error;
            }
        }

        const counter = await model.findOneAndUpdate(
            {
                _id: counterId,
                value: { $gte: 0, $lt: MAX_PRODUCT_ID },
            },
            {
                $inc: { value: 1 },
            },
            {
                returnDocument: "after",
                upsert: false,
            },
        );

        if (!counter) {
            const current = await model.findById(counterId).lean();

            if (current?.value === MAX_PRODUCT_ID) {
                throw counterError(
                    "PRODUCT_ID_EXHAUSTED",
                    "Product ID range exhausted",
                );
            }

            throw counterError(
                "PRODUCT_COUNTER_INVALID",
                "Product counter is missing or invalid",
            );
        }

        if (
            !Number.isInteger(counter.value) ||
            counter.value < 1 ||
            counter.value > MAX_PRODUCT_ID
        ) {
            throw counterError(
                "PRODUCT_COUNTER_INVALID",
                "Product counter is invalid",
            );
        }

        return counter.value;
    };
}

export const nextProductId = createProductIdAllocator();