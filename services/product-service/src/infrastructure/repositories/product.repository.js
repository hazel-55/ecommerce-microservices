import { ProductModel } from "../database/models/product.model.js";
import { nextProductId } from "../database/product-id.js";
import { DomainError } from "../../domain/errors.js";
function toProduct(document) {
    if (!document) {
        return null;
    }

    return {
        pid: document.pid,
        pname: document.pname,
        price: document.price.toString(),
        quantity: document.quantity,
    };
}

export function createProductRepository({
    model = ProductModel,
    allocateId = nextProductId,
} = {}) {
    return {
        async list() {
            const documents = await model.find().sort({ pid: 1 }).lean();

            return documents.map(toProduct);
        },

        async findById(pid) {
            const document = await model.findOne({ pid }).lean();

            return toProduct(document);
        },

        async create(input) {
            const pid = await allocateId();

            const document = await model.create({
                pid,
                pname: input.pname,
                price: input.price,
                quantity: input.quantity,
            });

            return toProduct(document);
        },

        async update(pid, input) {
            const document = await model.findOneAndUpdate(
                {
                    pid,
                    $or: [
                        { reservedQuantity: 0 },
                        { reservedQuantity: { $exists: false } },
                    ],
                },
                {
                    $set: {
                        pname: input.pname,
                        price: input.price,
                        quantity: input.quantity,
                    },
                },
                {
                    returnDocument: "after",
                    runValidators: true,
                    upsert: false,
                }
            );

            if (!document) {
                const existing = await model.exists({ pid });

                if (existing) {
                    throw new DomainError(
                        "CONFLICT",
                        "Product has active stock reservations"
                    );
                }

                return null;
            }

            return toProduct(document);
        },

        async deleteById(pid) {
            const document = await model.findOneAndDelete({
                pid,
                $or: [
                    { reservedQuantity: 0 },
                    { reservedQuantity: { $exists: false } },
                ],
            });

            if (!document) {
                const existing = await model.exists({ pid });

                if (existing) {
                    throw new DomainError(
                        "CONFLICT",
                        "Product has active stock reservations"
                    );
                }

                return null;
            }

            return toProduct(document);
        },
    };
}

export const productRepository = createProductRepository();