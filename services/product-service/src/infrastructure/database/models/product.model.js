import mongoose from "mongoose";

const { Schema } = mongoose;
const MAX_ID = 2147483647;

const productSchema = new Schema(
    {
        pid: {
            type: Number,
            required: true,
            min: 1,
            max: MAX_ID,
            validate: {
                validator: Number.isInteger,
                message: "pid must be an integer",
            },
        },

        pname: {
            type: String,
            required: true,
            trim: true,
            maxlength: 100,
        },

        price: {
            type: Schema.Types.Decimal128,
            required: true,
            validate: {
                validator(value) {
                    return value != null &&
                        /^\d{1,8}(?:\.\d{1,2})?$/.test(value.toString());
                },
                message: "price must be non-negative with at most 2 decimal places",
            },
        },

        quantity: {
            type: Number,
            required: true,
            min: 0,
            max: MAX_ID,
            validate: {
                validator: Number.isInteger,
                message: "quantity must be an integer",
            },
        },

        reservedQuantity: {
            type: Number,
            required: true,
            default: 0,
            min: 0,
            max: MAX_ID,
            validate: {
                validator: Number.isInteger,
                message: "reservedQuantity must be an integer",
            },
        },
    },
    {
        collection: "products",
        versionKey: false,
    },
);

productSchema.index(
    { pid: 1 },
    {
        unique: true,
        name: "product_pid_unique",
    },
);

export const ProductModel = mongoose.model("Product", productSchema);