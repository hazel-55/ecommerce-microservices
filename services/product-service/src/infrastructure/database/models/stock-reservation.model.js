import mongoose from "mongoose";

const { Schema } = mongoose;
const MAX_ID = 2147483647;

const UUID_PATTERN =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

const itemSchema = new Schema(
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

        qty: {
            type: Number,
            required: true,
            min: 1,
            max: MAX_ID,
            validate: {
                validator: Number.isInteger,
                message: "qty must be an integer",
            },
        },

        unit_price: {
            type: String,
            required: true,
            match: /^(0|[1-9]\d{0,7})\.\d{2}$/,
        },
    },
    { _id: false }
);

const stockReservationSchema = new Schema(
    {
        _id: {
            type: String,
            required: true,
            match: UUID_PATTERN,
        },

        uid: {
            type: Number,
            required: true,
            min: 1,
            max: MAX_ID,
            validate: {
                validator: Number.isInteger,
                message: "uid must be an integer",
            },
        },

        status: {
            type: String,
            required: true,
            enum: ["RESERVED", "CONFIRMED", "RELEASED"],
        },

        items: {
            type: [itemSchema],
            default: [],
            validate: {
                validator(items) {
                    if (
                        this.status !== "RELEASED" &&
                        items.length === 0
                    ) {
                        return false;
                    }

                    const productIds = items.map(
                        (item) => item.pid
                    );

                    return new Set(productIds).size === items.length;
                },
                message:
                    "Reservation must have valid, unique product items",
            },
        },
    },
    {
        collection: "stock_reservations",
        versionKey: false,
        timestamps: true,
    }
);

stockReservationSchema.index(
    { status: 1, createdAt: 1 },
    { name: "stock_reservation_status_created" }
);

export const StockReservationModel = mongoose.model(
    "StockReservation",
    stockReservationSchema
);