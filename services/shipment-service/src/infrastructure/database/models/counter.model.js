import mongoose from "mongoose";

const { Schema } = mongoose;
const MAX_ID = 2147483647;

const counterSchema = new Schema(
    {
        _id: {
            type: String,
            required: true,
            maxlength: 100,
        },

        seq: {
            type: Number,
            required: true,
            default: 0,
            min: 0,
            max: MAX_ID,
            validate: {
                validator: Number.isInteger,
                message: "Counter sequence must be an integer",
            },
        },
    },
    {
        collection: "counters",
        versionKey: false,
        strict: "throw",
        autoCreate: false,
        autoIndex: false,
    }
);

export const CounterModel = mongoose.model(
    "ShipmentCounter",
    counterSchema
);