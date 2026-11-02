import mongoose from "mongoose";

const { Schema } = mongoose;

const counterSchema = new Schema(
    {
        _id: {
            type: String,
            required: true,
        },

        value: {
            type: Number,
            required: true,
            min: 0,
            max: 2147483647,
            validate: {
                validator: Number.isInteger,
                message: "Counter value must be an integer",
            },
        },
    },
    {
        collection: "counters",
        versionKey: false,
    },
);

export const CounterModel = mongoose.model("Counter", counterSchema);