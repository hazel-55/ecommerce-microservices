import mongoose from "mongoose";

const { Schema } = mongoose;
const MAX_ID = 2147483647;

const UUID_PATTERN =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function identifier(name) {
    return {
        type: Number,
        required: true,
        min: 1,
        max: MAX_ID,
        immutable: true,
        validate: {
            validator: Number.isInteger,
            message: `${name} must be an integer`,
        },
    };
}

const shipmentSchema = new Schema(
    {
        shipid: identifier("shipid"),
        oid: identifier("oid"),
        uid: identifier("uid"),

        sourceEventId: {
            type: String,
            required: true,
            immutable: true,
            match: UUID_PATTERN,
        },

        status: {
            type: String,
            required: true,
            default: "PENDING",
            enum: [
                "PENDING",
                "SHIPPING",
                "DELIVERED",
                "CANCELLED",
            ],
        },
    },
    {
        collection: "shipments",
        versionKey: false,
        timestamps: true,
        strict: "throw",
        autoCreate: false,
        autoIndex: false,
    }
);

shipmentSchema.index(
    { shipid: 1 },
    {
        unique: true,
        name: "shipment_shipid_unique",
    }
);

shipmentSchema.index(
    { sourceEventId: 1 },
    {
        unique: true,
        name: "shipment_source_event_unique",
    }
);

shipmentSchema.index(
    { oid: 1 },
    {
        name: "shipment_oid_idx",
    }
);

export const ShipmentModel = mongoose.model(
    "Shipment",
    shipmentSchema
);