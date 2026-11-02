import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { ProductModel } from "../src/infrastructure/database/models/product.model.js";
import { StockReservationModel } from "../src/infrastructure/database/models/stock-reservation.model.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";

const productInput = {
    pid: 1,
    pname: "Inventory model fixture",
    price: "25.50",
    quantity: 10,
};

const reservationInput = {
    _id: randomUUID(),
    uid: 13,
    status: "RESERVED",
    items: [
        { pid: 1, qty: 2, unit_price: "25.50" },
    ],
};

async function expectValidationError(document) {
    await assert.rejects(
        () => document.validate(),
        (error) => {
            assert.equal(error.name, "ValidationError");
            return true;
        }
    );
}

async function main() {
    const product = new ProductModel(productInput);

    await product.validate();
    assert.equal(product.reservedQuantity, 0);

    for (const reservedQuantity of [
        -1,
        1.5,
        2147483648,
    ]) {
        await expectValidationError(
            new ProductModel({
                ...productInput,
                reservedQuantity,
            })
        );
    }

    console.log("Product reserved quantity validation: OK");

    for (const status of [
        "RESERVED",
        "CONFIRMED",
        "RELEASED",
    ]) {
        const reservation = new StockReservationModel({
            ...reservationInput,
            status,
        });

        await reservation.validate();

        assert.equal(
            reservation.items[0].unit_price,
            "25.50"
        );
    }

    await new StockReservationModel({
        ...reservationInput,
        status: "RELEASED",
        items: [],
    }).validate();

    console.log("Reservation states and price snapshots: OK");

    for (const invalidInput of [
        { ...reservationInput, _id: "invalid-id" },
        { ...reservationInput, uid: 0 },
        { ...reservationInput, status: "UNKNOWN" },
        { ...reservationInput, items: [] },
        {
            ...reservationInput,
            status: "CONFIRMED",
            items: [],
        },
        {
            ...reservationInput,
            items: [
                { pid: 1, qty: 1, unit_price: "25.50" },
                { pid: 1, qty: 2, unit_price: "25.50" },
            ],
        },
        {
            ...reservationInput,
            items: [
                { pid: 1, qty: 0, unit_price: "25.50" },
            ],
        },
        {
            ...reservationInput,
            items: [
                { pid: 1, qty: 1, unit_price: "1.001" },
            ],
        },
    ]) {
        await expectValidationError(
            new StockReservationModel(invalidInput)
        );
    }

    console.log("Invalid reservation data rejected: OK");

    await connectDatabase();

    try {
        await StockReservationModel.createCollection();
    } catch (error) {
        if (error.code !== 48) {
            throw error;
        }
    }

    await StockReservationModel.createIndexes();

    assert.equal(
        StockReservationModel.collection.collectionName,
        "stock_reservations"
    );

    const indexes =
        await StockReservationModel.collection.indexes();

    const idIndex = indexes.find(
        (index) => index.name === "_id_"
    );

    assert.ok(idIndex);
    assert.deepEqual(idIndex.key, { _id: 1 });

    const statusIndex = indexes.find(
        (index) =>
            index.name === "stock_reservation_status_created"
    );

    assert.ok(statusIndex);

    assert.deepEqual(statusIndex.key, {
        status: 1,
        createdAt: 1,
    });

    assert.ok(
        indexes.every(
            (index) => index.expireAfterSeconds === undefined
        )
    );

    console.log("Reservation collection and indexes: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Inventory model check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    await disconnectDatabase();
}