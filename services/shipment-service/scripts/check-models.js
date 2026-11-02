import assert from "node:assert/strict";
import { randomInt, randomUUID } from "node:crypto";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import {
    ShipmentModel,
} from "../src/infrastructure/database/models/shipment.model.js";
import {
    CounterModel,
} from "../src/infrastructure/database/models/counter.model.js";

const MAX_ID = 2147483647;

const eventIds = [
    randomUUID(),
    randomUUID(),
    randomUUID(),
];

const allocatedIds = new Set();

const validShipment = {
    shipid: 1,
    oid: 1,
    uid: 13,
    sourceEventId: eventIds[0],
};

async function expectValidation(Model, input) {
    await assert.rejects(
        () => new Model(input).validate(),
        (error) => error.name === "ValidationError"
    );
}

async function ensureCollection(Model) {
    try {
        await Model.createCollection();
    } catch (error) {
        if (error.code !== 48) {
            throw error;
        }
    }
}

async function fixtureShipId() {
    for (let attempt = 0; attempt < 10; attempt += 1) {
        const shipid = randomInt(1_000_000_000, MAX_ID + 1);

        if (
            !allocatedIds.has(shipid) &&
            !(await ShipmentModel.exists({ shipid }))
        ) {
            allocatedIds.add(shipid);
            return shipid;
        }
    }

    throw new Error("Could not allocate fixture shipment ID");
}

async function main() {
    const defaultShipment = new ShipmentModel(validShipment);

    await defaultShipment.validate();
    assert.equal(defaultShipment.status, "PENDING");

    for (const status of [
        "PENDING",
        "SHIPPING",
        "DELIVERED",
        "CANCELLED",
    ]) {
        await new ShipmentModel({
            ...validShipment,
            status,
        }).validate();
    }

    for (const field of ["shipid", "oid", "uid"]) {
        for (const value of [
            0, -1, 1.5, MAX_ID + 1, NaN,
        ]) {
            await expectValidation(ShipmentModel, {
                ...validShipment,
                [field]: value,
            });
        }
    }

    for (const field of [
        "shipid",
        "oid",
        "uid",
        "sourceEventId",
    ]) {
        const invalid = { ...validShipment };
        delete invalid[field];

        await expectValidation(ShipmentModel, invalid);
    }

    await expectValidation(ShipmentModel, {
        ...validShipment,
        sourceEventId: "invalid",
    });

    await expectValidation(ShipmentModel, {
        ...validShipment,
        status: "UNKNOWN",
    });

    console.log(
        "Shipment model validation and default status: OK"
    );

    const counter = new CounterModel({
        _id: "shipment-model-check",
    });

    await counter.validate();
    assert.equal(counter.seq, 0);

    await new CounterModel({
        _id: "shipment-model-check",
        seq: MAX_ID,
    }).validate();

    for (const seq of [-1, 0.5, MAX_ID + 1, NaN]) {
        await expectValidation(CounterModel, {
            _id: "shipment-model-check",
            seq,
        });
    }

    await expectValidation(CounterModel, {
        _id: "",
        seq: 0,
    });

    console.log("Shipment counter model validation: OK");

    await connectDatabase();

    await ensureCollection(ShipmentModel);
    await ensureCollection(CounterModel);
    await ShipmentModel.createIndexes();

    const indexes = await ShipmentModel.collection
        .listIndexes()
        .toArray();

    assert.equal(
        indexes.find(
            (index) => index.name === "shipment_shipid_unique"
        )?.unique,
        true
    );

    assert.equal(
        indexes.find(
            (index) =>
                index.name === "shipment_source_event_unique"
        )?.unique,
        true
    );

    const firstId = await fixtureShipId();
    const secondId = await fixtureShipId();
    const oid = randomInt(1_000_000_000, MAX_ID + 1);

    await ShipmentModel.create({
        shipid: firstId,
        oid,
        uid: 13,
        sourceEventId: eventIds[0],
    });

    await assert.rejects(
        () => ShipmentModel.create({
            shipid: firstId,
            oid,
            uid: 13,
            sourceEventId: eventIds[1],
        }),
        (error) => error.code === 11000
    );

    await assert.rejects(
        () => ShipmentModel.create({
            shipid: secondId,
            oid,
            uid: 13,
            sourceEventId: eventIds[0],
        }),
        (error) => error.code === 11000
    );

    console.log(
        "Shipment unique IDs and event dedup indexes: OK"
    );

    await ShipmentModel.create({
        shipid: secondId,
        oid,
        uid: 13,
        sourceEventId: eventIds[2],
    });

    const oidIndex = indexes.find(
        (index) => index.name === "shipment_oid_idx"
    );

    assert.ok(oidIndex);
    assert.notEqual(oidIndex.unique, true);

    assert.equal(
        await ShipmentModel.countDocuments({
            oid,
            sourceEventId: { $in: eventIds },
        }),
        2
    );

    console.log(
        "Order reference supports multiple distinct events: OK"
    );

    assert.equal(CounterModel.collection.name, "counters");

    const counterIndexes = await CounterModel.collection
        .listIndexes()
        .toArray();

    assert.ok(
        counterIndexes.some(
            (index) =>
                index.name === "_id_" &&
                index.key._id === 1
        )
    );

    console.log("Counter collection and ID index: OK");
}

async function cleanup() {
    await ShipmentModel.deleteMany({
        sourceEventId: { $in: eventIds },
    });

    assert.equal(
        await ShipmentModel.countDocuments({
            sourceEventId: { $in: eventIds },
        }),
        0
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Shipment model check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        if (ShipmentModel.db.readyState === 1) {
            await cleanup();
            console.log("Scoped model fixtures removed: OK");
        }
    } catch (error) {
        console.error(
            "Shipment model cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture event IDs:", eventIds);
        process.exitCode = 1;
    } finally {
        await disconnectDatabase();
    }
}