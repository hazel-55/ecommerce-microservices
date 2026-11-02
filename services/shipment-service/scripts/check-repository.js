import assert from "node:assert/strict";
import { randomInt, randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
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
import {
    createShipmentIdAllocator,
} from "../src/infrastructure/database/shipment-id.js";
import {
    createShipmentRepository,
} from "../src/infrastructure/repositories/shipment.repository.js";

const fixtureId = randomUUID();
const counterId = `check_shipment_repo_${fixtureId}`;

const eventIds = [
    randomUUID(),
    randomUUID(),
    randomUUID(),
];

function event(eventId) {
    return {
        eventId,
        eventType: "OrderCreated",
        version: 1,
        oid: 1001,
        uid: 13,
        discount: "0.00",
        items: [
            { pid: 1, qty: 1, unit_price: "10.00" },
        ],
    };
}

async function expectDomain(operation, code) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, code);
        return true;
    });
}

async function freeFixtureBase() {
    for (let attempt = 0; attempt < 10; attempt += 1) {
        const base = randomInt(
            1_000_000_000,
            2147483647 - 1000
        );

        const occupied = await ShipmentModel.exists({
            shipid: {
                $gte: base + 1,
                $lte: base + 64,
            },
        });

        if (!occupied) {
            return base;
        }
    }

    throw new Error("Could not find fixture ID range");
}

async function main() {
    await connectDatabase();
    await ShipmentModel.createIndexes();

    const base = await freeFixtureBase();

    await CounterModel.create({
        _id: counterId,
        seq: base,
    });

    const repository = createShipmentRepository({
        allocateId: createShipmentIdAllocator({
            counterId,
        }),
    });

    const firstEvent = {
        ...event(eventIds[0]),
        shipid: 999,
        status: "DELIVERED",
    };

    const created = await repository.createFromOrderEvent(
        firstEvent
    );

    assert.equal(created.created, true);

    assert.deepEqual(created.shipment, {
        shipid: base + 1,
        oid: 1001,
        uid: 13,
        sourceEventId: eventIds[0],
        status: "PENDING",
    });

    assert.deepEqual(
        await repository.findById(created.shipment.shipid),
        created.shipment
    );

    assert.deepEqual(
        await repository.findBySourceEventId(eventIds[0]),
        created.shipment
    );

    assert.equal(
        await repository.findById(base + 63),
        null
    );

    assert.equal(
        await repository.findBySourceEventId(randomUUID()),
        null
    );

    console.log(
        "Repository creates PENDING shipment and reads by IDs: OK"
    );

    // Simulate a valid status change on this fixture.
    await ShipmentModel.updateOne(
        { sourceEventId: eventIds[0] },
        { $set: { status: "SHIPPING" } },
        { runValidators: true }
    );

    const counterBeforeReplay = await CounterModel.findById(
        counterId
    ).lean();

    const replay = await repository.createFromOrderEvent(
        firstEvent
    );

    assert.equal(replay.created, false);
    assert.equal(
        replay.shipment.shipid,
        created.shipment.shipid
    );
    assert.equal(replay.shipment.status, "SHIPPING");

    assert.equal(
        (await CounterModel.findById(counterId).lean()).seq,
        counterBeforeReplay.seq
    );

    console.log(
        "Repeated OrderCreated event preserves existing shipment: OK"
    );

    const beforeConflict = await repository.findBySourceEventId(
        eventIds[0]
    );

    await expectDomain(
        () => repository.createFromOrderEvent({
            ...event(eventIds[0]),
            oid: 1002,
        }),
        "CONFLICT"
    );

    await expectDomain(
        () => repository.createFromOrderEvent({
            ...event(eventIds[0]),
            uid: 14,
        }),
        "CONFLICT"
    );

    assert.deepEqual(
        await repository.findBySourceEventId(eventIds[0]),
        beforeConflict
    );

    console.log(
        "Event identity cannot change order or owner: OK"
    );

    const results = await Promise.allSettled(
        Array.from(
            { length: 12 },
            () => repository.createFromOrderEvent(
                event(eventIds[1])
            )
        )
    );

    const failed = results.find(
        (result) => result.status === "rejected"
    );

    if (failed) {
        throw failed.reason;
    }

    const successful = results.map(
        (result) => result.value
    );

    assert.equal(
        successful.filter((result) => result.created).length,
        1
    );

    const shipmentIds = new Set(
        successful.map(
            (result) => result.shipment.shipid
        )
    );

    assert.equal(shipmentIds.size, 1);

    assert.notEqual(
        successful[0].shipment.shipid,
        created.shipment.shipid
    );

    assert.equal(
        await ShipmentModel.countDocuments({
            sourceEventId: eventIds[1],
        }),
        1
    );

    assert.equal(successful[0].shipment.oid, 1001);

    console.log(
        "Concurrent event replay creates one shipment: OK"
    );

    const counterBeforeInvalid = await CounterModel.findById(
        counterId
    ).lean();

    const invalid = event(eventIds[2]);
    delete invalid.oid;

    await expectDomain(
        () => repository.createFromOrderEvent(invalid),
        "INVALID_ARGUMENT"
    );

    await expectDomain(
        () => repository.findById("1"),
        "INVALID_ARGUMENT"
    );

    await expectDomain(
        () => repository.findBySourceEventId("invalid"),
        "INVALID_ARGUMENT"
    );

    assert.equal(
        (await CounterModel.findById(counterId).lean()).seq,
        counterBeforeInvalid.seq
    );

    assert.equal(
        await ShipmentModel.countDocuments({
            sourceEventId: eventIds[2],
        }),
        0
    );

    console.log(
        "Invalid event rejected before allocation or persistence: OK"
    );
}

async function cleanup() {
    await ShipmentModel.deleteMany({
        sourceEventId: { $in: eventIds },
    });

    await CounterModel.deleteOne({
        _id: counterId,
    });

    assert.equal(
        await ShipmentModel.countDocuments({
            sourceEventId: { $in: eventIds },
        }),
        0
    );

    assert.equal(
        await CounterModel.countDocuments({
            _id: counterId,
        }),
        0
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Shipment repository check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        if (ShipmentModel.db.readyState === 1) {
            await cleanup();

            console.log(
                "Scoped Shipment repository fixtures removed: OK"
            );
        }
    } catch (error) {
        console.error(
            "Shipment repository cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture event IDs:", eventIds);
        console.error("Fixture counter ID:", counterId);
        process.exitCode = 1;
    } finally {
        await disconnectDatabase();
    }
}