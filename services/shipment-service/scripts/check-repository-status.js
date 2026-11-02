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

const counterId = `check_shipment_status_${randomUUID()}`;

const eventIds = [
    randomUUID(),
    randomUUID(),
    randomUUID(),
];

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

        if (
            !(await ShipmentModel.exists({
                shipid: {
                    $gte: base + 1,
                    $lte: base + 64,
                },
            }))
        ) {
            return base;
        }
    }

    throw new Error("Could not find fixture ID range");
}

async function raceStatus(
    repository,
    shipid,
    expectedStatus,
    targets
) {
    const results = await Promise.allSettled(
        targets.map((nextStatus) =>
            repository.updateStatus(
                shipid,
                expectedStatus,
                nextStatus
            )
        )
    );

    const failed = results.find(
        (result) => result.status === "rejected"
    );

    if (failed) {
        throw failed.reason;
    }

    const values = results.map((result) => result.value);
    const winners = values.filter((value) => value !== null);

    assert.equal(winners.length, 1);
    assert.equal(
        values.filter((value) => value === null).length,
        1
    );

    const stored = await repository.findById(shipid);

    assert.equal(stored.status, winners[0].status);
    assert.ok(targets.includes(stored.status));

    return stored;
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

    const shipments = [];

    for (let index = 0; index < eventIds.length; index += 1) {
        const result = await repository.createFromOrderEvent({
            eventId: eventIds[index],
            eventType: "OrderCreated",
            version: 1,
            oid: 1001 + index,
            uid: 13,
            discount: "0.00",
            items: [
                { pid: 1, qty: 1, unit_price: "10.00" },
            ],
        });

        shipments.push(result.shipment);
    }

    const first = shipments[0];

    const updated = await repository.updateStatus(
        first.shipid,
        "PENDING",
        "SHIPPING"
    );

    assert.deepEqual(updated, {
        ...first,
        status: "SHIPPING",
    });

    console.log(
        "Status update preserves shipment identity and owner: OK"
    );

    // Give this fixture an old timestamp so an unintended write is visible.
    await ShipmentModel.collection.updateOne(
        { shipid: first.shipid },
        {
            $set: {
                updatedAt: new Date("2000-01-01T00:00:00Z"),
            },
        }
    );

    const beforeRepeat = await ShipmentModel.findOne({
        shipid: first.shipid,
    }).lean();

    assert.deepEqual(
        await repository.updateStatus(
            first.shipid,
            "SHIPPING",
            "SHIPPING"
        ),
        updated
    );

    assert.deepEqual(
        await ShipmentModel.findOne({
            shipid: first.shipid,
        }).lean(),
        beforeRepeat
    );

    console.log(
        "Repeated status update keeps stored timestamps: OK"
    );

    await repository.updateStatus(
        first.shipid,
        "SHIPPING",
        "DELIVERED"
    );

    const terminal = await repository.findById(first.shipid);

    await expectDomain(
        () => repository.updateStatus(
            first.shipid,
            "DELIVERED",
            "CANCELLED"
        ),
        "INVALID_ARGUMENT"
    );

    await expectDomain(
        () => repository.updateStatus(
            first.shipid,
            "INVALID",
            "SHIPPING"
        ),
        "INTERNAL"
    );

    await expectDomain(
        () => repository.updateStatus(
            first.shipid,
            "DELIVERED",
            "INVALID"
        ),
        "INVALID_ARGUMENT"
    );

    await expectDomain(
        () => repository.updateStatus(
            "invalid",
            "PENDING",
            "SHIPPING"
        ),
        "INVALID_ARGUMENT"
    );

    assert.equal(
        await repository.updateStatus(
            first.shipid,
            "PENDING",
            "CANCELLED"
        ),
        null
    );

    assert.equal(
        await repository.updateStatus(
            first.shipid,
            "PENDING",
            "PENDING"
        ),
        null
    );

    assert.equal(
        await repository.updateStatus(
            base + 63,
            "PENDING",
            "SHIPPING"
        ),
        null
    );

    assert.deepEqual(
        await repository.findById(first.shipid),
        terminal
    );

    console.log(
        "Invalid, terminal and stale status changes protected: OK"
    );

    await raceStatus(
        repository,
        shipments[1].shipid,
        "PENDING",
        ["SHIPPING", "CANCELLED"]
    );

    console.log(
        "Concurrent PENDING transitions have one winner: OK"
    );

    await repository.updateStatus(
        shipments[2].shipid,
        "PENDING",
        "SHIPPING"
    );

    await raceStatus(
        repository,
        shipments[2].shipid,
        "SHIPPING",
        ["DELIVERED", "CANCELLED"]
    );

    console.log(
        "Concurrent SHIPPING transitions settle once: OK"
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
        "Shipment status repository check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        if (ShipmentModel.db.readyState === 1) {
            await cleanup();

            console.log(
                "Scoped status repository fixtures removed: OK"
            );
        }
    } catch (error) {
        console.error(
            "Shipment status cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture event IDs:", eventIds);
        console.error("Fixture counter ID:", counterId);
        process.exitCode = 1;
    } finally {
        await disconnectDatabase();
    }
}