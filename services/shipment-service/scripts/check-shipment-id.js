import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import {
    CounterModel,
} from "../src/infrastructure/database/models/counter.model.js";
import {
    createShipmentIdAllocator,
} from "../src/infrastructure/database/shipment-id.js";

const MAX_ID = 2147483647;
const fixtureId = randomUUID();

const counterIds = [
    `check_shipid_${fixtureId}`,
    `check_shipid_limit_${fixtureId}`,
];

async function expectLimit(operation) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, "CONFLICT");
        return true;
    });
}

async function main() {
    await connectDatabase();

    try {
        await CounterModel.createCollection();
    } catch (error) {
        if (error.code !== 48) {
            throw error;
        }
    }

    const allocate = createShipmentIdAllocator({
        counterId: counterIds[0],
    });

    const ids = await Promise.all(
        Array.from({ length: 32 }, () => allocate())
    );

    assert.equal(new Set(ids).size, 32);

    assert.deepEqual(
        ids.sort((first, second) => first - second),
        Array.from({ length: 32 }, (_, index) => index + 1)
    );

    console.log("Concurrent Shipment ID allocation: OK");

    const anotherAllocator = createShipmentIdAllocator({
        counterId: counterIds[0],
    });

    const nextIds = await Promise.all([
        allocate(),
        anotherAllocator(),
    ]);

    assert.deepEqual(
        nextIds.sort((first, second) => first - second),
        [33, 34]
    );

    const stored = await CounterModel.findById(
        counterIds[0]
    ).lean();

    assert.equal(stored.seq, 34);

    console.log(
        "Shipment counter persists across allocator instances: OK"
    );

    await CounterModel.create({
        _id: counterIds[1],
        seq: MAX_ID - 1,
    });

    const allocateNearLimit = createShipmentIdAllocator({
        counterId: counterIds[1],
    });

    const results = await Promise.allSettled(
        Array.from(
            { length: 8 },
            () => allocateNearLimit()
        )
    );

    const fulfilled = results.filter(
        (result) => result.status === "fulfilled"
    );

    const rejected = results.filter(
        (result) => result.status === "rejected"
    );

    assert.equal(fulfilled.length, 1);
    assert.equal(fulfilled[0].value, MAX_ID);
    assert.equal(rejected.length, 7);

    for (const result of rejected) {
        assert.ok(result.reason instanceof DomainError);
        assert.equal(result.reason.code, "CONFLICT");
    }

    await expectLimit(() => allocateNearLimit());

    assert.equal(
        (
            await CounterModel.findById(
                counterIds[1]
            ).lean()
        ).seq,
        MAX_ID
    );

    for (const counterId of [
        "",
        " ",
        " trailing ",
        "a".repeat(101),
        null,
        123,
    ]) {
        assert.throws(
            () => createShipmentIdAllocator({ counterId }),
            (error) => {
                assert.ok(error instanceof DomainError);
                assert.equal(error.code, "INVALID_ARGUMENT");
                return true;
            }
        );
    }

    console.log(
        "Shipment ID upper bound and allocator input: OK"
    );
}

async function cleanup() {
    await CounterModel.deleteMany({
        _id: { $in: counterIds },
    });

    assert.equal(
        await CounterModel.countDocuments({
            _id: { $in: counterIds },
        }),
        0
    );
}

try {
    await main();
} catch (error) {
    console.error(
        "Shipment ID check failed:",
        error.code ?? error.message
    );

    process.exitCode = 1;
} finally {
    try {
        if (CounterModel.db.readyState === 1) {
            await cleanup();

            console.log(
                "Scoped Shipment counter fixtures removed: OK"
            );
        }
    } catch (error) {
        console.error(
            "Shipment counter cleanup failed:",
            error.code ?? error.message
        );

        console.error("Fixture counter IDs:", counterIds);
        process.exitCode = 1;
    } finally {
        await disconnectDatabase();
    }
}