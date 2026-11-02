import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import { CounterModel } from "../src/infrastructure/database/models/counter.model.js";
import {
    createProductIdAllocator,
    MAX_PRODUCT_ID,
} from "../src/infrastructure/database/product-id.js";

const suffix = randomUUID();
const concurrentCounterId = `test-product-id-${suffix}`;
const boundaryCounterId = `test-product-id-boundary-${suffix}`;
const fixtureIds = [concurrentCounterId, boundaryCounterId];

try {
    await connectDatabase();

    const allocateA = createProductIdAllocator({
        counterId: concurrentCounterId,
    });

    const allocateB = createProductIdAllocator({
        counterId: concurrentCounterId,
    });

    const results = await Promise.allSettled(
        Array.from({ length: 20 }, (_, index) => {
            return index % 2 === 0 ? allocateA() : allocateB();
        }),
    );

    const ids = [];

    for (const result of results) {
        if (result.status === "rejected") {
            throw result.reason;
        }

        ids.push(result.value);
    }

    assert.equal(new Set(ids).size, 20);
    assert.deepEqual(
        ids.toSorted((a, b) => a - b),
        Array.from({ length: 20 }, (_, index) => index + 1),
    );

    console.log("Concurrent PID allocation: OK");

    const allocateFresh = createProductIdAllocator({
        counterId: concurrentCounterId,
    });

    assert.equal(await allocateFresh(), 21);

    const stored = await CounterModel.findById(
        concurrentCounterId,
    ).lean();

    assert.equal(stored.value, 21);

    console.log("Counter persists across allocator instances: OK");

    await CounterModel.create({
        _id: boundaryCounterId,
        value: MAX_PRODUCT_ID - 1,
    });

    const allocateBoundary = createProductIdAllocator({
        counterId: boundaryCounterId,
    });

    assert.equal(await allocateBoundary(), MAX_PRODUCT_ID);

    await assert.rejects(
        () => allocateBoundary(),
        (error) => {
            assert.equal(error.code, "PRODUCT_ID_EXHAUSTED");
            return true;
        },
    );

    const boundary = await CounterModel.findById(
        boundaryCounterId,
    ).lean();

    assert.equal(boundary.value, MAX_PRODUCT_ID);

    console.log("PID upper bound enforced: OK");
} catch (error) {
    console.error("Product ID check failed:", {
        errorName: error.name,
        code: error.code ?? "UNKNOWN",
    });

    if (error.code === "ERR_ASSERTION") {
        console.error(error.message);
    }

    process.exitCode = 1;
} finally {
    try {
        if (CounterModel.db.readyState === 1) {
            await CounterModel.deleteMany({
                _id: { $in: fixtureIds },
            });

            assert.equal(
                await CounterModel.countDocuments({
                    _id: { $in: fixtureIds },
                }),
                0,
            );

            console.log("Isolated counter fixtures removed: OK");
        }
    } catch (error) {
        console.error("Counter fixture cleanup failed:", {
            errorName: error.name,
            code: error.code ?? "UNKNOWN",
        });
        process.exitCode = 1;
    } finally {
        await disconnectDatabase();
    }
}