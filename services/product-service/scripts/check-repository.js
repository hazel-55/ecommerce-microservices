import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import { ProductModel } from "../src/infrastructure/database/models/product.model.js";
import { productRepository } from "../src/infrastructure/repositories/product.repository.js";

const suffix = randomUUID();
const names = {
    first: `test_${suffix}_A`,
    second: `test_${suffix}_B`,
    updated: `test_${suffix}_Updated`,
};
const fixtureNames = Object.values(names);

try {
    await connectDatabase();

    const first = await productRepository.create({
        pname: names.first,
        price: "25.50",
        quantity: 10,
    });

    const second = await productRepository.create({
        pname: names.second,
        price: "100.00",
        quantity: 5,
    });

    assert.ok(Number.isInteger(first.pid) && first.pid > 0);
    assert.ok(Number.isInteger(second.pid) && second.pid > 0);
    assert.notEqual(first.pid, second.pid);
    assert.equal(first.price, "25.50");

    assert.deepEqual(
        Object.keys(first).sort(),
        ["pid", "pname", "price", "quantity"],
    );

    assert.deepEqual(
        await productRepository.findById(first.pid),
        first,
    );

    const listed = await productRepository.list();

    assert.deepEqual(
        listed.find((item) => item.pid === first.pid),
        first,
    );
    assert.deepEqual(
        listed.find((item) => item.pid === second.pid),
        second,
    );

    console.log("Repository create, read and list: OK");

    await assert.rejects(
        () => ProductModel.create({
            pid: first.pid,
            pname: names.first,
            price: "25.50",
            quantity: 10,
        }),
        (error) => {
            assert.equal(error.code, 11000);
            return true;
        },
    );

    console.log("Duplicate PID rejected: OK");

    await assert.rejects(
        () => productRepository.update(first.pid, {
            pname: names.updated,
            price: "30.75",
            quantity: -1,
        }),
        (error) => {
            assert.equal(error.name, "ValidationError");
            assert.ok(error.errors.quantity);
            return true;
        },
    );

    assert.deepEqual(
        await productRepository.findById(first.pid),
        first,
    );

    const updated = await productRepository.update(first.pid, {
        pid: second.pid,
        pname: names.updated,
        price: "30.75",
        quantity: 0,
    });

    assert.deepEqual(updated, {
        pid: first.pid,
        pname: names.updated,
        price: "30.75",
        quantity: 0,
    });

    assert.deepEqual(
        await productRepository.findById(first.pid),
        updated,
    );
    assert.deepEqual(
        await productRepository.findById(second.pid),
        second,
    );

    console.log("Update validation and decimal price: OK");

    assert.deepEqual(
        await productRepository.deleteById(first.pid),
        updated,
    );

    assert.equal(await productRepository.findById(first.pid), null);
    assert.equal(await productRepository.deleteById(first.pid), null);

    assert.equal(
        await productRepository.update(first.pid, {
            pname: names.updated,
            price: "30.75",
            quantity: 0,
        }),
        null,
    );

    console.log("Delete and missing product behavior: OK");
} catch (error) {
    console.error("Product repository check failed:", {
        errorName: error.name,
        code: error.code ?? "UNKNOWN",
    });

    if (error.code === "ERR_ASSERTION") {
        console.error(error.message);
    }

    process.exitCode = 1;
} finally {
    try {
        if (ProductModel.db.readyState === 1) {
            await ProductModel.deleteMany({
                pname: { $in: fixtureNames },
            });

            assert.equal(
                await ProductModel.countDocuments({
                    pname: { $in: fixtureNames },
                }),
                0,
            );

            console.log("Scoped product fixtures removed: OK");
        }
    } catch (error) {
        console.error("Product fixture cleanup failed:", {
            errorName: error.name,
            code: error.code ?? "UNKNOWN",
        });

        process.exitCode = 1;
    } finally {
        await disconnectDatabase();
    }
}