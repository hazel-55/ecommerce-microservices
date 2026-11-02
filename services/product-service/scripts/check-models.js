import assert from "node:assert/strict";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import { ProductModel } from "../src/infrastructure/database/models/product.model.js";
import { CounterModel } from "../src/infrastructure/database/models/counter.model.js";

const validProduct = {
    pid: 1,
    pname: "Test product",
    price: "1500.00",
    quantity: 10,
};

async function expectInvalidProduct(changes, field) {
    const product = new ProductModel({
        ...validProduct,
        ...changes,
    });

    await assert.rejects(
        () => product.validate(),
        (error) => {
            assert.equal(error.name, "ValidationError");
            assert.ok(
                error.errors[field],
                `Expected validation error for ${field}`,
            );
            return true;
        },
    );
}

async function ensureCollection(model) {
    try {
        await model.createCollection();
    } catch (error) {
        if (error.code !== 48) {
            throw error;
        }
    }
}

try {
    const product = new ProductModel(validProduct);

    await product.validate();
    assert.equal(product.price.toString(), "1500.00");

    await expectInvalidProduct({ pid: 0 }, "pid");
    await expectInvalidProduct({ pid: 1.5 }, "pid");
    await expectInvalidProduct({ pname: "   " }, "pname");
    await expectInvalidProduct({ price: "-1.00" }, "price");
    await expectInvalidProduct({ price: "1.001" }, "price");
    await expectInvalidProduct({ quantity: -1 }, "quantity");
    await expectInvalidProduct({ quantity: 1.5 }, "quantity");

    const zeroProduct = new ProductModel({
        ...validProduct,
        price: "0.00",
        quantity: 0,
    });

    await zeroProduct.validate();

    console.log("Product model validation: OK");

    const counter = new CounterModel({
        _id: "validation-only",
        value: 0,
    });

    await counter.validate();

    const invalidCounter = new CounterModel({
        _id: "validation-only",
        value: -1,
    });

    await assert.rejects(
        () => invalidCounter.validate(),
        (error) => {
            assert.equal(error.name, "ValidationError");
            assert.ok(error.errors.value);
            return true;
        },
    );

    console.log("Counter model validation: OK");

    await connectDatabase();

    await ensureCollection(ProductModel);
    await ensureCollection(CounterModel);
    await ProductModel.createIndexes();

    const productIndexes = await ProductModel.collection.listIndexes().toArray();
    const pidIndex = productIndexes.find(
        (index) => index.name === "product_pid_unique",
    );

    assert.ok(pidIndex);
    assert.equal(pidIndex.unique, true);
    assert.deepEqual(pidIndex.key, { pid: 1 });

    console.log("Product unique PID index: OK");

    const counterIndexes = await CounterModel.collection.listIndexes().toArray();
    const counterIdIndex = counterIndexes.find(
        (index) => index.name === "_id_",
    );

    assert.ok(counterIdIndex);
    assert.deepEqual(counterIdIndex.key, { _id: 1 });

    console.log("Counter collection and ID index: OK");
} catch (error) {
    console.error("Product model check failed:", {
        errorName: error.name,
        code: error.code ?? "UNKNOWN",
    });

    if (error.code === "ERR_ASSERTION") {
        console.error(error.message);
    }

    process.exitCode = 1;
} finally {
    await disconnectDatabase();
}