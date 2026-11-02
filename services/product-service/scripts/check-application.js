import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DomainError } from "../src/domain/errors.js";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";
import { ProductModel } from "../src/infrastructure/database/models/product.model.js";
import { productApplication } from "../src/application/product.application.js";

const suffix = randomUUID();
const names = {
    initial: `app_${suffix}_Initial`,
    updated: `app_${suffix}_Updated`,
    forbidden: `app_${suffix}_Forbidden`,
};
const fixtureNames = Object.values(names);

const admin = { uid: 1, role: "ADMIN" };
const customer = { uid: 1, role: "CUSTOMER" };
const staff = { uid: 1, role: "SHIPMENT_STAFF" };

async function expectDomainError(operation, expectedCode) {
    await assert.rejects(operation, (error) => {
        assert.ok(error instanceof DomainError);
        assert.equal(error.code, expectedCode);
        return true;
    });
}

try {
    await connectDatabase();

    const product = await productApplication.createProduct(
        {
            pname: `  ${names.initial}  `,
            price: "25",
            quantity: 10,
        },
        admin,
    );

    assert.equal(product.pname, names.initial);
    assert.equal(product.price, "25.00");
    assert.ok(Number.isInteger(product.pid) && product.pid > 0);
    assert.deepEqual(
        Object.keys(product).sort(),
        ["pid", "pname", "price", "quantity"],
    );

    for (const actor of [admin, customer, staff]) {
        assert.deepEqual(
            await productApplication.getProduct(product.pid, actor),
            product,
        );

        const listed = await productApplication.listProducts(actor);

        assert.deepEqual(
            listed.items.find((item) => item.pid === product.pid),
            product,
        );
    }

    await expectDomainError(
        () => productApplication.listProducts(null),
        "UNAUTHENTICATED",
    );

    console.log("Application create, read and list: OK");

    for (const actor of [customer, staff]) {
        await expectDomainError(
            () => productApplication.createProduct(
                {
                    pname: names.forbidden,
                    price: "10",
                    quantity: 1,
                },
                actor,
            ),
            "FORBIDDEN",
        );

        await expectDomainError(
            () => productApplication.updateProduct(
                {
                    pid: product.pid,
                    pname: names.updated,
                    price: "30",
                    quantity: 0,
                },
                actor,
            ),
            "FORBIDDEN",
        );

        await expectDomainError(
            () => productApplication.deleteProduct(product.pid, actor),
            "FORBIDDEN",
        );
    }

    assert.deepEqual(
        await productApplication.getProduct(product.pid, admin),
        product,
    );

    assert.equal(
        await ProductModel.countDocuments({
            pname: { $in: fixtureNames },
        }),
        1,
    );

    console.log("Product mutation role permissions: OK");

    await expectDomainError(
        () => productApplication.updateProduct(
            {
                pid: product.pid,
                pname: names.updated,
                price: "30.50",
                quantity: -1,
            },
            admin,
        ),
        "INVALID_ARGUMENT",
    );

    assert.deepEqual(
        await productApplication.getProduct(product.pid, admin),
        product,
    );

    const updateResult = await productApplication.updateProduct(
        {
            pid: product.pid,
            pname: names.updated,
            price: "30.5",
            quantity: 0,
        },
        admin,
    );

    assert.deepEqual(updateResult, {
        message: "Product updated successfully",
        pid: product.pid,
    });

    assert.deepEqual(
        await productApplication.getProduct(product.pid, customer),
        {
            pid: product.pid,
            pname: names.updated,
            price: "30.50",
            quantity: 0,
        },
    );

    console.log("Update validation and normalization: OK");

    assert.deepEqual(
        await productApplication.deleteProduct(product.pid, admin),
        {
            message: "Product deleted successfully",
            pid: product.pid,
        },
    );

    await expectDomainError(
        () => productApplication.getProduct(product.pid, customer),
        "NOT_FOUND",
    );

    await expectDomainError(
        () => productApplication.updateProduct(
            {
                pid: product.pid,
                pname: names.updated,
                price: "30.50",
                quantity: 0,
            },
            admin,
        ),
        "NOT_FOUND",
    );

    await expectDomainError(
        () => productApplication.deleteProduct(product.pid, admin),
        "NOT_FOUND",
    );

    console.log("Product NOT_FOUND mapping: OK");
} catch (error) {
    console.error("Product application check failed:", {
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

            console.log("Scoped application fixtures removed: OK");
        }
    } catch (error) {
        console.error("Application fixture cleanup failed:", {
            errorName: error.name,
            code: error.code ?? "UNKNOWN",
        });
        process.exitCode = 1;
    } finally {
        await disconnectDatabase();
    }
}