import assert from "node:assert/strict";
import jwt from "jsonwebtoken";
import { gatewayConfig } from "../src/config/env.js";
import { authClient } from "../src/infrastructure/grpc/auth.client.js";
import { productClient } from "../src/infrastructure/grpc/product.client.js";

async function expectHttpError(operation, status) {
    await assert.rejects(operation, (error) => {
        assert.equal(error.status, status);
        return true;
    });
}

async function main() {
    const login = await authClient.login({
        username: "http_8678dd4139734fe18d85dee359c76237",
        password: "P@ss123",
    });

    const payload = jwt.verify(
        login.accessToken,
        gatewayConfig.jwtSecret,
        {
            algorithms: ["HS256"],
            issuer: gatewayConfig.jwtIssuer,
            audience: gatewayConfig.jwtAudience,
        }
    );

    assert.equal(typeof payload, "object");
    assert.notEqual(payload, null);
    assert.equal(payload.sub, "13");
    assert.equal(payload.role, "CUSTOMER");

    const actor = {
        uid: Number(payload.sub),
        role: payload.role,
    };

    console.log("Product actor from verified JWT: OK");

    await expectHttpError(
        () => productClient.getProduct(0, actor),
        400
    );

    await expectHttpError(
        () => productClient.createProduct(null, actor),
        400
    );

    await expectHttpError(
        () => productClient.createProduct(
            {
                pname: "Invalid price fixture",
                price: "25",
                quantity: 1,
            },
            actor
        ),
        400
    );

    console.log("Product client input validation: OK");

    const result = await productClient.listProducts(actor);

    assert.ok(Array.isArray(result.items));

    for (const product of result.items) {
        assert.deepEqual(
            Object.keys(product).sort(),
            ["pid", "pname", "price", "quantity"].sort()
        );

        assert.ok(Number.isInteger(product.pid));
        assert.ok(product.pid > 0);
        assert.equal(typeof product.pname, "string");
        assert.equal(typeof product.price, "string");
        assert.ok(Number.isInteger(product.quantity));
        assert.ok(product.quantity >= 0);
    }

    console.log("Gateway to Product live read: OK");

    await expectHttpError(
        () => productClient.createProduct(
            {
                pname: "Must not be created",
                price: 25,
                quantity: 1,
                role: "ADMIN",
            },
            actor
        ),
        403
    );

    console.log("Product permission error mapping: OK");
}

try {
    await main();
} catch (error) {
    console.error(
        "Product client check failed:",
        error.message
    );

    process.exitCode = 1;
} finally {
    authClient.close();
    productClient.close();
}