import assert from "node:assert/strict";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";

try {
    const connection = await connectDatabase();

    assert.equal(connection.readyState, 1);
    console.log("Product MongoDB connection: OK");

    assert.equal(connection.db.databaseName, "product_db");
    console.log("Product database name: OK");

    const ping = await connection.db.command(
        { ping: 1 },
        { maxTimeMS: 5000 },
    );

    assert.equal(ping.ok, 1);
    console.log("Product MongoDB ping: OK");

    const hello = await connection.db.admin().command(
        { hello: 1 },
        { maxTimeMS: 5000 },
    );

    assert.equal(hello.setName, "product-rs");
    assert.equal(hello.isWritablePrimary, true);

    console.log("Product replica set and writable primary: OK");
} catch (error) {
    console.error("Product database check failed:", {
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