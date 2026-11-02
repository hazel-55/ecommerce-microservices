import assert from "node:assert/strict";
import {
    connectDatabase,
    disconnectDatabase,
} from "../src/infrastructure/database/mongoose.js";

try {
    const connection = await connectDatabase();

    assert.equal(connection.readyState, 1);

    console.log("Shipment MongoDB connection: OK");

    assert.equal(connection.name, "shipment_db");

    console.log("Shipment database name: OK");

    const ping = await connection.db.command(
        { ping: 1 },
        { maxTimeMS: 5000 }
    );

    assert.equal(ping.ok, 1);

    console.log("Shipment MongoDB ping: OK");

    const hello = await connection.db.admin().command(
        { hello: 1 },
        { maxTimeMS: 5000 }
    );

    assert.equal(hello.isWritablePrimary, true);

    console.log("Shipment writable primary: OK");
} catch (error) {
    console.error(
        "Shipment database check failed:",
        error.code ?? error.name
    );

    process.exitCode = 1;
} finally {
    try {
        await disconnectDatabase();
    } catch (error) {
        console.error(
            "Shipment database disconnect failed:",
            error.code ?? error.name
        );

        process.exitCode = 1;
    }
}