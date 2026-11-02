import assert from "node:assert/strict";
import bcrypt from "bcrypt";
import {
    hashPassword,
    verifyPassword,
} from "../src/infrastructure/security/password.js";

try {
    const password = "P@ss123";

    const firstHash = await hashPassword(password);
    const secondHash = await hashPassword(password);

    assert.notEqual(firstHash, password);
    assert.notEqual(firstHash, secondHash);

    assert.equal(bcrypt.getRounds(firstHash), 12);
    assert.equal(bcrypt.getRounds(secondHash), 12);

    assert.equal(await verifyPassword(password, firstHash), true);
    assert.equal(await verifyPassword(password, secondHash), true);
    assert.equal(await verifyPassword("WrongPassword", firstHash), false);

    await assert.rejects(
        () => hashPassword("é".repeat(37)),
        (error) => error.code === "INVALID_ARGUMENT"
    );

    console.log("Bcrypt password hashing: OK");
    console.log("Correct and incorrect password checks: OK");
    console.log("Random salt and cost checks: OK");
    console.log("Oversized password rejected: OK");
} catch (error) {
    console.error("Password check failed:", error.code ?? error.message);
    process.exitCode = 1;
}