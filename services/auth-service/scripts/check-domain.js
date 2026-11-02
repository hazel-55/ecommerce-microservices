import assert from "node:assert/strict";
import {
    normalizeRegisterInput,
    normalizeLoginInput,
} from "../src/domain/auth.rules.js";

function expectInvalid(action) {
    assert.throws(
        action,
        (error) => error.code === "INVALID_ARGUMENT"
    );
}

const sample = {
    username: "  john  ",
    password: "P@ss123",
    email: " JOHN@EXAMPLE.COM ",
    phone: "0900000000",
    role: "ADMIN",
};

const registered = normalizeRegisterInput(sample);

assert.deepEqual(registered, {
    username: "john",
    password: "P@ss123",
    email: "john@example.com",
    phone: "0900000000",
});

assert.equal(Object.hasOwn(registered, "role"), false);

assert.deepEqual(
    normalizeLoginInput({ username: " john ", password: "P@ss123" }),
    { username: "john", password: "P@ss123" }
);

assert.equal(
    normalizeLoginInput({ username: "john", password: " P@ss123 " }).password,
    " P@ss123 "
);

expectInvalid(() => normalizeRegisterInput({
    ...sample,
    email: undefined,
}));

expectInvalid(() => normalizeLoginInput({
    username: "   ",
    password: "P@ss123",
}));

expectInvalid(() => normalizeLoginInput({
    username: "john",
    password: "",
}));

const accepted = normalizeLoginInput({
    username: "john",
    password: "é".repeat(36),
});

assert.equal(Buffer.byteLength(accepted.password, "utf8"), 72);

expectInvalid(() => normalizeLoginInput({
    username: "john",
    password: "é".repeat(37),
}));

console.log("Auth domain rules: OK");
console.log("Register and login input checks: OK");
console.log("Bcrypt password byte limit: OK");