import { DomainError } from "./errors.js";

function invalid(message) {
    throw new DomainError("INVALID_ARGUMENT", message);
}

function requireObject(input) {
    if (!input || typeof input !== "object" || Array.isArray(input)) {
        invalid("Request must be an object");
    }
}

function normalizeText(value, field, maxLength) {
    if (typeof value !== "string") {
        invalid(`${field} must be a string`);
    }

    const result = value.trim();

    if (result.length === 0 || result.length > maxLength) {
        invalid(`${field} must contain between 1 and ${maxLength} characters`);
    }

    return result;
}

export function validatePassword(value) {
    if (typeof value !== "string" || value.trim().length === 0) {
        invalid("password must be a non-empty string");
    }

    if (Buffer.byteLength(value, "utf8") > 72) {
        invalid("password must not exceed 72 UTF-8 bytes");
    }

    return value;
}

export function normalizeRegisterInput(input) {
    requireObject(input);

    const username = normalizeText(input.username, "username", 100);
    const password = validatePassword(input.password);
    const email = normalizeText(input.email, "email", 255).toLowerCase();
    const phone = normalizeText(input.phone, "phone", 30);

    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        invalid("email is invalid");
    }

    return { username, password, email, phone };
}

export function normalizeLoginInput(input) {
    requireObject(input);

    return {
        username: normalizeText(input.username, "username", 100),
        password: validatePassword(input.password),
    };
}