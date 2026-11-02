import bcrypt from "bcrypt";
import { validatePassword } from "../../domain/auth.rules.js";

const SALT_ROUNDS = 12;

export async function hashPassword(password) {
    validatePassword(password);

    return bcrypt.hash(password, SALT_ROUNDS);
}

export async function verifyPassword(password, storedHash) {
    validatePassword(password);

    return bcrypt.compare(password, storedHash);
}