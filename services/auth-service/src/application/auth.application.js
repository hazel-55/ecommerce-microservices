import { DomainError } from "../domain/errors.js";
import {
    normalizeRegisterInput,
    normalizeLoginInput,
} from "../domain/auth.rules.js";
import {
    userRepository,
} from "../infrastructure/repositories/user.repository.js";
import {
    provisioningRepository,
} from "../infrastructure/repositories/provisioning.repository.js";
import {
    provisioningApplication,
} from "./provisioning.application.js";
import {
    hashPassword,
    verifyPassword,
} from "../infrastructure/security/password.js";
import {
    issueAccessToken,
} from "../infrastructure/security/token.js";

export function createAuthApplication({
    users = userRepository,
    tasks = provisioningRepository,
    provisioning = provisioningApplication,
} = {}) {
    async function register(input) {
        const data = normalizeRegisterInput(input);
        let user = await users.findByUsername(data.username);

        if (user) {
            const profile = await users.findByUid(user.uid);
            const task = await tasks.findByUid(user.uid);

            const canResume =
                user.role === "CUSTOMER" &&
                task &&
                !task.completed_at &&
                profile.email === data.email &&
                profile.phone === data.phone &&
                await verifyPassword(data.password, user.password);

            if (!canResume) {
                throw new DomainError(
                    "ALREADY_EXISTS",
                    "Username or email already exists"
                );
            }
        } else {
            const passwordHash = await hashPassword(data.password);

            user = await users.createCustomerUser({
                username: data.username,
                passwordHash,
                email: data.email,
                phone: data.phone,
            });
        }

        await provisioning.provisionCustomer(user.uid);

        return {
            message: "User registered successfully",
            uid: user.uid,
        };
    }

    async function login(input) {
        const data = normalizeLoginInput(input);
        const user = await users.findByUsername(data.username);

        if (!user || !await verifyPassword(data.password, user.password)) {
            throw new DomainError(
                "UNAUTHENTICATED",
                "Invalid username or password"
            );
        }

        if (user.role === "CUSTOMER") {
            const task = await tasks.findByUid(user.uid);

            if (!task?.completed_at) {
                throw new DomainError(
                    "SERVICE_UNAVAILABLE",
                    "Customer profile is not ready; please retry later"
                );
            }
        }

        return issueAccessToken(user);
    }

    return { register, login };
}

export const authApplication = createAuthApplication();