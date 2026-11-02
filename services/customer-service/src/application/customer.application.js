import { DomainError } from "../domain/errors.js";
import {
    validateId,
    normalizeFullname,
    assertCustomerAccess,
    assertMembershipChange,
} from "../domain/customer.rules.js";
import {
    customerRepository,
} from "../infrastructure/repositories/customer.repository.js";

export function createCustomerApplication(repository = customerRepository) {
    async function createCustomer(input, caller) {
        if (caller?.service !== "auth-service") {
            throw new DomainError(
                "FORBIDDEN",
                "Only Auth Service can create customer profiles"
            );
        }

        const uid = validateId(input.uid);
        const fullname = normalizeFullname(input.fullname);

        return repository.createDefault({ uid, fullname });
    }

    async function getCustomer(uid, actor) {
        assertCustomerAccess(actor, uid);

        const customer = await repository.findByUid(uid);

        if (!customer) {
            throw new DomainError("NOT_FOUND", "Customer not found");
        }

        return customer;
    }

    async function updateCustomer(input, actor) {
        const { uid, mid } = input;

        assertCustomerAccess(actor, uid);
        const fullname = normalizeFullname(input.fullname);

        if (mid !== undefined) {
            validateId(mid, "mid");
        }

        const current = await repository.findByUid(uid);

        if (!current) {
            throw new DomainError("NOT_FOUND", "Customer not found");
        }

        assertMembershipChange(actor, current.mid, mid);

        if (actor.role === "ADMIN" && mid !== undefined) {
            const membership = await repository.findMembershipById(mid);

            if (!membership) {
                throw new DomainError("INVALID_ARGUMENT", "Membership not found");
            }
        }

        const customer = await repository.update(uid, {
            fullname,
            mid: actor.role === "ADMIN" ? mid : undefined,
        });

        if (!customer) {
            throw new DomainError("NOT_FOUND", "Customer not found");
        }

        return customer;
    }

    return { createCustomer, getCustomer, updateCustomer };
}

export const customerApplication = createCustomerApplication();