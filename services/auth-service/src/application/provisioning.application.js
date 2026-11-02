import { DomainError } from "../domain/errors.js";
import {
    provisioningRepository,
} from "../infrastructure/repositories/provisioning.repository.js";
import {
    customerClient,
} from "../infrastructure/grpc/customer.client.js";

export function createProvisioningApplication({
    repository = provisioningRepository,
    client = customerClient,
} = {}) {
    async function provisionCustomer(uid) {
        if (!Number.isInteger(uid) || uid < 1 || uid > 2147483647) {
            throw new DomainError("INVALID_ARGUMENT", "Invalid uid");
        }

        const task = await repository.findByUid(uid);

        if (!task) {
            throw new DomainError("NOT_FOUND", "Provisioning task not found");
        }

        if (task.completed_at) {
            return { uid, ready: true };
        }

        try {
            const profile = await client.createProfile({
                uid,
                fullname: task.fullname,
            });

            if (profile.uid !== uid) {
                throw new DomainError(
                    "SERVICE_UNAVAILABLE",
                    "Customer returned an unexpected uid"
                );
            }
        } catch (error) {
            await repository.markFailed(
                uid,
                error instanceof DomainError ? error.code : "CUSTOMER_RPC_ERROR"
            );

            throw new DomainError(
                "SERVICE_UNAVAILABLE",
                "Customer profile is not ready; processing will be retried"
            );
        }

        const completed = await repository.markCompleted(uid);

        if (!completed?.completed_at) {
            throw new DomainError(
                "SERVICE_UNAVAILABLE",
                "Unable to confirm customer profile completion"
            );
        }

        return { uid, ready: true };
    }

    return { provisionCustomer };
}

export const provisioningApplication = createProvisioningApplication();