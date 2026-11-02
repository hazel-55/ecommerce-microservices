import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { DomainError } from "../../domain/errors.js";
import {
    validateId,
    assertOrderActor,
} from "../../domain/order.rules.js";
import {
    customerClientConfig,
} from "../config/env.js";
import { invokeUnary } from "./unary.js";

const protoPath = fileURLToPath(
    new URL("../../../../../proto/customer.proto", import.meta.url)
);

const definition = loadSync(protoPath, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.customer.v1;

function mapCustomerError(error) {
    switch (error.code) {
        case grpc.status.INVALID_ARGUMENT:
            return new DomainError(
                "INVALID_ARGUMENT",
                "Invalid customer request"
            );

        case grpc.status.PERMISSION_DENIED:
            return new DomainError(
                "FORBIDDEN",
                "Customer access forbidden"
            );

        case grpc.status.NOT_FOUND:
            return new DomainError(
                "NOT_FOUND",
                "Customer not found"
            );

        case grpc.status.UNAUTHENTICATED:
            return new DomainError(
                "INTERNAL",
                "Customer service authentication failed"
            );

        case grpc.status.CANCELLED:
        case grpc.status.UNAVAILABLE:
        case grpc.status.DEADLINE_EXCEEDED:
            return new DomainError(
                "SERVICE_UNAVAILABLE",
                "Customer service unavailable"
            );

        default:
            return new DomainError(
                "INTERNAL",
                "Customer operation failed"
            );
    }
}

export function createCustomerClient({
    address = customerClientConfig.grpcAddress,
    serviceToken = customerClientConfig.orderToken,
} = {}) {
    const client = new proto.CustomerService(
        address,
        grpc.credentials.createInsecure()
    );

    return {
        async getCustomer(
            uid,
            actor,
            { timeoutMs, deadline, signal } = {},
        ) {
            validateId(uid, "customer ID");
            assertOrderActor(actor);

            const metadata = new grpc.Metadata();

            metadata.set("x-service-name", "order-service");
            metadata.set("x-service-token", serviceToken);
            metadata.set("x-user-id", String(actor.uid));
            metadata.set("x-user-role", actor.role);

            try {
                return await invokeUnary({
                    client,
                    method: "getCustomer",
                    request: { uid },
                    metadata,
                    timeoutMs,
                    deadline,
                    signal,
                });
            } catch (error) {
                throw mapCustomerError(error);
            }
        },

        close() {
            client.close();
        },
    };
}

export const customerClient = createCustomerClient();