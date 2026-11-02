import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { DomainError } from "../../domain/errors.js";
import { customerClientConfig } from "../config/env.js";

const definition = loadSync(
    fileURLToPath(
        new URL("../../../../../proto/customer.proto", import.meta.url)
    ),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    }
);

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.customer.v1;

export function createCustomerClient({
    address = customerClientConfig.address,
    token = customerClientConfig.token,
} = {}) {
    const client = new proto.CustomerService(
        address,
        grpc.credentials.createInsecure()
    );

    function createProfile(input) {
        const metadata = new grpc.Metadata();
        metadata.set("x-service-name", "auth-service");
        metadata.set("x-service-token", token);

        return new Promise((resolve, reject) => {
            client.CreateCustomer(
                input,
                metadata,
                { deadline: Date.now() + 5000 },
                (error, response) => {
                    if (!error) {
                        resolve(response);
                        return;
                    }

                    if (error.code === grpc.status.INVALID_ARGUMENT) {
                        reject(new DomainError("INVALID_ARGUMENT", error.details));
                        return;
                    }

                    reject(new DomainError(
                        "SERVICE_UNAVAILABLE",
                        "Unable to create customer profile"
                    ));
                }
            );
        });
    }

    return {
        createProfile,
        close() {
            client.close();
        },
    };
}

export const customerClient = createCustomerClient();