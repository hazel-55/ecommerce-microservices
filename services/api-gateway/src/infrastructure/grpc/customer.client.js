import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { gatewayConfig } from "../../config/env.js";
import { HttpError } from "../../errors/http-error.js";

const definition = protoLoader.loadSync(
    fileURLToPath(new URL("../../../../../proto/customer.proto", import.meta.url)),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    },
);

const customerPackage = grpc.loadPackageDefinition(definition)
    .ecommerce.customer.v1;

export function mapCustomerRpcError(error) {
    switch (error.code) {
        case grpc.status.INVALID_ARGUMENT:
            return new HttpError(400, "Invalid customer data");

        case grpc.status.UNAUTHENTICATED:
            return new HttpError(502, "Customer service authentication failed");

        case grpc.status.PERMISSION_DENIED:
            return new HttpError(403, "Forbidden");

        case grpc.status.NOT_FOUND:
            return new HttpError(404, "Customer or membership not found");

        case grpc.status.UNAVAILABLE:
        case grpc.status.DEADLINE_EXCEEDED:
            return new HttpError(503, "Customer service unavailable");

        default:
            return new HttpError(500, "Customer operation failed");
    }
}

function validateUid(uid) {
    if (
        !Number.isInteger(uid) ||
        uid < 1 ||
        uid > 2147483647
    ) {
        throw new HttpError(400, "Invalid customer ID");
    }
}

function createActorMetadata(actor) {
    if (
        !actor ||
        !Number.isInteger(actor.uid) ||
        actor.uid < 1 ||
        actor.uid > 2147483647 ||
        !["CUSTOMER", "ADMIN", "SHIPMENT_STAFF"].includes(actor.role)
    ) {
        throw new HttpError(401, "Invalid authenticated user");
    }

    const metadata = new grpc.Metadata();

    metadata.set("x-service-name", "api-gateway");
    metadata.set("x-service-token", gatewayConfig.customerGatewayToken);
    metadata.set("x-user-id", String(actor.uid));
    metadata.set("x-user-role", actor.role);

    return metadata;
}

export function createCustomerClient({
    address = gatewayConfig.customerGrpcAddress,
} = {}) {
    const client = new customerPackage.CustomerService(
        address,
        grpc.credentials.createInsecure(),
    );

    function invoke(method, request, actor) {
        const metadata = createActorMetadata(actor);

        return new Promise((resolve, reject) => {
            client[method](
                request,
                metadata,
                { deadline: new Date(Date.now() + 10_000) },
                (error, response) => {
                    if (error) {
                        reject(mapCustomerRpcError(error));
                        return;
                    }

                    resolve(response);
                },
            );
        });
    }

    return {
        async getCustomer(uid, actor) {
            validateUid(uid);
            return invoke("GetCustomer", { uid }, actor);
        },

        async updateCustomer(uid, input, actor) {
            validateUid(uid);

            if (
                input === null ||
                typeof input !== "object" ||
                Array.isArray(input) ||
                typeof input.fullname !== "string"
            ) {
                throw new HttpError(400, "Invalid customer data");
            }

            const request = {
                uid,
                fullname: input.fullname,
            };

            if (Object.hasOwn(input, "mid")) {
                if (
                    !Number.isInteger(input.mid) ||
                    input.mid < 1 ||
                    input.mid > 2147483647
                ) {
                    throw new HttpError(400, "Invalid membership ID");
                }

                request.mid = input.mid;
            }

            return invoke("UpdateCustomer", request, actor);
        },

        close() {
            client.close();
        },
    };
}

export const customerClient = createCustomerClient();