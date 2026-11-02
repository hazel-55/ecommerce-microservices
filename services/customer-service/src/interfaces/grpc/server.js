import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { DomainError } from "../../domain/errors.js";
import { customerApplication } from "../../application/customer.application.js";
import {
    readAuthCaller,
    readGatewayActor,
    readCustomerReader,
} from "./caller.js";

const protoPath = fileURLToPath(
    new URL("../../../../../proto/customer.proto", import.meta.url)
);

const definition = loadSync(protoPath, {
    includeDirs: [dirname(protoPath)],
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const proto = grpc.loadPackageDefinition(definition)
    .ecommerce.customer.v1;

const errorCodes = {
    INVALID_ARGUMENT: grpc.status.INVALID_ARGUMENT,
    UNAUTHENTICATED: grpc.status.UNAUTHENTICATED,
    FORBIDDEN: grpc.status.PERMISSION_DENIED,
    NOT_FOUND: grpc.status.NOT_FOUND,
};

export function createGrpcServer(logger) {
    const server = new grpc.Server();

    function handler(operation) {
        return async (call, callback) => {
            try {
                const result = await operation(call);
                callback(null, result);
            } catch (error) {
                if (error instanceof DomainError) {
                    callback({
                        code: errorCodes[error.code] ?? grpc.status.INTERNAL,
                        details: error.message,
                    });
                    return;
                }

                logger.error(
                    { code: error.code ?? "UNKNOWN" },
                    "Customer request failed"
                );

                callback({
                    code: grpc.status.INTERNAL,
                    details: "Internal server error",
                });
            }
        };
    }

    server.addService(proto.CustomerService.service, {
        CreateCustomer: handler((call) => {
            const caller = readAuthCaller(call.metadata);

            return customerApplication.createCustomer(call.request, caller);
        }),

        GetCustomer: handler((call) => {
            const actor = readCustomerReader(call.metadata);

            return customerApplication.getCustomer(
                call.request.uid,
                actor
            );
        }),
        UpdateCustomer: handler(async (call) => {
            const actor = readGatewayActor(call.metadata);
            const input = {
                uid: call.request.uid,
                fullname: call.request.fullname,
            };

            if (call.request._mid === "mid") {
                input.mid = call.request.mid;
            }

            const customer = await customerApplication.updateCustomer(input, actor);

            return {
                message: "Customer updated successfully",
                uid: customer.uid,
            };
        }),
    });

    return server;
}