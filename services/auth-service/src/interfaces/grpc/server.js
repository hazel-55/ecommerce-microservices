import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { DomainError } from "../../domain/errors.js";
import { authApplication } from "../../application/auth.application.js";

const definition = loadSync(
    fileURLToPath(
        new URL("../../../../../proto/auth.proto", import.meta.url)
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
    .ecommerce.auth.v1;

const errorCodes = {
    INVALID_ARGUMENT: grpc.status.INVALID_ARGUMENT,
    UNAUTHENTICATED: grpc.status.UNAUTHENTICATED,
    ALREADY_EXISTS: grpc.status.ALREADY_EXISTS,
    SERVICE_UNAVAILABLE: grpc.status.UNAVAILABLE,
};

export function createGrpcServer(logger) {
    const server = new grpc.Server();

    function handler(operation) {
        return async (call, callback) => {
            try {
                const result = await operation(call.request);
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
                    { code: error?.code ?? "UNKNOWN" },
                    "Auth request failed"
                );

                callback({
                    code: grpc.status.INTERNAL,
                    details: "Internal server error",
                });
            }
        };
    }

    server.addService(proto.AuthService.service, {
        Register: handler(authApplication.register),
        Login: handler(authApplication.login),
    });

    return server;
}