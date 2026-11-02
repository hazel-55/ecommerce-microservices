import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { gatewayConfig } from "../../config/env.js";
import { HttpError } from "../../errors/http-error.js";

const definition = protoLoader.loadSync(
    fileURLToPath(new URL("../../../../../proto/auth.proto", import.meta.url)),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    },
);

const authPackage = grpc.loadPackageDefinition(definition)
    .ecommerce.auth.v1;

export function mapAuthRpcError(error) {
    switch (error.code) {
        case grpc.status.INVALID_ARGUMENT:
            return new HttpError(400, "Invalid authentication data");

        case grpc.status.UNAUTHENTICATED:
            return new HttpError(401, "Invalid username or password");

        case grpc.status.PERMISSION_DENIED:
            return new HttpError(403, "Forbidden");

        case grpc.status.ALREADY_EXISTS:
            return new HttpError(409, "Username or email already exists");

        case grpc.status.UNAVAILABLE:
        case grpc.status.DEADLINE_EXCEEDED:
            return new HttpError(503, "Authentication service unavailable");

        default:
            return new HttpError(500, "Authentication operation failed");
    }
}

function pickStringFields(input, fields) {
    if (input === null || typeof input !== "object" || Array.isArray(input)) {
        throw new HttpError(400, "Invalid authentication data");
    }

    const request = {};

    for (const field of fields) {
        if (typeof input[field] !== "string") {
            throw new HttpError(400, "Invalid authentication data");
        }

        request[field] = input[field];
    }

    return request;
}

export function createAuthClient({
    address = gatewayConfig.authGrpcAddress,
} = {}) {
    const client = new authPackage.AuthService(
        address,
        grpc.credentials.createInsecure(),
    );

    function invoke(method, request) {
        return new Promise((resolve, reject) => {
            client[method](
                request,
                new grpc.Metadata(),
                { deadline: new Date(Date.now() + 10_000) },
                (error, response) => {
                    if (error) {
                        reject(mapAuthRpcError(error));
                        return;
                    }

                    resolve(response);
                },
            );
        });
    }

    return {
        async register(input) {
            const request = pickStringFields(input, [
                "username",
                "password",
                "email",
                "phone",
            ]);

            return invoke("Register", request);
        },

        async login(input) {
            const request = pickStringFields(input, ["username", "password"]);
            return invoke("Login", request);
        },

        close() {
            client.close();
        },
    };
}

export const authClient = createAuthClient();