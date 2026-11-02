import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { gatewayConfig } from "../../config/env.js";
import { HttpError } from "../../errors/http-error.js";

const protoPath = fileURLToPath(
    new URL("../../../../../proto/product.proto", import.meta.url)
);

const packageDefinition = protoLoader.loadSync(protoPath, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const productProto =
    grpc.loadPackageDefinition(packageDefinition)
        .ecommerce.product.v1;

const MAX_ID = 2147483647;
const ROLES = new Set(["CUSTOMER", "ADMIN", "SHIPMENT_STAFF"]);

function validatePid(pid) {
    if (
        !Number.isInteger(pid) ||
        pid < 1 ||
        pid > MAX_ID
    ) {
        throw new HttpError(400, "Invalid product ID");
    }
}

function actorMetadata(actor) {
    if (
        !actor ||
        !Number.isInteger(actor.uid) ||
        actor.uid < 1 ||
        actor.uid > MAX_ID ||
        !ROLES.has(actor.role)
    ) {
        throw new HttpError(401, "Authenticated user required");
    }

    const metadata = new grpc.Metadata();

    metadata.set("x-service-name", "api-gateway");
    metadata.set(
        "x-service-token",
        gatewayConfig.productGatewayToken
    );
    metadata.set("x-user-id", String(actor.uid));
    metadata.set("x-user-role", actor.role);

    return metadata;
}

function productInput(input) {
    if (
        !input ||
        typeof input !== "object" ||
        Array.isArray(input)
    ) {
        throw new HttpError(400, "Invalid product data");
    }

    if (typeof input.pname !== "string") {
        throw new HttpError(400, "Invalid product name");
    }

    const pname = input.pname.trim();

    if (pname.length < 1 || pname.length > 100) {
        throw new HttpError(400, "Invalid product name");
    }

    if (
        typeof input.price !== "number" ||
        !Number.isFinite(input.price)
    ) {
        throw new HttpError(400, "Price must be a valid number");
    }

    const priceText = String(input.price);

    if (
        !/^(0|[1-9]\d{0,7})(\.\d{1,2})?$/.test(priceText)
    ) {
        throw new HttpError(
            400,
            "Price must be between 0 and 99999999.99 with at most two decimal places"
        );
    }

    if (
        !Number.isInteger(input.quantity) ||
        input.quantity < 0 ||
        input.quantity > MAX_ID
    ) {
        throw new HttpError(400, "Invalid product quantity");
    }

    const [whole, fraction = ""] = priceText.split(".");

    return {
        pname,
        price: `${whole}.${fraction.padEnd(2, "0")}`,
        quantity: input.quantity,
    };
}

function mapProductRpcError(error) {
    switch (error.code) {
        case grpc.status.INVALID_ARGUMENT:
            return new HttpError(400, "Invalid product request");

        case grpc.status.UNAUTHENTICATED:
            return new HttpError(
                502,
                "Product service authentication failed"
            );

        case grpc.status.PERMISSION_DENIED:
            return new HttpError(403, "Insufficient permission");

        case grpc.status.NOT_FOUND:
            return new HttpError(404, "Product not found");

        case grpc.status.UNAVAILABLE:
        case grpc.status.DEADLINE_EXCEEDED:
            return new HttpError(503, "Product service unavailable");
        case grpc.status.ABORTED:
            return new HttpError(
                409,
                "Product has active stock reservations"
            );

        default:
            return new HttpError(500, "Product operation failed");
    }
}

export function createProductClient({
    address = gatewayConfig.productGrpcAddress,
} = {}) {
    const client = new productProto.ProductService(
        address,
        grpc.credentials.createInsecure()
    );

    function invoke(method, request, actor) {
        const metadata = actorMetadata(actor);

        return new Promise((resolve, reject) => {
            client[method](
                request,
                metadata,
                { deadline: new Date(Date.now() + 10000) },
                (error, response) => {
                    if (error) {
                        reject(mapProductRpcError(error));
                        return;
                    }

                    resolve(response);
                }
            );
        });
    }

    return {
        async listProducts(actor) {
            return invoke("listProducts", {}, actor);
        },

        async getProduct(pid, actor) {
            validatePid(pid);

            return invoke("getProduct", { pid }, actor);
        },

        async createProduct(input, actor) {
            const request = productInput(input);

            return invoke("createProduct", request, actor);
        },

        async updateProduct(pid, input, actor) {
            validatePid(pid);

            const request = {
                pid,
                ...productInput(input),
            };

            return invoke("updateProduct", request, actor);
        },

        async deleteProduct(pid, actor) {
            validatePid(pid);

            return invoke("deleteProduct", { pid }, actor);
        },

        close() {
            client.close();
        },
    };
}

export const productClient = createProductClient();