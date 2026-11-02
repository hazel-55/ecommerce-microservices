import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { DomainError } from "../../domain/errors.js";
import { productApplication } from "../../application/product.application.js";
import {
    readGatewayActor,
    readOrderActor,
} from "./caller.js";
import {
    inventoryApplication,
} from "../../application/inventory.application.js";
const definition = protoLoader.loadSync(
    fileURLToPath(new URL("../../../../../proto/product.proto", import.meta.url)),
    {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    },
);

const productPackage = grpc.loadPackageDefinition(definition)
    .ecommerce.product.v1;

const errorCodes = {
    INVALID_ARGUMENT: grpc.status.INVALID_ARGUMENT,
    UNAUTHENTICATED: grpc.status.UNAUTHENTICATED,
    FORBIDDEN: grpc.status.PERMISSION_DENIED,
    NOT_FOUND: grpc.status.NOT_FOUND,
    CONFLICT: grpc.status.ABORTED,
    SERVICE_UNAVAILABLE: grpc.status.UNAVAILABLE,
};
function stockResponse(reservation) {
    return {
        operationId: reservation.operationId,
        uid: reservation.uid,
        status: reservation.status,
        items: reservation.items.map((item) => ({
            pid: item.pid,
            qty: item.qty,
            unitPrice: item.unit_price,
        })),
    };
}
export function createGrpcServer(logger) {
    const server = new grpc.Server();

    function handler(operation, readActor = readGatewayActor) {
        return async (call, callback) => {
            try {
                const actor = readActor(call.metadata);
                const result = await operation(call.request, actor);

                callback(null, result);
            } catch (error) {
                const code = error instanceof DomainError
                    ? errorCodes[error.code]
                    : undefined;

                if (code !== undefined) {
                    callback({
                        code,
                        details: error.message,
                    });
                    return;
                }

                logger.error({
                    errorName: error.name,
                    code: error.code ?? "UNKNOWN",
                }, "Product operation failed");

                callback({
                    code: grpc.status.INTERNAL,
                    details: "Product operation failed",
                });
            }
        };
    }

    server.addService(productPackage.ProductService.service, {
        ListProducts: handler((request, actor) =>
            productApplication.listProducts(actor)),

        GetProduct: handler((request, actor) =>
            productApplication.getProduct(request.pid, actor)),

        CreateProduct: handler((request, actor) =>
            productApplication.createProduct(request, actor)),

        UpdateProduct: handler((request, actor) =>
            productApplication.updateProduct(request, actor)),

        DeleteProduct: handler((request, actor) =>
            productApplication.deleteProduct(request.pid, actor)),
        ReserveStock: handler(
            (request, actor) =>
                inventoryApplication
                    .reserveStock(request, actor)
                    .then(stockResponse),
            readOrderActor
        ),

        ConfirmStock: handler(
            (request, actor) =>
                inventoryApplication
                    .confirmStock(request, actor)
                    .then(stockResponse),
            readOrderActor
        ),

        ReleaseStock: handler(
            (request, actor) =>
                inventoryApplication
                    .releaseStock(request, actor)
                    .then(stockResponse),
            readOrderActor
        ),

        GetStockReservation: handler(
            (request, actor) =>
                inventoryApplication
                    .getStockReservation(request, actor)
                    .then(stockResponse),
            readOrderActor
        ),
    });

    return server;
}