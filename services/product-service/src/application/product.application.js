import { DomainError } from "../domain/errors.js";
import {
    validateId,
    normalizeProductInput,
    assertProductRead,
    assertProductAdmin,
} from "../domain/product.rules.js";
import { productRepository } from "../infrastructure/repositories/product.repository.js";

function requireProduct(product) {
    if (!product) {
        throw new DomainError("NOT_FOUND", "Product not found");
    }

    return product;
}

export function createProductApplication({
    repository = productRepository,
} = {}) {
    return {
        async listProducts(actor) {
            assertProductRead(actor);

            return {
                items: await repository.list(),
            };
        },

        async getProduct(pid, actor) {
            assertProductRead(actor);
            validateId(pid);

            return requireProduct(await repository.findById(pid));
        },

        async createProduct(input, actor) {
            assertProductAdmin(actor);

            const data = normalizeProductInput(input);

            return repository.create(data);
        },

        async updateProduct(input, actor) {
            assertProductAdmin(actor);
            validateId(input?.pid);

            const data = normalizeProductInput(input);
            const product = requireProduct(
                await repository.update(input.pid, data),
            );

            return {
                message: "Product updated successfully",
                pid: product.pid,
            };
        },

        async deleteProduct(pid, actor) {
            assertProductAdmin(actor);
            validateId(pid);

            const product = requireProduct(
                await repository.deleteById(pid),
            );

            return {
                message: "Product deleted successfully",
                pid: product.pid,
            };
        },
    };
}

export const productApplication = createProductApplication();