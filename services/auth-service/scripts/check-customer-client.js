import assert from "node:assert/strict";
import {
    customerClient,
    createCustomerClient,
} from "../src/infrastructure/grpc/customer.client.js";

const invalidCaller = createCustomerClient({
    token: "wrong-token",
});

try {
    await assert.rejects(
        () => customerClient.createProfile({
            uid: 0,
            fullname: "John Doe",
        }),
        (error) => error.code === "INVALID_ARGUMENT"
    );

    await assert.rejects(
        () => invalidCaller.createProfile({
            uid: 0,
            fullname: "John Doe",
        }),
        (error) => error.code === "SERVICE_UNAVAILABLE"
    );

    console.log("Auth to Customer gRPC client: OK");
    console.log("Trusted caller reaches validation: OK");
    console.log("Invalid service token rejected: OK");
} catch (error) {
    console.error("Customer client check failed:", error.code ?? error.message);
    process.exitCode = 1;
} finally {
    customerClient.close();
    invalidCaller.close();
}