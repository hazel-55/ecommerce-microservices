import assert from "node:assert/strict";
import {
    validateId,
    normalizeFullname,
    assertCustomerAccess,
    assertMembershipChange,
} from "../src/domain/customer.rules.js";

function expectError(action, code) {
    assert.throws(action, (error) => error.code === code);
}

const customer = { uid: 1, role: "CUSTOMER" };
const admin = { uid: 2, role: "ADMIN" };

assert.equal(validateId(1), 1);
expectError(() => validateId(0), "INVALID_ARGUMENT");
expectError(() => validateId(1.5), "INVALID_ARGUMENT");

assert.equal(normalizeFullname("  John Doe  "), "John Doe");
expectError(() => normalizeFullname("   "), "INVALID_ARGUMENT");

assertCustomerAccess(customer, 1);
assertCustomerAccess(admin, 1);

expectError(
    () => assertCustomerAccess(undefined, 1),
    "UNAUTHENTICATED"
);

expectError(
    () => assertCustomerAccess(customer, 2),
    "FORBIDDEN"
);

assertMembershipChange(customer, 1, undefined);
assertMembershipChange(customer, 1, 1);

expectError(
    () => assertMembershipChange(customer, 1, 2),
    "FORBIDDEN"
);

assertMembershipChange(admin, 1, 2);

console.log("Customer domain rules: OK");
console.log("Profile ownership checks: OK");
console.log("Membership permission checks: OK");