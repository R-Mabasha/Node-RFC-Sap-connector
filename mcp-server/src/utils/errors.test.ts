import assert from "node:assert/strict";
import test from "node:test";

import {
  getSapErrorKey,
  isBusyResourceError,
  isSapCapabilityError,
  shouldTripCircuitBreaker,
} from "./errors.js";

test("getSapErrorKey normalizes SAP error keys", () => {
  assert.equal(
    getSapErrorKey({ key: "field_not_valid" }),
    "FIELD_NOT_VALID",
  );
});

test("busy resource errors are detected from the error message", () => {
  assert.equal(
    isBusyResourceError(new Error("device or resource busy: device or resource busy")),
    true,
  );
});

test("capability mismatches are classified as non-breaker SAP errors", () => {
  const error = Object.assign(new Error("Table missing"), {
    key: "TABLE_NOT_AVAILABLE",
  });

  assert.equal(isSapCapabilityError(error), true);
  assert.equal(shouldTripCircuitBreaker(error), false);
});

test("transport or connectivity failures still trip the circuit breaker", () => {
  const error = new Error("RFC logon failure");

  assert.equal(isSapCapabilityError(error), false);
  assert.equal(shouldTripCircuitBreaker(error), true);
});
