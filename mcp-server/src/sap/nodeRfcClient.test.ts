import assert from "node:assert/strict";
import test from "node:test";

import type { SapConfig } from "../types.js";
import { NodeRfcSapClient, normalizeTableReadWhere } from "./nodeRfcClient.js";

test("normalizeTableReadWhere compacts standalone clauses for BBP_RFC_READ_TABLE", () => {
  assert.deepEqual(
    normalizeTableReadWhere("BBP_RFC_READ_TABLE", [
      "STATUS EQ 'A'",
      "ENDDATE GE '20260401'",
      "ENDDATE LE '20260405'",
    ]),
    ["STATUS EQ 'A' AND ENDDATE GE '20260401' AND ENDDATE LE '20260405'"],
  );
});

test("normalizeTableReadWhere preserves advanced caller-managed clause splitting", () => {
  assert.deepEqual(
    normalizeTableReadWhere("BBP_RFC_READ_TABLE", [
      "STATUS EQ 'A' AND",
      "ENDDATE GE '20260401'",
    ]),
    ["STATUS EQ 'A' AND", "ENDDATE GE '20260401'"],
  );
});

test("normalizeTableReadWhere leaves non-BBP readers untouched", () => {
  assert.deepEqual(
    normalizeTableReadWhere("RFC_READ_TABLE", [
      "FIELD1 EQ 'X'",
      "FIELD2 EQ 'Y'",
    ]),
    ["FIELD1 EQ 'X'", "FIELD2 EQ 'Y'"],
  );
});

test("capability mismatches do not trip the circuit breaker", async () => {
  const config: SapConfig = {
    connectionParameters: {
      dest: "TEST",
    },
    connectionMode: "direct",
    configSources: [],
    configWarnings: [],
    poolLow: 0,
    poolHigh: 1,
    timeoutMs: 1000,
    tableReadFunctions: ["BBP_RFC_READ_TABLE"],
    allowedTables: [],
    allowedFunctions: [],
    unrestrictedMode: true,
    circuitBreakerThreshold: 2,
    circuitBreakerResetMs: 60000,
  };
  const client = new NodeRfcSapClient(config);
  const capabilityError = Object.assign(
    new Error("Function module missing"),
    { key: "FU_NOT_FOUND" },
  );

  (
    client as unknown as {
      withClient: (
        fn: (clientHandle: unknown) => Promise<unknown>,
      ) => Promise<unknown>;
    }
  ).withClient = async () => {
    throw capabilityError;
  };

  await assert.rejects(() => client.call("Z_MISSING_RFC"));
  await assert.rejects(() => client.call("Z_MISSING_RFC"));

  const diagnostics = client.getDiagnostics();
  assert.equal(diagnostics.totalFailures, 2);
  assert.equal(diagnostics.circuitBreakerState, "closed");
});
