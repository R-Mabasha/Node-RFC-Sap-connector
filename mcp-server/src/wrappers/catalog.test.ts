import assert from "node:assert/strict";
import test from "node:test";

import { KPI_DEFINITIONS } from "../kpis/definitions.js";
import {
  buildWrapperCatalog,
  buildWrapperProbeResult,
  parseWrapperResponse,
} from "./catalog.js";

test("buildWrapperCatalog groups wrapper-backed KPIs by function", () => {
  const catalog = buildWrapperCatalog(KPI_DEFINITIONS, [
    "ZHC_GET_SECURITY_KPIS",
    "ZHC_GET_OTC_KPIS",
  ]);
  const security = catalog.find(
    (entry) => entry.functionName === "ZHC_GET_SECURITY_KPIS",
  );

  assert.ok(security);
  assert.equal(security.allowlisted, true);
  assert.deepEqual(
    security.kpis.map((kpi) => kpi.wrapperKpiId),
    [
      "authorization_failures",
      "emergency_access_sessions",
      "expired_password_pct",
      "users_with_sod_conflicts",
    ],
  );
});

test("parseWrapperResponse normalizes KPI rows and notes", () => {
  const parsed = parseWrapperResponse("ZHC_GET_SECURITY_KPIS", {
    EV_SCHEMA_VERSION: "1.0",
    ET_KPIS: [
      {
        KPI_ID: "authorization_failures",
        TITLE: "Authorization Failures",
        CATEGORY: "Security & Authorization",
        STATUS: "ERROR",
        UNIT: "count",
        VALUE_NUM: "5",
        NOTES_JSON: "[\"Security Audit Log disabled\"]",
      },
    ],
    ET_MESSAGES: [{ TYPE: "W", MESSAGE: "Partial dataset." }],
  });

  assert.equal(parsed.schemaVersion, "1.0");
  assert.equal(parsed.messages.length, 1);

  const result = parsed.resultsByKpiId.get("authorization_failures");
  assert.ok(result);
  assert.equal(result.status, "error");
  assert.equal(result.value, 5);
  assert.deepEqual(result.notes, ["Security Audit Log disabled"]);
});

test("buildWrapperProbeResult flags missing expected KPI rows", () => {
  const probe = buildWrapperProbeResult({
    functionName: "ZHC_GET_P2P_KPIS",
    expectedKpiIds: ["po_creation_errors", "invoice_match_failures"],
    result: {
      EV_SCHEMA_VERSION: "1.0",
      ET_KPIS: [
        {
          KPI_ID: "po_creation_errors",
          TITLE: "PO Creation Errors",
          CATEGORY: "Business Process KPIs",
          STATUS: "OK",
          UNIT: "count",
          VALUE_NUM: 7,
        },
      ],
    },
  });

  assert.equal(probe.ok, false);
  assert.deepEqual(probe.returnedKpiIds, ["po_creation_errors"]);
  assert.deepEqual(probe.missingKpiIds, ["invoice_match_failures"]);
  assert.deepEqual(probe.unexpectedKpiIds, []);
});
