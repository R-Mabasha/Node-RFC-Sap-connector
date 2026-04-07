import assert from "node:assert/strict";
import test from "node:test";

import type { SapClient, SapDiagnostics, TableReadRequest, TableReadResult } from "../types.js";
import { KpiExecutor } from "./executor.js";

class FakeSapClient implements SapClient {
  readonly calls: string[] = [];

  async ping(): Promise<boolean> {
    return true;
  }

  async call(
    functionName: string,
    _parameters: Record<string, unknown> = {},
  ): Promise<Record<string, unknown>> {
    this.calls.push(functionName);

    return {
      EV_SCHEMA_VERSION: "1.0",
      ET_KPIS: [
        {
          KPI_ID: "service_calls",
          TITLE: "Service Calls",
          CATEGORY: "Business Process KPIs",
          STATUS: "OK",
          UNIT: "count",
          VALUE_NUM: 3,
        },
        {
          KPI_ID: "parts_consumed",
          TITLE: "Parts Consumed",
          CATEGORY: "Business Process KPIs",
          STATUS: "OK",
          UNIT: "count",
          VALUE_NUM: 1,
        },
      ],
    };
  }

  async readTable(_request: TableReadRequest): Promise<TableReadResult> {
    throw new Error("readTable should not be called in this test");
  }

  getDiagnostics(): SapDiagnostics {
    return {
      tableReadFunctions: [],
      activeTableReadFunction: undefined,
      circuitBreakerState: "closed",
      totalCalls: 0,
      totalFailures: 0,
    };
  }

  async close(): Promise<void> {}
}

test("runMany reuses one wrapper call for multiple KPIs in the same family", async () => {
  const sapClient = new FakeSapClient();
  const executor = new KpiExecutor(sapClient);

  const results = await executor.runMany(
    ["service_calls", "parts_consumed"],
    {
      from: "2026-01-01T00:00:00.000Z",
      to: "2026-01-02T00:00:00.000Z",
    },
  );

  assert.deepEqual(sapClient.calls, ["ZHC_GET_SERVICE_KPIS"]);
  assert.deepEqual(
    results.map((result) => [result.kpiId, result.value]),
    [
      ["service_calls", 3],
      ["parts_consumed", 1],
    ],
  );
});
