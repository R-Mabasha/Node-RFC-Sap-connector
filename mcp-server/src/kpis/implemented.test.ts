import assert from "node:assert/strict";
import test from "node:test";

import type { SapClient, SapDiagnostics, TableReadRequest, TableReadResult } from "../types.js";
import { KpiExecutor } from "./executor.js";

class WorkProcessSapClient implements SapClient {
  async ping(): Promise<boolean> {
    return true;
  }

  async call(
    functionName: string,
    _parameters: Record<string, unknown> = {},
  ): Promise<Record<string, unknown>> {
    if (functionName !== "TH_WPINFO") {
      throw new Error(`Unexpected RFC call: ${functionName}`);
    }

    return {
      WPLIST: [
        { WP_STATUS: "Running" },
        { WP_STATUS: "On Hold" },
        { WP_STATUS: "Waiting" },
        { WP_STATUS: "Waiting" },
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

test("work_process_utilization is derived from TH_WPINFO statuses", async () => {
  const executor = new KpiExecutor(new WorkProcessSapClient());
  const result = await executor.run("work_process_utilization", {});

  assert.equal(result.status, "ok");
  assert.equal(result.value, 50);
  assert.equal(result.tier, "realtime");
});

class ServerListSapClient implements SapClient {
  async ping(): Promise<boolean> {
    return true;
  }

  async call(
    functionName: string,
    _parameters: Record<string, unknown> = {},
  ): Promise<Record<string, unknown>> {
    if (functionName !== "TH_SERVER_LIST") {
      throw new Error(`Unexpected RFC call: ${functionName}`);
    }

    return {
      LIST_IPV6: [
        { NAME: "APP_00", STATE: Uint8Array.from([1]) },
        { NAME: "APP_01", STATE: Uint8Array.from([0]) },
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

test("application_server_uptime_per_instance uses TH_SERVER_LIST availability", async () => {
  const executor = new KpiExecutor(new ServerListSapClient());
  const result = await executor.run("application_server_uptime_per_instance", {});

  assert.equal(result.status, "ok");
  assert.equal(result.value, 50);
  assert.equal(result.tier, "frequent");
});

class BatchWindowSapClient implements SapClient {
  async ping(): Promise<boolean> {
    return true;
  }

  async call(
    _functionName: string,
    _parameters: Record<string, unknown> = {},
  ): Promise<Record<string, unknown>> {
    throw new Error("RFC calls should not be used in this test");
  }

  async readTable(request: TableReadRequest): Promise<TableReadResult> {
    assert.equal(request.table, "TBTCO");

    const rows =
      request.rowSkips === 0
        ? [
            {
              JOBNAME: "JOB_A",
              STRTDATE: "20260401",
              STRTTIME: "000000",
              ENDDATE: "20260401",
              ENDTIME: "030000",
              STATUS: "F",
            },
            {
              JOBNAME: "JOB_B",
              STRTDATE: "20260401",
              STRTTIME: "060000",
              ENDDATE: "20260401",
              ENDTIME: "120000",
              STATUS: "A",
            },
          ]
        : [];

    return {
      table: request.table,
      fields: request.fields,
      rows,
      rowCount: rows.length,
      truncated: false,
      readerFunction: "BBP_RFC_READ_TABLE",
    };
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

test("batch_window_utilization_pct derives capped utilization from TBTCO runtime overlap", async () => {
  const executor = new KpiExecutor(new BatchWindowSapClient());
  const result = await executor.run("batch_window_utilization_pct", {
    from: "2026-04-01T00:00:00.000Z",
    to: "2026-04-01T12:00:00.000Z",
  });

  assert.equal(result.status, "ok");
  assert.equal(result.value, 75);
  assert.equal(result.tier, "batch");
});
