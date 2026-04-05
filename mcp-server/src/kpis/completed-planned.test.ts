import assert from "node:assert/strict";
import test from "node:test";

import type { SapClient, SapDiagnostics, TableReadRequest, TableReadResult } from "../types.js";
import { KpiExecutor } from "./executor.js";

class FixtureSapClient implements SapClient {
  constructor(
    private readonly fixtures: {
      calls?: Record<string, Record<string, unknown>>;
      tables?: Record<string, Array<Record<string, string>>>;
    },
  ) {}

  async ping(): Promise<boolean> {
    return true;
  }

  async call(
    functionName: string,
    _parameters: Record<string, unknown> = {},
  ): Promise<Record<string, unknown>> {
    const response = this.fixtures.calls?.[functionName];

    if (!response) {
      throw new Error(`Unexpected RFC call: ${functionName}`);
    }

    return response;
  }

  async readTable(request: TableReadRequest): Promise<TableReadResult> {
    const rows = this.fixtures.tables?.[request.table] ?? [];
    const start = request.rowSkips ?? 0;
    const end = start + (request.rowCount ?? rows.length);
    const page = rows.slice(start, end).map((row) =>
      Object.fromEntries(
        request.fields.map((field) => [field, row[field] ?? ""]),
      ),
    );

    return {
      table: request.table,
      fields: request.fields,
      rows: page,
      rowCount: page.length,
      truncated: end < rows.length,
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

test("uptime, restart frequency, license usage, and system log errors are implemented", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      calls: {
        TH_SERVER_LIST: {
          LIST_IPV6: [
            { NAME: "APP_00", STATE: Uint8Array.from([1]) },
            { NAME: "APP_01", STATE: Uint8Array.from([1]) },
            { NAME: "APP_02", STATE: Uint8Array.from([0]) },
          ],
        },
        RSLG_GET_MESSAGES: {
          MESSAGES: [
            {
              MSGTYPE: "E",
              TEXT: "Work process restart on app server",
              DATE: "20260405",
              TIME: "010000",
            },
            {
              MSGTYPE: "I",
              TEXT: "Informational startup notice",
              DATE: "20260405",
              TIME: "020000",
            },
            {
              MSGTYPE: "W",
              TEXT: "Background error detected",
              DATE: "20260405",
              TIME: "030000",
            },
          ],
        },
        SLIC_GET_INSTALLATIONS: {
          LICENSES: [
            {
              USED_USERS: 80,
              LICENSED_USERS: 100,
            },
          ],
        },
      },
    }),
  );

  const [uptime, restartFrequency, licenseUtilization, systemLogErrors] =
    await executor.runMany(
      [
        "sap_application_uptime_pct",
        "average_system_restart_frequency",
        "license_utilization_pct",
        "system_log_errors",
      ],
      {
        from: "2026-04-05T00:00:00.000Z",
        to: "2026-04-06T00:00:00.000Z",
      },
    );

  assert.ok(uptime);
  assert.ok(restartFrequency);
  assert.ok(licenseUtilization);
  assert.ok(systemLogErrors);
  assert.equal(uptime.value, 66.67);
  assert.equal(restartFrequency.value, 2);
  assert.equal(licenseUtilization.value, 80);
  assert.equal(systemLogErrors.value, 2);
});

test("update, CPU, memory, API response, gateway, lock overflow, and failed API KPIs are implemented", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      calls: {
        SWNC_COLLECTOR_GET_AGGREGATES: {
          ASTAT: [
            {
              AVG_UPD_RESPTI: 150,
              AVG_RESPTI: 280,
            },
          ],
        },
        BAPI_SYSTEM_MON_GETSYSINFO: {
          ET_SYSINFO: [
            {
              CPU_UTILIZATION: 60,
              MEM_USED: 3,
              MEM_TOTAL: 4,
            },
            {
              CPU_UTILIZATION: 40,
              MEM_USED: 1,
              MEM_TOTAL: 2,
            },
          ],
        },
        ICM_GET_MONITOR_INFO: {
          SERVICES: [
            {
              FAILED_REQUESTS: 4,
              AVG_RESPONSE_TIME: 250,
            },
            {
              HTTP_5XX: 1,
              AVG_RESPONSE_TIME: 350,
            },
          ],
        },
        GW_GET_STATISTIC: {
          STATISTICS: [
            { ERRCOUNT: 2 },
            { ERR_COUNT: 3 },
          ],
        },
        ENQUEUE_STATISTICS: {
          STATISTICS: [
            { OVERFLOW_CNT: 1 },
            { OVERFLOW_COUNT: 2 },
          ],
        },
      },
    }),
  );

  const [
    updateTaskResponseTime,
    cpuUtilization,
    memoryUtilization,
    apiResponseTime,
    gatewayErrors,
    lockTableOverflows,
    failedApiCalls,
  ] = await executor.runMany(
    [
      "update_task_response_time",
      "cpu_utilization_pct",
      "memory_utilization_pct",
      "api_response_time",
      "gateway_errors",
      "lock_table_overflows",
      "failed_api_calls",
    ],
    {
      from: "2026-04-05T00:00:00.000Z",
      to: "2026-04-06T00:00:00.000Z",
      },
    );

  assert.ok(updateTaskResponseTime);
  assert.ok(cpuUtilization);
  assert.ok(memoryUtilization);
  assert.ok(apiResponseTime);
  assert.ok(gatewayErrors);
  assert.ok(lockTableOverflows);
  assert.ok(failedApiCalls);
  assert.equal(updateTaskResponseTime.value, 150);
  assert.equal(cpuUtilization.value, 50);
  assert.equal(memoryUtilization.value, 66.67);
  assert.equal(apiResponseTime.value, 300);
  assert.equal(gatewayErrors.value, 5);
  assert.equal(lockTableOverflows.value, 3);
  assert.equal(failedApiCalls.value, 5);
});

test("replication_delays derives lag from the newest readable replication timestamp", async () => {
  const now = new Date();
  const lagged = new Date(now.getTime() - 90_000);
  const dateValue = lagged.toISOString().slice(0, 10).replaceAll("-", "");
  const timeValue = lagged.toISOString().slice(11, 19).replaceAll(":", "");
  const executor = new KpiExecutor(
    new FixtureSapClient({
      tables: {
        IUUC_REPL_CONTENT: [
          {
            TABNAME: "MARA",
            LUPD_DATE: dateValue,
            LUPD_TIME: timeValue,
          },
        ],
      },
    }),
  );
  const result = await executor.run("replication_delays", {});

  assert.equal(result.status, "ok");
  assert.ok(typeof result.value === "number");
  assert.ok(result.value >= 60 && result.value <= 180);
});
