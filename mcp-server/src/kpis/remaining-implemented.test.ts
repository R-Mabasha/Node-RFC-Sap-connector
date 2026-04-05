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

test("number_range_exhaustion_pct returns the highest scoped NRIV utilization", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      tables: {
        NRIV: [
          {
            OBJECT: "SD_DOC",
            NRRANGENR: "01",
            FROMNUMBER: "1",
            TONUMBER: "100",
            NRLEVEL: "91",
          },
          {
            OBJECT: "FI_DOC",
            NRRANGENR: "02",
            FROMNUMBER: "1",
            TONUMBER: "200",
            NRLEVEL: "51",
          },
        ],
      },
    }),
  );
  const result = await executor.run("number_range_exhaustion_pct", {});

  assert.equal(result.status, "ok");
  assert.equal(result.value, 90);
  assert.equal(result.tier, "daily");
});

test("SWNC-backed KPIs extract peak users, dialog response time, and timeout errors", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      calls: {
        SWNC_COLLECTOR_GET_AGGREGATES: {
          ASTAT: [
            {
              PEAK_USERS: 120,
              AVG_RESPTI: 240,
              TIMEOUTS: 3,
            },
            {
              PEAK_USERS: 80,
              AVG_RESPTI: 360,
              TIMEOUTS: 2,
            },
          ],
        },
      },
    }),
  );

  const [peakUsers, dialogTime, timeoutErrors] = await executor.runMany(
    ["peak_concurrent_users", "dialog_response_time", "timeout_errors"],
    {
      from: "2026-04-01T00:00:00.000Z",
      to: "2026-04-02T00:00:00.000Z",
    },
  );

  assert.ok(peakUsers);
  assert.ok(dialogTime);
  assert.ok(timeoutErrors);
  assert.equal(peakUsers.value, 120);
  assert.equal(dialogTime.value, 300);
  assert.equal(timeoutErrors.value, 5);
});

test("retry_attempt_count sums TRIES inside the requested window", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      tables: {
        ARFCSSTATE: [
          {
            TRIES: "2",
            LSTCHDATE: "20260402",
            LSTCHTIME: "101500",
            ARFCSTATE: "SYSFAIL",
          },
          {
            TRIES: "1",
            LSTCHDATE: "20260403",
            LSTCHTIME: "111500",
            ARFCSTATE: "CPICERR",
          },
          {
            TRIES: "0",
            LSTCHDATE: "20260403",
            LSTCHTIME: "131500",
            ARFCSTATE: "DONE",
          },
        ],
      },
    }),
  );
  const result = await executor.run("retry_attempt_count", {
    from: "2026-04-01T00:00:00.000Z",
    to: "2026-04-05T00:00:00.000Z",
  });

  assert.equal(result.status, "ok");
  assert.equal(result.value, 3);
});

test("queue_lock_failures counts qRFC rows with lock-like or error states", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      tables: {
        QRFCSSTATE: [
          {
            QERRCNT: "0",
            QSTATE: "LOCKED",
            LUPD_DATE: "20260402",
            LUPD_TIME: "101500",
            QNAME: "QUEUE_A",
          },
          {
            QERRCNT: "2",
            QSTATE: "READY",
            LUPD_DATE: "20260403",
            LUPD_TIME: "111500",
            QNAME: "QUEUE_B",
          },
          {
            QERRCNT: "0",
            QSTATE: "READY",
            LUPD_DATE: "20260403",
            LUPD_TIME: "131500",
            QNAME: "QUEUE_C",
          },
        ],
      },
    }),
  );
  const result = await executor.run("queue_lock_failures", {
    from: "2026-04-01T00:00:00.000Z",
    to: "2026-04-05T00:00:00.000Z",
  });

  assert.equal(result.status, "ok");
  assert.equal(result.value, 2);
});

test("mrp_errors and goods_receipts count table-backed process rows", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      tables: {
        MDLG: [
          { DELNR: "1", DAT00: "20260402" },
          { DELNR: "2", DAT00: "20260403" },
        ],
        MSEG: [
          { MBLNR: "5001", BWART: "101", BUDAT_MKPF: "20260402" },
          { MBLNR: "5002", BWART: "105", BUDAT_MKPF: "20260403" },
        ],
      },
    }),
  );

  const [mrpErrors, goodsReceipts] = await executor.runMany(
    ["mrp_errors", "goods_receipts"],
    {
      from: "2026-04-01T00:00:00.000Z",
      to: "2026-04-05T00:00:00.000Z",
    },
  );

  assert.ok(mrpErrors);
  assert.ok(goodsReceipts);
  assert.equal(mrpErrors.value, 2);
  assert.equal(goodsReceipts.value, 2);
});
