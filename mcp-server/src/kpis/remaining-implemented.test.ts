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

test("retry_attempt_count sums ARFCRETRYS inside the requested window", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      tables: {
        ARFCSSTATE: [
          {
            ARFCRETRYS: "2",
            ARFCDATUM: "20260402",
            ARFCUZEIT: "101500",
            ARFCSTATE: "SYSFAIL",
          },
          {
            ARFCRETRYS: "1",
            ARFCDATUM: "20260403",
            ARFCUZEIT: "111500",
            ARFCSTATE: "CPICERR",
          },
          {
            ARFCRETRYS: "0",
            ARFCDATUM: "20260403",
            ARFCUZEIT: "131500",
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

test("queue_lock_failures counts outbound and inbound qRFC rows with lock-like or error states", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      tables: {
        TRFCQOUT: [
          {
            QLOCKCNT: "0",
            QSTATE: "LOCKED",
            QRFCDATUM: "20260402",
            QNAME: "QUEUE_A",
            ERRMESS: "",
          },
          {
            QLOCKCNT: "2",
            QSTATE: "READY",
            QRFCDATUM: "20260403",
            QNAME: "QUEUE_B",
            ERRMESS: "",
          },
          {
            QLOCKCNT: "0",
            QSTATE: "READY",
            QRFCDATUM: "20260403",
            QNAME: "QUEUE_C",
            ERRMESS: "",
          },
        ],
        TRFCQIN: [
          {
            QLOCKCNT: "0",
            QSTATE: "READY",
            QRFCDATUM: "20260403",
            QNAME: "QUEUE_D",
            ERRMESS: "Queue error",
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
  assert.equal(result.value, 3);
});

test("mrp_errors, mfg_errors, and goods_receipts count table-backed process rows", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      tables: {
        MDKP: [
          { MATNR: "MAT1", DSDAT: "20260402", AUSZ1: "1", AUSZ2: "0", AUSZ3: "0", AUSZ4: "0", AUSZ5: "0", AUSZ6: "0", AUSZ7: "0", AUSZ8: "0" },
          { MATNR: "MAT2", DSDAT: "20260403", AUSZ1: "0", AUSZ2: "2", AUSZ3: "0", AUSZ4: "0", AUSZ5: "0", AUSZ6: "0", AUSZ7: "0", AUSZ8: "0" },
          { MATNR: "MAT3", DSDAT: "20260403", AUSZ1: "0", AUSZ2: "0", AUSZ3: "0", AUSZ4: "0", AUSZ5: "0", AUSZ6: "0", AUSZ7: "0", AUSZ8: "0" },
        ],
        MKPF: [
          { MBLNR: "5001", BUDAT: "20260402" },
          { MBLNR: "5002", BUDAT: "20260403" },
        ],
      },
    }),
  );

  const [mrpErrors, mfgErrors, goodsReceipts] = await executor.runMany(
    ["mrp_errors", "mfg_errors", "goods_receipts"],
    {
      from: "2026-04-01T00:00:00.000Z",
      to: "2026-04-05T00:00:00.000Z",
    },
  );

  assert.ok(mrpErrors);
  assert.ok(mfgErrors);
  assert.ok(goodsReceipts);
  assert.equal(mrpErrors.value, 2);
  assert.equal(mfgErrors.value, 3);
  assert.equal(goodsReceipts.value, 2);
});

test("rfc_user_password_age and expired_password_pct use landscape-specific USR02 date fields", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      tables: {
        USR02: [
          { BNAME: "RFC_1", USTYP: "S", UFLAG: "0", PWDCHGDATE: "20260101" },
          { BNAME: "RFC_2", USTYP: "S", UFLAG: "64", PWDCHGDATE: "20251201" },
          { BNAME: "RFC_3", USTYP: "S", UFLAG: "0", PWDCHGDATE: "00000000" },
        ],
      },
    }),
  );

  const [passwordAge, expiredPct] = await executor.runMany(
    ["rfc_user_password_age", "expired_password_pct"],
    {
      dimensions: {
        password_max_age_days: "90",
      },
    },
  );

  assert.ok(passwordAge);
  assert.ok(expiredPct);
  assert.equal(passwordAge.status, "ok");
  assert.equal(expiredPct.status, "ok");
  assert.ok((passwordAge.value ?? 0) > 0);
  assert.equal(expiredPct.value, 50);
  assert.match(
    expiredPct.notes.join(" "),
    /Password date field used: PWDCHGDATE/,
  );
});

test("job KPIs use current TBTCO/TBTCP fields for restart and step failure logic", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      tables: {
        TBTCO: [
          { JOBNAME: "JOB_A", JOBCOUNT: "0001", PREDNUM: "1", STATUS: "F", STRTDATE: "20260402" },
          { JOBNAME: "JOB_B", JOBCOUNT: "0002", PREDNUM: "1", STATUS: "A", STRTDATE: "20260402" },
          { JOBNAME: "JOB_C", JOBCOUNT: "0003", PREDNUM: "0", STATUS: "F", STRTDATE: "20260402" },
        ],
        TBTCP: [
          { JOBNAME: "JOB_A", JOBCOUNT: "0001", STEPCOUNT: "1", STATUS: "E", SDLDATE: "20260402" },
          { JOBNAME: "JOB_B", JOBCOUNT: "0002", STEPCOUNT: "1", STATUS: "A", SDLDATE: "20260402" },
          { JOBNAME: "JOB_C", JOBCOUNT: "0003", STEPCOUNT: "1", STATUS: "F", SDLDATE: "20260402" },
        ],
      },
    }),
  );

  const [restartRate, stepFailures] = await executor.runMany(
    ["job_restart_success_rate", "job_step_failures"],
    {
      from: "2026-04-01T00:00:00.000Z",
      to: "2026-04-05T00:00:00.000Z",
    },
  );

  assert.ok(restartRate);
  assert.ok(stepFailures);
  assert.equal(restartRate.status, "ok");
  assert.equal(stepFailures.status, "ok");
  assert.equal(restartRate.value, 50);
  assert.equal(stepFailures.value, 2);
});

test("peak_concurrent_users falls back to live session count when SWNC aggregates are empty", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      calls: {
        SWNC_COLLECTOR_GET_AGGREGATES: {
          ASTAT: [],
          FRONTEND: [],
        },
      },
      tables: {
        USR41: [
          { BNAME: "USER1" },
          { BNAME: "USER2" },
          { BNAME: "USER3" },
        ],
      },
    }),
  );

  const result = await executor.run("peak_concurrent_users", {
    from: "2026-04-01T00:00:00.000Z",
    to: "2026-04-05T00:00:00.000Z",
  });

  assert.equal(result.status, "ok");
  assert.equal(result.value, 3);
  assert.match(result.notes.join(" "), /used current live-session count from USR41/i);
});

test("sales and p2p fallbacks use landscape-safe fields and JS-side matching", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      tables: {
        VBAK: [
          { VBELN: "SO1", ERDAT: "20260402", LIFSK: " ", FAKSK: " ", GBSTK: "C", KNUMV: "0000000001", NETWR: "100.00", CMGST: "A" },
          { VBELN: "SO2", ERDAT: "20260402", LIFSK: "01", FAKSK: " ", GBSTK: "B", KNUMV: "", NETWR: "0.00", CMGST: "B" },
          { VBELN: "SO3", ERDAT: "20260403", LIFSK: " ", FAKSK: " ", GBSTK: "C", KNUMV: "0000000002", NETWR: "50.00", CMGST: " " },
        ],
        VBEP: [
          { VBELN: "SO1", POSNR: "0010", EDATU: "20260402", WMENG: "10.000", BMENG: "10.000" },
          { VBELN: "SO2", POSNR: "0010", EDATU: "20260403", WMENG: "10.000", BMENG: "4.000" },
          { VBELN: "SO3", POSNR: "0010", EDATU: "20260403", WMENG: "5.000", BMENG: "0.000" },
        ],
        EKKO: [
          { EBELN: "PO1", BEDAT: "20260402" },
          { EBELN: "PO2", BEDAT: "20260402" },
          { EBELN: "PO3", BEDAT: "20260403" },
        ],
        EKBE: [
          { EBELN: "PO1", VGABE: "1", BUDAT: "20260402" },
          { EBELN: "PO1", VGABE: "2", BUDAT: "20260402" },
          { EBELN: "PO2", VGABE: "1", BUDAT: "20260402" },
        ],
        RBKP: [
          { BELNR: "INV1", BLDAT: "20260402", RBSTAT: "B", ZLSPR: "" },
          { BELNR: "INV2", BLDAT: "20260402", RBSTAT: " ", ZLSPR: "A" },
          { BELNR: "INV3", BLDAT: "20260403", RBSTAT: " ", ZLSPR: "" },
        ],
        AFFW: [
          { WEBLNR: "1", ERSDA: "20260402", BWART: "101" },
          { WEBLNR: "2", ERSDA: "20260403", BWART: "261" },
          { WEBLNR: "3", ERSDA: "20260403", BWART: "105" },
        ],
      },
    }),
  );

  const results = await executor.runMany(
    [
      "fulfillment_accuracy",
      "backorder_rate",
      "pricing_compliance",
      "credit_failures",
      "po_match_rate",
      "invoice_hold_rate",
      "three_way_matching_failures",
      "gr_posting_failures",
    ],
    {
      from: "2026-04-01T00:00:00.000Z",
      to: "2026-04-05T00:00:00.000Z",
    },
  );

  const byId = new Map(results.map((result) => [result.kpiId, result]));

  assert.equal(byId.get("fulfillment_accuracy")?.value, 66.67);
  assert.equal(byId.get("backorder_rate")?.value, 66.67);
  assert.equal(byId.get("pricing_compliance")?.value, 66.67);
  assert.equal(byId.get("credit_failures")?.value, 1);
  assert.equal(byId.get("po_match_rate")?.value, 33.33);
  assert.equal(byId.get("invoice_hold_rate")?.value, 66.67);
  assert.equal(byId.get("three_way_matching_failures")?.value, 2);
  assert.equal(byId.get("gr_posting_failures")?.value, 2);
});

test("invoice_to_cash_cycle, payment_run_errors, and replication_delays use new fallbacks", async () => {
  class ReplicationFallbackSapClient extends FixtureSapClient {
    async readTable(request: TableReadRequest): Promise<TableReadResult> {
      if (request.table === "IUUC_REPL_CONTENT") {
        throw Object.assign(new Error("IUUC_REPL_CONTENT unavailable"), {
          key: "TABLE_NOT_AVAILABLE",
        });
      }

      return super.readTable(request);
    }
  }

  const executor = new KpiExecutor(
    new ReplicationFallbackSapClient({
      tables: {
        BSAD: [
          { BELNR: "1", BLDAT: "20260401", AUGDT: "20260403" },
          { BELNR: "2", BLDAT: "20260402", AUGDT: "20260405" },
        ],
        REGUH: [
          { LAUFD: "20260402", XVORL: "X", VBLNR: "0000000000" },
          { LAUFD: "20260403", XVORL: "X", VBLNR: "1900000001" },
          { LAUFD: "20260403", XVORL: " ", VBLNR: "1900000002" },
        ],
        IUUC_REPL_HDR: [
          { CONFIG_GUID: "CFG1", CHDATE: "20260407", CHTIME: "120000", CRDATE: "20260401", CRTIME: "080000" },
        ],
      },
    }),
  );

  const [invoiceToCash, paymentRunErrors, replicationDelays] = await executor.runMany(
    ["invoice_to_cash_cycle", "payment_run_errors", "replication_delays"],
    {
      from: "2026-04-01T00:00:00.000Z",
      to: "2026-04-07T23:59:59.000Z",
    },
  );

  assert.equal(invoiceToCash?.status, "ok");
  assert.equal(invoiceToCash?.value, 2.5);
  assert.equal(paymentRunErrors?.status, "ok");
  assert.equal(paymentRunErrors?.value, 1);
  assert.equal(replicationDelays?.status, "ok");
  assert.ok((replicationDelays?.value ?? 0) >= 0);
  assert.match(replicationDelays?.notes.join(" ") ?? "", /Fell back to IUUC_REPL_HDR/i);
});

test("goods_receipts and order_completion_rate honor sapFlavor-specific paths without cache bleed", async () => {
  const executor = new KpiExecutor(
    new FixtureSapClient({
      tables: {
        MKPF: [{ MBLNR: "5001", BUDAT: "20260402" }],
        MATDOC: [
          { MBLNR: "9001", BUDAT: "20260402" },
          { MBLNR: "9002", BUDAT: "20260403" },
        ],
        VBAK: [
          { VBELN: "SO1", ERDAT: "20260402", GBSTK: "" },
          { VBELN: "SO2", ERDAT: "20260403", GBSTK: "" },
        ],
        VBUK: [{ VBELN: "SO1", GBSTK: "C" }],
      },
    }),
  );

  const sharedGoodsReceipts = await executor.run("goods_receipts", {
    from: "2026-05-01T00:00:00.000Z",
    to: "2026-05-05T00:00:00.000Z",
    sapFlavor: "shared",
  });
  const s4GoodsReceipts = await executor.run("goods_receipts", {
    from: "2026-05-01T00:00:00.000Z",
    to: "2026-05-05T00:00:00.000Z",
    sapFlavor: "s4hana",
  });
  const eccOrderCompletion = await executor.run("order_completion_rate", {
    from: "2026-05-01T00:00:00.000Z",
    to: "2026-05-05T00:00:00.000Z",
    sapFlavor: "ecc",
  });

  assert.equal(sharedGoodsReceipts.status, "ok");
  assert.equal(sharedGoodsReceipts.value, 1);
  assert.equal(s4GoodsReceipts.status, "ok");
  assert.equal(s4GoodsReceipts.value, 2);
  assert.match(s4GoodsReceipts.notes.join(" "), /sapFlavor=s4hana/i);
  assert.equal(eccOrderCompletion.status, "ok");
  assert.equal(eccOrderCompletion.value, 50);
  assert.match(eccOrderCompletion.notes.join(" "), /Used VBUK status join for ECC mode/i);
});

test("accrual_accuracy honors sapFlavor=ecc and skips the ACDOCA-first path", async () => {
  class EccFinanceSapClient extends FixtureSapClient {
    async readTable(request: TableReadRequest): Promise<TableReadResult> {
      if (request.table === "ACDOCA") {
        throw new Error("ACDOCA should not be read in ECC mode");
      }

      return super.readTable(request);
    }
  }

  const executor = new KpiExecutor(
    new EccFinanceSapClient({
      tables: {
        BKPF: [{ BELNR: "1900000001", BUDAT: "20260402" }],
      },
    }),
  );

  const result = await executor.run("accrual_accuracy", {
    from: "2026-04-01T00:00:00.000Z",
    to: "2026-04-05T00:00:00.000Z",
    sapFlavor: "ecc",
  });

  assert.equal(result.status, "ok");
  assert.equal(result.value, 100);
  assert.match(result.notes.join(" "), /sapFlavor=ecc used BKPF-based estimate/i);
});
