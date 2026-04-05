import type {
  KpiRequestInput,
  KpiResult,
  KpiSource,
  KpiTier,
  ResolvedWindow,
} from "../types.js";
import { describeError } from "../utils/errors.js";

export type KpiMaturity =
  | "implemented"
  | "planned"
  | "custom_abap_required"
  | "excluded";

export interface CountRowsRequest {
  table: string;
  fields: string[];
  where?: string[];
  pageSize?: number;
  scanCap?: number;
}

export interface ScanRowsRequest extends CountRowsRequest {}

export interface KpiExecutionHelpers {
  countRows(request: CountRowsRequest): Promise<number>;
  scanRows(request: ScanRowsRequest): Promise<Array<Record<string, string>>>;
  callFunction(
    functionName: string,
    parameters?: Record<string, unknown>,
  ): Promise<Record<string, unknown>>;
  resolveWindow(input: KpiRequestInput, fallbackHours?: number): ResolvedWindow;
  getNumberDimension(
    input: KpiRequestInput,
    key: string,
    defaultValue: number,
  ): number;
  toSapDateDaysAgo(days: number): string;
  parseSapDateTime(dateValue: string, timeValue: string): Date | undefined;
  daysSinceSapDate(dateValue: string): number | undefined;
}

interface BaseKpiDefinition {
  id: string;
  title: string;
  category: string;
  unit?: string;
  tier?: KpiTier;
  maturity: KpiMaturity;
  summary: string;
  source: KpiSource;
  notes?: string[];
}

export interface WrapperSpec {
  functionName: string;
  wrapperKpiId: string;
}

export interface ExecutableKpiDefinition extends BaseKpiDefinition {
  maturity: "implemented";
  execute(
    helpers: KpiExecutionHelpers,
    input: KpiRequestInput,
  ): Promise<KpiResult>;
}

export interface NonExecutableKpiDefinition extends BaseKpiDefinition {
  maturity: Exclude<KpiMaturity, "implemented">;
  blocker: string;
  wrapper?: WrapperSpec;
}

export type KpiDefinition = ExecutableKpiDefinition | NonExecutableKpiDefinition;

function countResult(
  definition: BaseKpiDefinition,
  value: number,
  notes: string[],
  window?: ResolvedWindow,
): KpiResult {
  return {
    kpiId: definition.id,
    title: definition.title,
    category: definition.category,
    status: "ok",
    unit: definition.unit,
    tier: definition.tier,
    value,
    window,
    source: definition.source,
    notes,
  };
}

function plannedDefinition(definition: {
  id: string;
  title: string;
  category: string;
  unit?: string;
  tier?: KpiTier;
  summary: string;
  source: KpiSource;
  blocker: string;
  notes?: string[];
}): NonExecutableKpiDefinition {
  return {
    ...definition,
    maturity: "planned",
  };
}

function wrapperDefinition(definition: {
  id: string;
  title: string;
  category: string;
  unit?: string;
  tier?: KpiTier;
  summary: string;
  source: KpiSource;
  blocker: string;
  functionName: string;
  wrapperKpiId?: string;
  notes?: string[];
}): NonExecutableKpiDefinition {
  return {
    id: definition.id,
    title: definition.title,
    category: definition.category,
    unit: definition.unit,
    tier: definition.tier,
    maturity: "custom_abap_required",
    summary: definition.summary,
    source: definition.source,
    blocker: definition.blocker,
    notes: definition.notes,
    wrapper: {
      functionName: definition.functionName,
      wrapperKpiId: definition.wrapperKpiId ?? definition.id,
    },
  };
}

const REALTIME_KPI_IDS = new Set([
  "active_user_count",
  "work_process_utilization",
  "spool_queue_errors",
  "work_item_backlog",
]);
const FREQUENT_KPI_IDS = new Set([
  "delayed_job_count",
  "long_running_job_count",
  "application_server_uptime_per_instance",
  "peak_concurrent_users",
  "dialog_response_time",
  "update_task_response_time",
  "cpu_utilization_pct",
  "memory_utilization_pct",
  "system_log_errors",
  "gateway_errors",
  "timeout_errors",
  "lock_table_overflows",
  "failed_api_calls",
  "api_response_time",
  "retry_attempt_count",
  "queue_lock_failures",
]);
const DAILY_KPI_IDS = new Set([
  "sap_application_uptime_pct",
  "rfc_user_password_age",
  "inactive_users",
  "expired_password_pct",
  "replication_delays",
  "number_range_exhaustion_pct",
]);

function inferKpiTier(definition: Pick<BaseKpiDefinition, "id" | "category">): KpiTier {
  if (REALTIME_KPI_IDS.has(definition.id)) {
    return "realtime";
  }

  if (FREQUENT_KPI_IDS.has(definition.id)) {
    return "frequent";
  }

  if (DAILY_KPI_IDS.has(definition.id)) {
    return "daily";
  }

  if (definition.category === "Data Consistency & Master Data") {
    return "daily";
  }

  if (definition.category === "System Connectivity & Availability") {
    return "frequent";
  }

  return "batch";
}

function withResolvedTier<T extends KpiDefinition>(definition: T): T {
  return {
    ...definition,
    tier: definition.tier ?? inferKpiTier(definition),
  };
}

function readByteValue(value: unknown): number | undefined {
  if (value instanceof Uint8Array) {
    return value[0];
  }

  return undefined;
}

function normalizeRecordKey(value: string): string {
  return value.trim().toUpperCase();
}

function parseNumericValue(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "bigint") {
    return Number(value);
  }

  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim().replaceAll(",", "");

  if (!/^-?\d+(\.\d+)?$/.test(normalized)) {
    return undefined;
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseIntegerText(value: string): number | undefined {
  const normalized = value.trim();

  if (!/^-?\d+$/.test(normalized)) {
    return undefined;
  }

  const parsed = Number(normalized);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
}

function parseBigIntValue(value: string): bigint | undefined {
  const normalized = value.trim();

  if (!/^-?\d+$/.test(normalized)) {
    return undefined;
  }

  return BigInt(normalized);
}

function isSapDateInWindow(dateValue: string, window: ResolvedWindow): boolean {
  const normalized = dateValue.trim();

  if (normalized.length !== 8 || normalized === "00000000") {
    return false;
  }

  return normalized >= window.sapFrom && normalized <= window.sapTo;
}

function getObjectEntries(
  value: Record<string, unknown>,
): Array<[string, unknown]> {
  return Object.entries(value);
}

function collectRecords(
  value: unknown,
  preferredKeys: string[] = [],
): Array<Record<string, unknown>> {
  if (!value || typeof value !== "object") {
    return [];
  }

  const record = value as Record<string, unknown>;
  const normalizedPreferred = new Set(
    preferredKeys.map((key) => normalizeRecordKey(key)),
  );
  const preferred: Array<Record<string, unknown>> = [];
  const fallback: Array<Record<string, unknown>> = [];

  for (const [key, entry] of getObjectEntries(record)) {
    if (!Array.isArray(entry)) {
      continue;
    }

    const rows = entry.filter(
      (item): item is Record<string, unknown> =>
        typeof item === "object" && item !== null,
    );

    if (rows.length === 0) {
      continue;
    }

    if (normalizedPreferred.has(normalizeRecordKey(key))) {
      preferred.push(...rows);
    } else {
      fallback.push(...rows);
    }
  }

  return preferred.length > 0 ? preferred : fallback;
}

function collectNumericValues(
  value: unknown,
  options: {
    preferredKeys?: string[];
    exactKeys?: string[];
    regexes?: RegExp[];
  } = {},
): number[] {
  const normalizedExact = new Set(
    (options.exactKeys ?? []).map((key) => normalizeRecordKey(key)),
  );
  const regexes = options.regexes ?? [];
  const values: number[] = [];

  const visit = (current: unknown): void => {
    if (Array.isArray(current)) {
      for (const item of current) {
        visit(item);
      }
      return;
    }

    if (!current || typeof current !== "object") {
      return;
    }

    for (const [key, entry] of getObjectEntries(current as Record<string, unknown>)) {
      const normalizedKey = normalizeRecordKey(key);
      const shouldInclude =
        normalizedExact.has(normalizedKey) ||
        regexes.some((pattern) => pattern.test(normalizedKey));

      if (shouldInclude) {
        const parsed = parseNumericValue(entry);
        if (parsed !== undefined) {
          values.push(parsed);
        }
      }

      if (Array.isArray(entry) || (entry && typeof entry === "object")) {
        visit(entry);
      }
    }
  };

  const records = collectRecords(value, options.preferredKeys);

  if (records.length > 0) {
    visit(records);
  } else {
    visit(value);
  }

  return values;
}

interface MetricCollectionOptions {
  preferredKeys?: string[];
  exactKeys?: string[];
  regexes?: RegExp[];
}

function metricKeyMatches(key: string, options: MetricCollectionOptions): boolean {
  const normalizedKey = normalizeRecordKey(key);
  const exactKeys = new Set(
    (options.exactKeys ?? []).map((entry) => normalizeRecordKey(entry)),
  );

  return (
    exactKeys.has(normalizedKey) ||
    (options.regexes ?? []).some((pattern) => pattern.test(normalizedKey))
  );
}

function collectMetricValuesFromRecords(
  value: unknown,
  options: MetricCollectionOptions,
): number[] {
  const records = collectRecords(value, options.preferredKeys);

  if (records.length === 0) {
    return collectNumericValues(value, options);
  }

  const values: number[] = [];

  for (const record of records) {
    const recordValues = getObjectEntries(record)
      .flatMap(([key, entry]) => {
        if (!metricKeyMatches(key, options)) {
          return [];
        }

        const parsed = parseNumericValue(entry);
        return parsed === undefined ? [] : [parsed];
      });

    if (recordValues.length > 0) {
      values.push(Math.max(...recordValues));
    }
  }

  return values;
}

function collectTextFragments(value: unknown): string[] {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length > 0 ? [trimmed] : [];
  }

  if (Array.isArray(value)) {
    return value.flatMap((entry) => collectTextFragments(entry));
  }

  if (!value || typeof value !== "object") {
    return [];
  }

  return getObjectEntries(value as Record<string, unknown>)
    .flatMap(([, entry]) => collectTextFragments(entry));
}

function parseFlexibleDateTime(
  dateValue: string,
  timeValue?: string,
): Date | undefined {
  const dateDigits = dateValue.replaceAll(/\D/g, "");

  if (dateDigits.length !== 8) {
    return undefined;
  }

  const timeDigits = (timeValue ?? "").replaceAll(/\D/g, "").padEnd(6, "0");
  const normalizedTime =
    timeDigits.length >= 6 ? timeDigits.slice(0, 6) : "000000";
  const year = Number(dateDigits.slice(0, 4));
  const month = Number(dateDigits.slice(4, 6));
  const day = Number(dateDigits.slice(6, 8));
  const hour = Number(normalizedTime.slice(0, 2));
  const minute = Number(normalizedTime.slice(2, 4));
  const second = Number(normalizedTime.slice(4, 6));

  if (
    [year, month, day, hour, minute, second].some(
      (value) => !Number.isFinite(value),
    )
  ) {
    return undefined;
  }

  return new Date(Date.UTC(year, month - 1, day, hour, minute, second));
}

function parseFlexibleTimestamp(value: string): Date | undefined {
  const trimmed = value.trim();

  if (trimmed.length === 0) {
    return undefined;
  }

  const directDate = parseFlexibleDateTime(trimmed);
  if (directDate) {
    return directDate;
  }

  const digitsOnly = trimmed.replaceAll(/\D/g, "");

  if (digitsOnly.length >= 14) {
    return parseFlexibleDateTime(
      digitsOnly.slice(0, 8),
      digitsOnly.slice(8, 14),
    );
  }

  const parsed = new Date(trimmed);
  return Number.isNaN(parsed.getTime()) ? undefined : parsed;
}

function extractRecordDateTime(
  record: Record<string, unknown>,
): Date | undefined {
  for (const [key, entry] of getObjectEntries(record)) {
    if (typeof entry !== "string") {
      continue;
    }

    const normalizedKey = normalizeRecordKey(key);
    if (!/(TIMESTAMP|TIME_STAMP|TSTMP|UTC)/.test(normalizedKey)) {
      continue;
    }

    const parsed = parseFlexibleTimestamp(entry);
    if (parsed) {
      return parsed;
    }
  }

  const dateCandidates = getObjectEntries(record)
    .filter(([key, entry]) =>
      typeof entry === "string" &&
      /(DATE|DATUM|SLGDAT|UDATE|AEDAT|DAT00|CREDAT|LOGDAT|ERDAT|LUPD_DATE)/.test(
        normalizeRecordKey(key),
      ),
    )
    .map(([, entry]) => String(entry));
  const timeCandidates = getObjectEntries(record)
    .filter(([key, entry]) =>
      typeof entry === "string" &&
      /(TIME|UZEIT|ZEIT|TIMS|SLGTIM|UTIME|AEZET|CRETIME|LOGTIM|LUPD_TIME)/.test(
        normalizeRecordKey(key),
      ),
    )
    .map(([, entry]) => String(entry));

  for (const dateValue of dateCandidates) {
    const parsedWithTime = parseFlexibleDateTime(dateValue, timeCandidates[0]);
    if (parsedWithTime) {
      return parsedWithTime;
    }
  }

  return undefined;
}

function filterRecordsToWindow(
  records: Array<Record<string, unknown>>,
  window: ResolvedWindow,
): Array<Record<string, unknown>> {
  const windowStart = new Date(window.from).getTime();
  const windowEnd = new Date(window.to).getTime();
  const datedRecords = records
    .map((record) => ({
      record,
      timestamp: extractRecordDateTime(record),
    }))
    .filter(
      (
        entry,
      ): entry is { record: Record<string, unknown>; timestamp: Date } =>
        entry.timestamp !== undefined,
    );

  if (datedRecords.length === 0) {
    return records;
  }

  return datedRecords
    .filter((entry) => {
      const timestamp = entry.timestamp.getTime();
      return timestamp >= windowStart && timestamp <= windowEnd;
    })
    .map((entry) => entry.record);
}

function recordContainsTerms(
  record: Record<string, unknown>,
  terms: string[],
): boolean {
  const haystack = collectTextFragments(record).join(" ").toUpperCase();

  return terms.some((term) => haystack.includes(term.toUpperCase()));
}

function average(values: number[]): number {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function roundValue(value: number, decimals = 2): number {
  return Number(value.toFixed(decimals));
}

function extractServerAvailabilitySnapshot(result: Record<string, unknown>): {
  total: number;
  activeCount: number;
  instances: Array<{ name: string; active: boolean; stateCode?: number }>;
} {
  const rawRows = Array.isArray(result.LIST_IPV6)
    ? (result.LIST_IPV6 as Array<Record<string, unknown>>)
    : Array.isArray(result.LIST)
      ? (result.LIST as Array<Record<string, unknown>>)
      : [];
  const instances = rawRows.map((row) => {
    const name = String(row.NAME ?? row.HOST ?? "").trim();
    const stateCode = readByteValue(row.STATE);
    const active = stateCode === 1;

    return {
      name: name.length > 0 ? name : "unknown",
      active,
      stateCode,
    };
  });

  return {
    total: instances.length,
    activeCount: instances.filter((instance) => instance.active).length,
    instances,
  };
}

async function scanRowsWithFieldFallbacks(
  helpers: KpiExecutionHelpers,
  request: {
    table: string;
    candidateFields: string[][];
    where?: string[];
    pageSize?: number;
    scanCap?: number;
  },
): Promise<{ rows: Array<Record<string, string>>; fields: string[] }> {
  let lastError: unknown;

  for (const fields of request.candidateFields) {
    try {
      const rows = await helpers.scanRows({
        table: request.table,
        fields,
        where: request.where,
        pageSize: request.pageSize,
        scanCap: request.scanCap,
      });

      return { rows, fields };
    } catch (error) {
      lastError = error;
    }
  }

  throw new Error(
    `Unable to read ${request.table} with any candidate field set. ${describeError(
      lastError,
    )}`,
  );
}

function buildSwncParameters(
  window: ResolvedWindow,
  input: KpiRequestInput,
): Record<string, unknown> {
  const periodType = (
    input.dimensions?.swnc_period_type ??
    "D"
  ).trim().toUpperCase();
  const derivedPeriodStart =
    periodType === "M"
      ? `${window.sapFrom.slice(0, 6)}01`
      : window.sapFrom;

  return {
    COMPONENT: input.dimensions?.swnc_component ?? "TOTAL",
    PERIODTYPE: periodType,
    PERIODSTRT: input.dimensions?.swnc_period_start ?? derivedPeriodStart,
    SUMMARY_ONLY: input.dimensions?.swnc_summary_only ?? "X",
  };
}

const failedJobCount: ExecutableKpiDefinition = {
  id: "failed_job_count",
  title: "Failed Job Count",
  category: "Job & Batch Monitoring",
  unit: "count",
  maturity: "implemented",
  summary: "Aborted background jobs in the requested time window.",
  source: { kind: "table", objects: ["TBTCO"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "TBTCO",
      fields: ["JOBNAME"],
      where: [
        "STATUS EQ 'A'",
        `ENDDATE GE '${window.sapFrom}'`,
        `ENDDATE LE '${window.sapTo}'`,
      ],
      scanCap: 50000,
    });

    return countResult(this, count, [], window);
  },
};

const delayedJobCount: ExecutableKpiDefinition = {
  id: "delayed_job_count",
  title: "Delayed Job Count",
  category: "Job & Batch Monitoring",
  unit: "count",
  maturity: "implemented",
  summary: "Jobs whose actual start time exceeded the scheduled start time.",
  source: { kind: "derived", objects: ["TBTCO"] },
  notes: [
    "Uses the configured S/4-compatible table-reader chain over TBTCO. Replace with a custom RFC if volume becomes large.",
  ],
  async execute(helpers, input) {
    const thresholdMinutes = helpers.getNumberDimension(
      input,
      "delay_minutes",
      15,
    );
    const window = helpers.resolveWindow(input, 24);
    const rows = await helpers.scanRows({
      table: "TBTCO",
      fields: ["JOBNAME", "SDLSTRTDT", "SDLSTRTTM", "STRTDATE", "STRTTIME", "STATUS"],
      where: [
        `STRTDATE GE '${window.sapFrom}'`,
        `STRTDATE LE '${window.sapTo}'`,
        "STATUS NE 'S'",
      ],
      pageSize: 200,
      scanCap: 50000,
    });

    const delayedCount = rows.filter((row) => {
      const scheduled = helpers.parseSapDateTime(
        row.SDLSTRTDT ?? "",
        row.SDLSTRTTM ?? "",
      );
      const actual = helpers.parseSapDateTime(
        row.STRTDATE ?? "",
        row.STRTTIME ?? "",
      );

      if (!scheduled || !actual) {
        return false;
      }

      const deltaMinutes = (actual.getTime() - scheduled.getTime()) / 60000;
      return deltaMinutes > thresholdMinutes;
    }).length;

    return countResult(
      this,
      delayedCount,
      [`Delay threshold: ${thresholdMinutes} minutes.`, ...(this.notes ?? [])],
      window,
    );
  },
};

const activeUserCount: ExecutableKpiDefinition = {
  id: "active_user_count",
  title: "Active User Count",
  category: "System Connectivity & Availability",
  unit: "count",
  maturity: "implemented",
  summary: "Current live SAP session count.",
  source: { kind: "table", objects: ["USR41"] },
  async execute(helpers) {
    const count = await helpers.countRows({
      table: "USR41",
      fields: ["BNAME"],
      scanCap: 20000,
    });

    return countResult(this, count, ["Live session count."]);
  },
};

const workProcessUtilization: ExecutableKpiDefinition = {
  id: "work_process_utilization",
  title: "Work Process Utilization",
  category: "System Performance",
  unit: "percent",
  maturity: "implemented",
  summary: "Current share of SAP work processes that are not idle.",
  source: { kind: "rfc", objects: ["TH_WPINFO"] },
  notes: [
    "Computed from the live TH_WPINFO snapshot. Busy work processes are counted as all statuses other than 'Waiting'.",
  ],
  async execute(helpers) {
    const result = await helpers.callFunction("TH_WPINFO", {});
    const rawRows = Array.isArray(result.WPLIST)
      ? (result.WPLIST as Array<Record<string, unknown>>)
      : [];
    const total = rawRows.length;
    const busy = rawRows.filter((row) => {
      const status = String(row.WP_STATUS ?? "").trim().toUpperCase();
      return status.length > 0 && status !== "WAITING";
    }).length;
    const value =
      total === 0 ? 0 : Number(((busy / total) * 100).toFixed(2));

    return countResult(
      this,
      value,
      [`Busy work processes: ${busy}.`, `Total work processes: ${total}.`, ...(this.notes ?? [])],
    );
  },
};

const unauthorizedLoginAttempts: ExecutableKpiDefinition = {
  id: "unauthorized_login_attempts",
  title: "Unauthorized Login Attempts",
  category: "System Connectivity & Availability",
  unit: "count",
  maturity: "implemented",
  summary: "Failed logon attempts captured by Security Audit Log in the requested window.",
  source: { kind: "table", objects: ["RSECACTPROT"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "RSECACTPROT",
      fields: ["UNAME"],
      where: [
        "EVENT EQ 'AU1'",
        "SUBEVENT EQ 'F'",
        `SLGDAT GE '${window.sapFrom}'`,
        `SLGDAT LE '${window.sapTo}'`,
      ],
      scanCap: 20000,
    });

    return countResult(
      this,
      count,
      [
        "Requires Security Audit Log to be enabled and retained.",
        "This system measures the same SAL event pattern as 'failed_login_attempts'. Keep both only if the dashboard needs separate labels.",
      ],
      window,
    );
  },
};

const failedLoginAttempts: ExecutableKpiDefinition = {
  id: "failed_login_attempts",
  title: "Failed Login Attempts",
  category: "Security & Authorization",
  unit: "count",
  maturity: "implemented",
  summary: "Failed SAP logon attempts in the requested window.",
  source: { kind: "table", objects: ["RSECACTPROT"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "RSECACTPROT",
      fields: ["UNAME"],
      where: [
        "EVENT EQ 'AU1'",
        "SUBEVENT EQ 'F'",
        `SLGDAT GE '${window.sapFrom}'`,
        `SLGDAT LE '${window.sapTo}'`,
      ],
      scanCap: 20000,
    });

    return countResult(
      this,
      count,
      [
        "Requires Security Audit Log to be enabled and retained.",
        "This system measures the same SAL event pattern as 'unauthorized_login_attempts'. Keep both only if the dashboard needs separate labels.",
      ],
      window,
    );
  },
};

const abapDumpFrequency: ExecutableKpiDefinition = {
  id: "abap_dump_frequency",
  title: "ABAP Dump Frequency (ST22)",
  category: "System Performance",
  unit: "count",
  maturity: "implemented",
  summary: "Runtime dumps recorded in the requested window.",
  source: { kind: "table", objects: ["SNAP"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "SNAP",
      fields: ["SEQNO"],
      where: [`DATUM GE '${window.sapFrom}'`, `DATUM LE '${window.sapTo}'`],
    });

    return countResult(this, count, [], window);
  },
};

const backgroundJobThroughput: ExecutableKpiDefinition = {
  id: "background_job_throughput",
  title: "Background Job Throughput",
  category: "System Performance",
  unit: "count",
  maturity: "implemented",
  summary: "Finished background jobs in the requested window.",
  source: { kind: "table", objects: ["TBTCO"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "TBTCO",
      fields: ["JOBNAME"],
      where: [
        "STATUS EQ 'F'",
        `ENDDATE GE '${window.sapFrom}'`,
        `ENDDATE LE '${window.sapTo}'`,
      ],
      scanCap: 200000,
    });

    return countResult(this, count, [], window);
  },
};

const longRunningJobCount: ExecutableKpiDefinition = {
  id: "long_running_job_count",
  title: "Long-Running Job Count",
  category: "Job & Batch Monitoring",
  unit: "count",
  maturity: "implemented",
  summary: "Running jobs older than the configured threshold.",
  source: { kind: "derived", objects: ["TBTCO"] },
  async execute(helpers, input) {
    const thresholdMinutes = helpers.getNumberDimension(
      input,
      "long_running_minutes",
      120,
    );
    const rows = await helpers.scanRows({
      table: "TBTCO",
      fields: ["JOBNAME", "STRTDATE", "STRTTIME", "STATUS"],
      where: ["STATUS EQ 'R'"],
      pageSize: 200,
      scanCap: 5000,
    });

    const count = rows.filter((row) => {
      const startedAt = helpers.parseSapDateTime(
        row.STRTDATE ?? "",
        row.STRTTIME ?? "",
      );

      if (!startedAt) {
        return false;
      }

      return (Date.now() - startedAt.getTime()) / 60000 > thresholdMinutes;
    }).length;

    return countResult(this, count, [
      `Running duration threshold: ${thresholdMinutes} minutes.`,
    ]);
  },
};

const jobSuccessRate: ExecutableKpiDefinition = {
  id: "job_success_rate",
  title: "Job Success Rate",
  category: "Job & Batch Monitoring",
  unit: "percent",
  maturity: "implemented",
  summary: "Finished jobs divided by finished plus aborted jobs in the requested window.",
  source: { kind: "derived", objects: ["TBTCO"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const [finishedJobs, abortedJobs] = await Promise.all([
      helpers.countRows({
        table: "TBTCO",
        fields: ["JOBNAME"],
        where: [
          "STATUS EQ 'F'",
          `ENDDATE GE '${window.sapFrom}'`,
          `ENDDATE LE '${window.sapTo}'`,
        ],
        scanCap: 200000,
      }),
      helpers.countRows({
        table: "TBTCO",
        fields: ["JOBNAME"],
        where: [
          "STATUS EQ 'A'",
          `ENDDATE GE '${window.sapFrom}'`,
          `ENDDATE LE '${window.sapTo}'`,
        ],
        scanCap: 50000,
      }),
    ]);

    const denominator = finishedJobs + abortedJobs;
    const value =
      denominator === 0
        ? 0
        : Number(((finishedJobs / denominator) * 100).toFixed(2));

    return countResult(
      this,
      value,
      [`Finished jobs: ${finishedJobs}.`, `Aborted jobs: ${abortedJobs}.`],
      window,
    );
  },
};

const totalIdocsProcessed: ExecutableKpiDefinition = {
  id: "total_idocs_processed",
  title: "Total IDocs Processed",
  category: "Integration & Interfaces",
  unit: "count",
  maturity: "implemented",
  summary: "IDoc control records created in the requested window.",
  source: { kind: "table", objects: ["EDIDC"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "EDIDC",
      fields: ["DOCNUM"],
      where: [`CREDAT GE '${window.sapFrom}'`, `CREDAT LE '${window.sapTo}'`],
    });

    return countResult(this, count, [], window);
  },
};

const idocsInError: ExecutableKpiDefinition = {
  id: "idocs_in_error",
  title: "IDocs in Error",
  category: "Integration & Interfaces",
  unit: "count",
  maturity: "implemented",
  summary: "IDocs in an error-like status in the requested window.",
  source: { kind: "table", objects: ["EDIDC"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const statuses = ["51", "52", "56", "63", "65", "66", "69"];
    const total = await helpers.countRows({
      table: "EDIDC",
      fields: ["DOCNUM"],
      where: [
        `STATUS IN ('${statuses.join("','")}')`,
        `CREDAT GE '${window.sapFrom}'`,
        `CREDAT LE '${window.sapTo}'`,
      ],
    });

    return countResult(
      this,
      total,
      ["Statuses counted with one SQL clause: 51, 52, 56, 63, 65, 66, 69."],
      window,
    );
  },
};

const reprocessingSuccessRate: ExecutableKpiDefinition = {
  id: "reprocessing_success_rate",
  title: "Reprocessing Success Rate",
  category: "Integration & Interfaces",
  unit: "percent",
  maturity: "implemented",
  summary: "IDocs that moved from an error status to status 53 in the requested window.",
  source: { kind: "derived", objects: ["EDIDS"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const trackedStatuses = ["51", "52", "56", "63", "65", "66", "69", "53"];
    const rows = await helpers.scanRows({
      table: "EDIDS",
      fields: ["DOCNUM", "STATUS", "LOGDAT"],
      where: [
        `LOGDAT GE '${window.sapFrom}'`,
        `LOGDAT LE '${window.sapTo}'`,
      ],
      pageSize: 500,
      scanCap: 20000,
    });

    const trackedRows = rows.filter((row) =>
      trackedStatuses.includes(row.STATUS ?? ""),
    );
    const byDocnum = new Map<string, Set<string>>();

    for (const row of trackedRows) {
      const docnum = row.DOCNUM?.trim();
      const status = row.STATUS?.trim();

      if (!docnum || !status) {
        continue;
      }

      const current = byDocnum.get(docnum) ?? new Set<string>();
      current.add(status);
      byDocnum.set(docnum, current);
    }

    const errorStatuses = ["51", "52", "56", "63", "65", "66", "69"];
    let errorDocCount = 0;
    let recoveredDocCount = 0;

    for (const statuses of byDocnum.values()) {
      const hasError = errorStatuses.some((status) => statuses.has(status));

      if (!hasError) {
        continue;
      }

      errorDocCount += 1;

      if (statuses.has("53")) {
        recoveredDocCount += 1;
      }
    }

    const value =
      errorDocCount === 0
        ? 0
        : Number(((recoveredDocCount / errorDocCount) * 100).toFixed(2));

    return countResult(
      this,
      value,
      [
        `Error IDocs observed: ${errorDocCount}.`,
        `Recovered IDocs observed: ${recoveredDocCount}.`,
      ],
      window,
    );
  },
};

const idocBacklogVolume: ExecutableKpiDefinition = {
  id: "idoc_backlog_volume",
  title: "IDoc Backlog Volume",
  category: "Integration & Interfaces",
  unit: "count",
  maturity: "implemented",
  summary: "IDocs waiting in backlog-like states in the requested window.",
  source: { kind: "table", objects: ["EDIDC"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const statuses = ["30", "64", "66", "69"];
    const total = await helpers.countRows({
      table: "EDIDC",
      fields: ["DOCNUM"],
      where: [
        `STATUS IN ('${statuses.join("','")}')`,
        `CREDAT GE '${window.sapFrom}'`,
        `CREDAT LE '${window.sapTo}'`,
      ],
    });
    return countResult(
      this,
      total,
      ["Statuses counted with one SQL clause: 30, 64, 66, 69."],
      window,
    );
  },
};

const lockedUsers: ExecutableKpiDefinition = {
  id: "locked_users",
  title: "Locked Users",
  category: "Security & Authorization",
  unit: "count",
  maturity: "implemented",
  summary: "Users with lock flags in USR02.",
  source: { kind: "table", objects: ["USR02"] },
  async execute(helpers) {
    const flags = ["32", "64", "128"];
    const total = await helpers.countRows({
      table: "USR02",
      fields: ["BNAME"],
      where: [`UFLAG IN ('${flags.join("','")}')`],
    });

    return countResult(
      this,
      total,
      ["Lock flags counted with one SQL clause: 32, 64, 128."],
    );
  },
};

const rfcUserPasswordAge: ExecutableKpiDefinition = {
  id: "rfc_user_password_age",
  title: "RFC User Password Age",
  category: "Security & Authorization",
  unit: "days",
  maturity: "implemented",
  summary: "Oldest password age across technical SAP users.",
  source: { kind: "derived", objects: ["USR02"] },
  async execute(helpers) {
    const users = await helpers.scanRows({
      table: "USR02",
      fields: ["BNAME", "USTYP", "PWDLGDATE"],
      where: ["USTYP EQ 'S'"],
      pageSize: 500,
      scanCap: 5000,
    });

    const ages = users
      .map((row) => helpers.daysSinceSapDate(row.PWDLGDATE ?? ""))
      .filter((value): value is number => value !== undefined);

    const value = ages.length === 0 ? 0 : Math.max(...ages);
    const average =
      ages.length === 0
        ? 0
        : Number(
            (ages.reduce((sum, current) => sum + current, 0) / ages.length).toFixed(
              2,
            ),
          );

    return countResult(this, value, [
      `Technical users scanned: ${users.length}.`,
      `Average password age: ${average} days.`,
      "Value returned is the oldest password age across scanned RFC-style users.",
    ]);
  },
};

const inactiveUsers: ExecutableKpiDefinition = {
  id: "inactive_users",
  title: "Inactive Users",
  category: "Security & Authorization",
  unit: "count",
  maturity: "implemented",
  summary: "Users whose last login date is older than the configured threshold.",
  source: { kind: "table", objects: ["USR02"] },
  async execute(helpers, input) {
    const days = helpers.getNumberDimension(input, "inactive_days", 90);
    const cutoffDate = helpers.toSapDateDaysAgo(days);
    const inactiveCount = await helpers.countRows({
      table: "USR02",
      fields: ["BNAME"],
      where: [`TRDAT LT '${cutoffDate}'`],
    });
    const neverLoggedInCount = await helpers.countRows({
      table: "USR02",
      fields: ["BNAME"],
      where: ["TRDAT EQ '00000000'"],
    });

    return countResult(this, inactiveCount + neverLoggedInCount, [
      `Inactive threshold: ${days} days.`,
    ]);
  },
};

const postingErrors: ExecutableKpiDefinition = {
  id: "posting_errors",
  title: "Posting Errors",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Accounting documents in a non-posted status in the requested window.",
  source: { kind: "table", objects: ["BKPF"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "BKPF",
      fields: ["BELNR"],
      where: [
        `BUDAT GE '${window.sapFrom}'`,
        `BUDAT LE '${window.sapTo}'`,
        "BSTAT NE ' '",
      ],
    });

    return countResult(this, count, [], window);
  },
};

const unpostedBillingDocuments: ExecutableKpiDefinition = {
  id: "unposted_billing_documents",
  title: "Unposted Billing Documents",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Billing documents not transferred to FI in the requested window.",
  source: { kind: "table", objects: ["VBRK"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "VBRK",
      fields: ["VBELN"],
      where: [
        `FKDAT GE '${window.sapFrom}'`,
        `FKDAT LE '${window.sapTo}'`,
        "RFBSK NE 'C'",
      ],
    });

    return countResult(this, count, [], window);
  },
};

const apInvoices: ExecutableKpiDefinition = {
  id: "ap_invoices",
  title: "AP Invoices",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "AP invoice document volume in the requested window.",
  source: { kind: "table", objects: ["RBKP"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "RBKP",
      fields: ["BELNR"],
      where: [
        `BLDAT GE '${window.sapFrom}'`,
        `BLDAT LE '${window.sapTo}'`,
      ],
    });

    return countResult(this, count, [], window);
  },
};

const arInvoices: ExecutableKpiDefinition = {
  id: "ar_invoices",
  title: "AR Invoices",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "AR billing document volume in the requested window.",
  source: { kind: "table", objects: ["VBRK"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "VBRK",
      fields: ["VBELN"],
      where: [
        `FKDAT GE '${window.sapFrom}'`,
        `FKDAT LE '${window.sapTo}'`,
      ],
    });

    return countResult(this, count, [], window);
  },
};

const glPosted: ExecutableKpiDefinition = {
  id: "gl_posted",
  title: "GL Posted",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Posted GL document volume in the requested window.",
  source: { kind: "table", objects: ["BKPF"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "BKPF",
      fields: ["BELNR"],
      where: [
        `BUDAT GE '${window.sapFrom}'`,
        `BUDAT LE '${window.sapTo}'`,
        "BSTAT EQ ' '",
      ],
    });

    return countResult(this, count, [], window);
  },
};

const workOrders: ExecutableKpiDefinition = {
  id: "work_orders",
  title: "Work Orders",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Work order header volume in the requested window.",
  source: { kind: "table", objects: ["AUFK"] },
  notes: ["This first-pass implementation counts AUFK headers. Add order-type scoping if your process requires it."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "AUFK",
      fields: ["AUFNR"],
      where: [
        `ERDAT GE '${window.sapFrom}'`,
        `ERDAT LE '${window.sapTo}'`,
      ],
    });

    return countResult(this, count, this.notes ?? [], window);
  },
};

const notifications: ExecutableKpiDefinition = {
  id: "notifications",
  title: "Notifications",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Notification volume in the requested window.",
  source: { kind: "table", objects: ["QMEL"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "QMEL",
      fields: ["QMNUM"],
      where: [
        `ERDAT GE '${window.sapFrom}'`,
        `ERDAT LE '${window.sapTo}'`,
      ],
    });

    return countResult(this, count, [], window);
  },
};

const purchaseOrdersCreated: ExecutableKpiDefinition = {
  id: "pos_created",
  title: "POs Created",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Purchase order header volume in the requested window.",
  source: { kind: "table", objects: ["EKKO"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "EKKO",
      fields: ["EBELN"],
      where: [
        `BEDAT GE '${window.sapFrom}'`,
        `BEDAT LE '${window.sapTo}'`,
      ],
    });

    return countResult(this, count, [], window);
  },
};

const materialsCreated: ExecutableKpiDefinition = {
  id: "materials_created",
  title: "Materials Created",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Material master records created in the requested window.",
  source: { kind: "table", objects: ["MARA"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "MARA",
      fields: ["MATNR"],
      where: [
        `ERSDA GE '${window.sapFrom}'`,
        `ERSDA LE '${window.sapTo}'`,
      ],
    });

    return countResult(this, count, [], window);
  },
};

const deliveryBlockRate: ExecutableKpiDefinition = {
  id: "delivery_block_rate",
  title: "Delivery Block Rate",
  category: "Business Process KPIs",
  unit: "percent",
  maturity: "implemented",
  summary: "Ratio of sales orders carrying a delivery block in the requested window.",
  source: { kind: "derived", objects: ["VBAK"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const baseWhere = [
      `ERDAT GE '${window.sapFrom}'`,
      `ERDAT LE '${window.sapTo}'`,
    ];
    const [totalOrders, blockedOrders] = await Promise.all([
      helpers.countRows({
        table: "VBAK",
        fields: ["VBELN"],
        where: baseWhere,
      }),
      helpers.countRows({
        table: "VBAK",
        fields: ["VBELN"],
        where: [...baseWhere, "LIFSK NE ' '"],
      }),
    ]);

    const value =
      totalOrders === 0
        ? 0
        : Number(((blockedOrders / totalOrders) * 100).toFixed(2));

    return countResult(
      this,
      value,
      [`Blocked orders: ${blockedOrders}.`, `Total orders: ${totalOrders}.`],
      window,
    );
  },
};

const transportRequestBacklog: ExecutableKpiDefinition = {
  id: "transport_request_backlog",
  title: "Transport Request Backlog",
  category: "System Connectivity & Availability",
  unit: "count",
  maturity: "implemented",
  summary: "Transport requests that are still not released in the requested time window.",
  source: { kind: "table", objects: ["E070"] },
  notes: [
    "Uses E070.TRSTATUS != 'R' as the backlog signal. Import success or failure still needs STMS or TPLOG integration.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "E070",
      fields: ["TRKORR"],
      where: [
        "TRSTATUS NE 'R'",
        `AS4DATE GE '${window.sapFrom}'`,
        `AS4DATE LE '${window.sapTo}'`,
      ],
    });

    return countResult(this, count, this.notes ?? [], window);
  },
};

const workItemBacklog: ExecutableKpiDefinition = {
  id: "work_item_backlog",
  title: "Work Item Backlog",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Workflow work items that are still waiting for action.",
  source: { kind: "table", objects: ["SWWWIHEAD"] },
  notes: [
    "Counts READY and STARTED workflow items. Tighten the status set if the workflow team wants a narrower backlog definition.",
  ],
  async execute(helpers) {
    const count = await helpers.countRows({
      table: "SWWWIHEAD",
      fields: ["WI_ID"],
      where: ["WI_STAT IN ('READY','STARTED')"],
    });

    return countResult(this, count, this.notes ?? []);
  },
};

const spoolQueueErrors: ExecutableKpiDefinition = {
  id: "spool_queue_errors",
  title: "Spool Queue Errors",
  category: "System Performance",
  unit: "count",
  maturity: "implemented",
  summary: "Current spool requests with a non-zero error flag.",
  source: { kind: "table", objects: ["TSP01"] },
  notes: [
    "This KPI is a current snapshot. Time-windowed spool analysis needs a safer timestamp strategy for RQCRETIME on this system.",
  ],
  async execute(helpers) {
    const count = await helpers.countRows({
      table: "TSP01",
      fields: ["RQIDENT"],
      where: ["RQERROR NE '0'"],
    });

    return countResult(this, count, this.notes ?? []);
  },
};

const applicationServerUptimePerInstance: ExecutableKpiDefinition = {
  id: "application_server_uptime_per_instance",
  title: "Application Server Uptime Per Instance",
  category: "System Connectivity & Availability",
  unit: "percent",
  maturity: "implemented",
  summary:
    "Live application-server availability snapshot derived from TH_SERVER_LIST.",
  source: { kind: "rfc", objects: ["TH_SERVER_LIST"] },
  notes: [
    "This is a live availability snapshot, not a historical uptime SLA. Use persisted polling if you need true uptime percentage over time.",
  ],
  async execute(helpers) {
    const result = await helpers.callFunction("TH_SERVER_LIST", {});
    const snapshot = extractServerAvailabilitySnapshot(result);
    const value =
      snapshot.total === 0
        ? 0
        : roundValue((snapshot.activeCount / snapshot.total) * 100);

    return countResult(
      this,
      value,
      [
        `Active instances: ${snapshot.activeCount}/${snapshot.total}.`,
        `Instance states: ${snapshot.instances
          .map((instance) =>
            `${instance.name}=${instance.active ? "active" : `state_${instance.stateCode ?? "unknown"}`}`,
          )
          .join(", ") || "none"}.`,
        ...(this.notes ?? []),
      ],
    );
  },
};

const batchWindowUtilizationPct: ExecutableKpiDefinition = {
  id: "batch_window_utilization_pct",
  title: "Batch Window Utilization %",
  category: "Job & Batch Monitoring",
  unit: "percent",
  maturity: "implemented",
  summary:
    "Approximate batch window utilization derived from aggregate job runtime inside the requested window.",
  source: { kind: "derived", objects: ["TBTCO"] },
  notes: [
    "This calculation sums job runtime overlap against the requested window and caps the result at 100%. Heavy overlap still indicates saturation pressure even when the KPI is capped.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const windowStart = new Date(window.from);
    const windowEnd = new Date(window.to);
    const windowMinutes = (windowEnd.getTime() - windowStart.getTime()) / 60000;
    const rows = await helpers.scanRows({
      table: "TBTCO",
      fields: ["JOBNAME", "STRTDATE", "STRTTIME", "ENDDATE", "ENDTIME", "STATUS"],
      where: [
        "STATUS IN ('F','A')",
        `ENDDATE GE '${window.sapFrom}'`,
        `ENDDATE LE '${window.sapTo}'`,
      ],
      pageSize: 500,
      scanCap: 200000,
    });

    let runtimeMinutes = 0;
    let countedJobs = 0;

    for (const row of rows) {
      const startedAt = helpers.parseSapDateTime(
        row.STRTDATE ?? "",
        row.STRTTIME ?? "",
      );
      const endedAt = helpers.parseSapDateTime(
        row.ENDDATE ?? "",
        row.ENDTIME ?? "",
      );

      if (!startedAt || !endedAt || endedAt <= startedAt) {
        continue;
      }

      const overlapStart = Math.max(startedAt.getTime(), windowStart.getTime());
      const overlapEnd = Math.min(endedAt.getTime(), windowEnd.getTime());

      if (overlapEnd <= overlapStart) {
        continue;
      }

      countedJobs += 1;
      runtimeMinutes += (overlapEnd - overlapStart) / 60000;
    }

    const uncappedValue =
      windowMinutes <= 0 ? 0 : (runtimeMinutes / windowMinutes) * 100;
    const value = Number(Math.min(uncappedValue, 100).toFixed(2));

    return countResult(
      this,
      value,
      [
        `Jobs contributing runtime: ${countedJobs}.`,
        `Aggregate overlapping runtime: ${Number(runtimeMinutes.toFixed(2))} minutes.`,
        ...(this.notes ?? []),
      ],
      window,
    );
  },
};

const numberRangeExhaustionPct: ExecutableKpiDefinition = {
  id: "number_range_exhaustion_pct",
  title: "Number Range Exhaustion %",
  category: "Business Process KPIs",
  unit: "percent",
  maturity: "implemented",
  summary:
    "Highest observed number-range utilization across the scanned NRIV objects.",
  source: { kind: "derived", objects: ["NRIV"] },
  notes: [
    "By default this scans all readable NRIV objects and returns the highest utilization. Use the 'nriv_objects' dimension to scope the KPI to business-relevant objects.",
  ],
  async execute(helpers, input) {
    const scopedObjects = new Set(
      (input.dimensions?.nriv_objects ?? "")
        .split(",")
        .map((value) => value.trim().toUpperCase())
        .filter((value) => value.length > 0),
    );
    const rows = await helpers.scanRows({
      table: "NRIV",
      fields: ["OBJECT", "NRRANGENR", "FROMNUMBER", "TONUMBER", "NRLEVEL"],
      pageSize: 500,
      scanCap: 20000,
    });

    const scopedRows = rows.filter((row) => {
      const objectName = (row.OBJECT ?? "").trim().toUpperCase();
      return scopedObjects.size === 0 || scopedObjects.has(objectName);
    });
    let maxUtilization = 0;
    let maxLabel = "none";

    for (const row of scopedRows) {
      const fromNumber = parseBigIntValue(row.FROMNUMBER ?? "");
      const toNumber = parseBigIntValue(row.TONUMBER ?? "");
      const currentLevel = parseBigIntValue(row.NRLEVEL ?? "");

      if (
        fromNumber === undefined ||
        toNumber === undefined ||
        currentLevel === undefined ||
        toNumber < fromNumber
      ) {
        continue;
      }

      const rangeSize = toNumber - fromNumber + 1n;
      if (rangeSize <= 0n) {
        continue;
      }

      const consumed = currentLevel <= fromNumber ? 0n : currentLevel - fromNumber;
      const rawUtilization = Number(consumed) / Number(rangeSize);
      const utilization = Number(
        Math.max(0, Math.min(rawUtilization, 1)) * 100,
      );

      if (utilization > maxUtilization) {
        maxUtilization = utilization;
        maxLabel = `${(row.OBJECT ?? "").trim()}:${(row.NRRANGENR ?? "").trim()}`;
      }
    }

    return countResult(
      this,
      Number(maxUtilization.toFixed(2)),
      [
        `Ranges scanned: ${scopedRows.length}.`,
        `Highest observed range: ${maxLabel}.`,
        ...(this.notes ?? []),
      ],
    );
  },
};

const peakConcurrentUsers: ExecutableKpiDefinition = {
  id: "peak_concurrent_users",
  title: "Peak Concurrent Users",
  category: "System Connectivity & Availability",
  unit: "count",
  maturity: "implemented",
  summary: "Peak concurrent users from SAP workload aggregates.",
  source: { kind: "rfc", objects: ["SWNC_COLLECTOR_GET_AGGREGATES"] },
  notes: [
    "This reads one SWNC aggregation bucket. Use the 'swnc_period_type' and 'swnc_period_start' dimensions if your landscape keeps workload statistics in a different period layout.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const result = await helpers.callFunction(
      "SWNC_COLLECTOR_GET_AGGREGATES",
      buildSwncParameters(window, input),
    );
    const values = collectNumericValues(result, {
      preferredKeys: ["ASTAT", "FRONTEND", "EXTSYSTEM", "ORG_UNITS"],
      exactKeys: [
        "MAX_CONCURRENT_USERS",
        "PEAK_USERS",
        "MAX_USER_COUNT",
        "PEAK_USER_COUNT",
        "USERS_PEAK",
        "MAX_CONNECTED_USERS",
        "PEAK_SESSIONS",
      ],
      regexes: [
        /(MAX|PEAK).*(USER|SESSION)/,
        /(USER|SESSION).*(MAX|PEAK)/,
      ],
    });

    if (values.length === 0) {
      throw new Error(
        "SWNC response did not expose a recognizable peak-concurrent-user metric.",
      );
    }

    return countResult(
      this,
      Math.max(...values.map((value) => Math.trunc(value))),
      this.notes ?? [],
      window,
    );
  },
};

const dialogResponseTime: ExecutableKpiDefinition = {
  id: "dialog_response_time",
  title: "Dialog Response Time",
  category: "System Performance",
  unit: "ms",
  maturity: "implemented",
  summary: "Average dialog response time from SAP workload aggregates.",
  source: { kind: "rfc", objects: ["SWNC_COLLECTOR_GET_AGGREGATES"] },
  notes: [
    "This reads one SWNC aggregation bucket and expects response-time fields in milliseconds.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const result = await helpers.callFunction(
      "SWNC_COLLECTOR_GET_AGGREGATES",
      buildSwncParameters(window, input),
    );
    const values = collectNumericValues(result, {
      preferredKeys: ["ASTAT", "ASHITL_RESPTIME", "HITLIST_RESPTIME"],
      exactKeys: [
        "AVG_RESPTI",
        "AVRESPTI",
        "DIALOG_RESPTI",
        "AVERAGE_RESPONSE_TIME",
        "AVG_RESPONSE_TIME",
        "RESPTI",
      ],
      regexes: [/(AVG|AVERAGE).*(RESP|RESPONSE)/, /(RESP|RESPONSE).*(TIME|TI)/],
    }).filter((value) => value > 0);

    if (values.length === 0) {
      throw new Error(
        "SWNC response did not expose a recognizable dialog response-time metric.",
      );
    }

    const average =
      values.reduce((sum, value) => sum + value, 0) / values.length;

    return countResult(
      this,
      Number(average.toFixed(2)),
      this.notes ?? [],
      window,
    );
  },
};

const timeoutErrors: ExecutableKpiDefinition = {
  id: "timeout_errors",
  title: "Timeout Errors",
  category: "System Performance",
  unit: "count",
  maturity: "implemented",
  summary: "Timeout-related workload error count from SAP workload aggregates.",
  source: { kind: "rfc", objects: ["SWNC_COLLECTOR_GET_AGGREGATES"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const result = await helpers.callFunction(
      "SWNC_COLLECTOR_GET_AGGREGATES",
      buildSwncParameters(window, input),
    );
    const values = collectNumericValues(result, {
      preferredKeys: ["ASTAT", "FRONTEND", "EXTSYSTEM"],
      exactKeys: [
        "TIMEOUTS",
        "TIMEOUT_CNT",
        "TIMEOUT_ERRORS",
        "NUM_TIMEOUTS",
      ],
      regexes: [/TIME.?OUT/],
    });

    if (values.length === 0) {
      throw new Error(
        "SWNC response did not expose a recognizable timeout metric.",
      );
    }

    return countResult(
      this,
      Math.round(values.reduce((sum, value) => sum + value, 0)),
      [],
      window,
    );
  },
};

const retryAttemptCount: ExecutableKpiDefinition = {
  id: "retry_attempt_count",
  title: "Retry Attempt Count",
  category: "Integration & Interfaces",
  unit: "count",
  maturity: "implemented",
  summary: "Sum of asynchronous RFC retries recorded in ARFCSSTATE within the requested window.",
  source: { kind: "table", objects: ["ARFCSSTATE"] },
  notes: [
    "This implementation sums the TRIES field for rows changed in the requested window.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const rows = await helpers.scanRows({
      table: "ARFCSSTATE",
      fields: ["TRIES", "LSTCHDATE", "LSTCHTIME", "ARFCSTATE"],
      pageSize: 500,
      scanCap: 50000,
    });
    const value = rows.reduce((sum, row) => {
      if (!isSapDateInWindow(row.LSTCHDATE ?? "", window)) {
        return sum;
      }

      const tries = parseIntegerText(row.TRIES ?? "") ?? 0;
      return tries > 0 ? sum + tries : sum;
    }, 0);

    return countResult(this, value, this.notes ?? [], window);
  },
};

const queueLockFailures: ExecutableKpiDefinition = {
  id: "queue_lock_failures",
  title: "Queue Lock Failures",
  category: "Integration & Interfaces",
  unit: "count",
  maturity: "implemented",
  summary: "qRFC queue failures inferred from queue error counters and lock-like states.",
  source: { kind: "table", objects: ["QRFCSSTATE"] },
  notes: [
    "Rows are counted when QERRCNT is positive or QSTATE contains 'LOCK'. Tighten the filter if the target system uses a different queue-state vocabulary.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const rows = await helpers.scanRows({
      table: "QRFCSSTATE",
      fields: ["QERRCNT", "QSTATE", "LUPD_DATE", "LUPD_TIME", "QNAME"],
      pageSize: 500,
      scanCap: 50000,
    });
    const value = rows.filter((row) => {
      if (!isSapDateInWindow(row.LUPD_DATE ?? "", window)) {
        return false;
      }

      const errorCount = parseIntegerText(row.QERRCNT ?? "") ?? 0;
      const queueState = (row.QSTATE ?? "").trim().toUpperCase();

      return errorCount > 0 || queueState.includes("LOCK");
    }).length;

    return countResult(this, value, this.notes ?? [], window);
  },
};

const mrpErrors: ExecutableKpiDefinition = {
  id: "mrp_errors",
  title: "MRP Errors",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "MRP error-message volume from MDLG in the requested window.",
  source: { kind: "table", objects: ["MDLG"] },
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const count = await helpers.countRows({
      table: "MDLG",
      fields: ["DELNR"],
      where: [`DAT00 GE '${window.sapFrom}'`, `DAT00 LE '${window.sapTo}'`],
      scanCap: 50000,
    });

    return countResult(this, count, [], window);
  },
};

const goodsReceipts: ExecutableKpiDefinition = {
  id: "goods_receipts",
  title: "Goods Receipts",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Goods-receipt document volume from MSEG in the requested window.",
  source: { kind: "derived", objects: ["MKPF", "MSEG"] },
  notes: [
    "Default movement types are 101, 103, 105, 107, and 109. Override them with the 'movement_types' dimension if your process uses a different GR scope.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const movementTypes = (input.dimensions?.movement_types ?? "101,103,105,107,109")
      .split(",")
      .map((value) => value.trim())
      .filter((value) => value.length > 0);
    const count = await helpers.countRows({
      table: "MSEG",
      fields: ["MBLNR"],
      where: [
        `BWART IN ('${movementTypes.join("','")}')`,
        `BUDAT_MKPF GE '${window.sapFrom}'`,
        `BUDAT_MKPF LE '${window.sapTo}'`,
      ],
      scanCap: 200000,
    });

    return countResult(
      this,
      count,
      [`Movement types counted: ${movementTypes.join(", ")}.`, ...(this.notes ?? [])],
      window,
    );
  },
};

const sapApplicationUptimePct: ExecutableKpiDefinition = {
  id: "sap_application_uptime_pct",
  title: "SAP Application Uptime %",
  category: "System Connectivity & Availability",
  unit: "percent",
  maturity: "implemented",
  summary:
    "Current system availability snapshot derived from TH_SERVER_LIST instance states.",
  source: { kind: "derived", objects: ["TH_SERVER_LIST"] },
  notes: [
    "This is the live system-availability percentage at read time. Your scheduler history should aggregate repeated samples into a true uptime trend.",
  ],
  async execute(helpers) {
    const result = await helpers.callFunction("TH_SERVER_LIST", {});
    const snapshot = extractServerAvailabilitySnapshot(result);
    const value =
      snapshot.total === 0
        ? 0
        : roundValue((snapshot.activeCount / snapshot.total) * 100);

    return countResult(
      this,
      value,
      [
        `Active instances: ${snapshot.activeCount}/${snapshot.total}.`,
        ...(this.notes ?? []),
      ],
    );
  },
};

const customAuthorizationFailures: NonExecutableKpiDefinition = {
  id: "authorization_failures",
  title: "Authorization Failures (SU53)",
  category: "Security & Authorization",
  maturity: "custom_abap_required",
  summary:
    "System-wide authorization failure counting should not be modeled as a raw SU53 extraction.",
  source: { kind: "derived", objects: ["Security Audit Log", "custom RFC"] },
  blocker:
    "Requires Security Audit Log configuration or a custom wrapper around the chosen security source.",
  wrapper: {
    functionName: "ZHC_GET_SECURITY_KPIS",
    wrapperKpiId: "authorization_failures",
  },
};

const customSodConflicts: NonExecutableKpiDefinition = {
  id: "users_with_sod_conflicts",
  title: "Users with SoD Conflicts",
  category: "Security & Authorization",
  maturity: "custom_abap_required",
  summary: "SoD is a rules-engine problem, not a raw SAP table counter.",
  source: { kind: "derived", objects: ["GRC tables", "custom RFC"] },
  blocker: "Implement only via GRC-backed logic or a custom rules wrapper.",
  wrapper: {
    functionName: "ZHC_GET_SECURITY_KPIS",
    wrapperKpiId: "users_with_sod_conflicts",
  },
};

const customEmergencyAccessSessions: NonExecutableKpiDefinition = {
  id: "emergency_access_sessions",
  title: "Emergency Access Sessions",
  category: "Security & Authorization",
  maturity: "custom_abap_required",
  summary:
    "Emergency access tracking is only reliable when packaged against GRC or the chosen privileged-access source.",
  source: { kind: "derived", objects: ["GRC Firefighter", "custom RFC"] },
  blocker:
    "Requires a security wrapper that encapsulates firefighter session logic and source availability.",
  wrapper: {
    functionName: "ZHC_GET_SECURITY_KPIS",
    wrapperKpiId: "emergency_access_sessions",
  },
};

const customExpiredPasswordPct: NonExecutableKpiDefinition = {
  id: "expired_password_pct",
  title: "Expired Password %",
  category: "Security & Authorization",
  unit: "percent",
  maturity: "custom_abap_required",
  summary:
    "Password-expiry percentage needs policy-aware denominator logic and should be packaged once in SAP.",
  source: { kind: "derived", objects: ["USR02", "profile parameters", "custom RFC"] },
  blocker:
    "Requires a security wrapper that combines password-change dates with the active-user denominator and policy rules.",
  wrapper: {
    functionName: "ZHC_GET_SECURITY_KPIS",
    wrapperKpiId: "expired_password_pct",
  },
};

const excludedDbUptime: NonExecutableKpiDefinition = {
  id: "database_uptime_pct",
  title: "Database Uptime %",
  category: "System Connectivity & Availability",
  unit: "percent",
  maturity: "excluded",
  summary: "Database uptime does not fit a standard RFC-only SAP MCP extractor.",
  source: { kind: "rfc", objects: ["none"] },
  blocker:
    "Keep this out of SAP RFC MCP unless you add a dedicated HANA or custom ABAP monitoring path.",
};

const excludedHanaMemory: NonExecutableKpiDefinition = {
  id: "hana_memory_consumption",
  title: "HANA Memory Consumption",
  category: "System Performance",
  maturity: "excluded",
  summary: "This is HANA-internal monitoring, not a clean RFC KPI.",
  source: { kind: "rfc", objects: ["none"] },
  blocker:
    "Use a dedicated HANA connector or a custom ABAP wrapper if you really need it.",
};

const customPeriodEndClose: NonExecutableKpiDefinition = {
  id: "period_end_closing_errors",
  title: "Period-End Closing Errors",
  category: "Business Process KPIs",
  maturity: "custom_abap_required",
  summary:
    "Period-close status interpretation is workflow-heavy and should be wrapped.",
  source: { kind: "derived", objects: ["FAGLPERI", "custom RFC"] },
  blocker: "Implement through a finance-specific wrapper, not raw table reads.",
  wrapper: {
    functionName: "ZHC_GET_FINANCE_KPIS",
    wrapperKpiId: "period_end_closing_errors",
  },
};

const customMissingMandatoryFields: NonExecutableKpiDefinition = {
  id: "missing_mandatory_fields",
  title: "Missing Mandatory Fields",
  category: "Data Consistency & Master Data",
  maturity: "custom_abap_required",
  summary:
    "Required-field checking depends on object type, country, and process-specific business rules.",
  source: { kind: "derived", objects: ["BP/Customer/Vendor/Material", "custom RFC"] },
  blocker:
    "Requires a data-quality wrapper that packages the approved missing-field rules.",
  wrapper: {
    functionName: "ZHC_GET_DATA_QUALITY_KPIS",
    wrapperKpiId: "missing_mandatory_fields",
  },
};

const customDuplicateEntries: NonExecutableKpiDefinition = {
  id: "duplicate_entries",
  title: "Duplicate Entries",
  category: "Data Consistency & Master Data",
  maturity: "custom_abap_required",
  summary:
    "Duplicate detection requires approved matching rules and should not be rebuilt in MCP calls.",
  source: { kind: "derived", objects: ["BUT000/KNA1/LFA1/MARA", "custom RFC"] },
  blocker:
    "Requires a data-quality wrapper that encodes the duplicate-detection rules.",
  wrapper: {
    functionName: "ZHC_GET_DATA_QUALITY_KPIS",
    wrapperKpiId: "duplicate_entries",
  },
};

const customCviInconsistencies: NonExecutableKpiDefinition = {
  id: "cvi_bp_inconsistencies",
  title: "CVI/BP Inconsistencies",
  category: "Data Consistency & Master Data",
  maturity: "custom_abap_required",
  summary:
    "CVI consistency should be packaged once in SAP rather than implemented as repeated cross-table reads.",
  source: { kind: "derived", objects: ["CVI_*", "BUT000", "custom RFC"] },
  blocker: "Requires a CVI-aware data-quality wrapper.",
  wrapper: {
    functionName: "ZHC_GET_DATA_QUALITY_KPIS",
    wrapperKpiId: "cvi_bp_inconsistencies",
  },
};

const customStuckSalesDocuments: NonExecutableKpiDefinition = {
  id: "stuck_sales_documents",
  title: "Stuck Sales Documents",
  category: "Data Consistency & Master Data",
  maturity: "custom_abap_required",
  summary:
    "Stuck document logic needs status plus aging rules and should be standardized in SAP.",
  source: { kind: "derived", objects: ["VBUK", "VBAK", "custom RFC"] },
  blocker: "Requires an OTC wrapper with agreed stuck-document rules.",
  wrapper: {
    functionName: "ZHC_GET_OTC_KPIS",
    wrapperKpiId: "stuck_sales_documents",
  },
};

const customStuckDeliveryDocuments: NonExecutableKpiDefinition = {
  id: "stuck_delivery_documents",
  title: "Stuck Delivery Documents",
  category: "Data Consistency & Master Data",
  maturity: "custom_abap_required",
  summary:
    "Delivery blockage requires combined status and aging interpretation.",
  source: { kind: "derived", objects: ["LIKP", "VBUK", "custom RFC"] },
  blocker: "Requires an OTC or logistics wrapper with agreed delivery-aging rules.",
  wrapper: {
    functionName: "ZHC_GET_OTC_KPIS",
    wrapperKpiId: "stuck_delivery_documents",
  },
};

const customGrIrMismatch: NonExecutableKpiDefinition = {
  id: "gr_ir_mismatch",
  title: "GR/IR Mismatch",
  category: "Data Consistency & Master Data",
  maturity: "custom_abap_required",
  summary:
    "GR/IR is a reconciliation KPI and should be packaged in SAP with the approved matching logic.",
  source: { kind: "derived", objects: ["EKBE", "RBKP", "custom RFC"] },
  blocker: "Requires a P2P or finance wrapper for reconciliation exceptions.",
  wrapper: {
    functionName: "ZHC_GET_P2P_KPIS",
    wrapperKpiId: "gr_ir_mismatch",
  },
};

const customFailedSalesOrders: NonExecutableKpiDefinition = {
  id: "failed_sales_orders",
  title: "Failed Sales Orders",
  category: "Business Process KPIs",
  maturity: "custom_abap_required",
  summary:
    "Failed-order logic is business-rule heavy and should be packaged behind an OTC wrapper.",
  source: { kind: "derived", objects: ["VBAK", "VBUK", "custom RFC"] },
  blocker: "Requires an OTC wrapper with the approved failure criteria.",
  wrapper: {
    functionName: "ZHC_GET_OTC_KPIS",
    wrapperKpiId: "failed_sales_orders",
  },
};

const customAtpCheckFailures: NonExecutableKpiDefinition = {
  id: "atp_check_failures",
  title: "ATP Check Failures",
  category: "Business Process KPIs",
  maturity: "custom_abap_required",
  summary:
    "ATP failure semantics vary by process and should be packaged once in SAP.",
  source: { kind: "derived", objects: ["VBEP", "custom RFC"] },
  blocker: "Requires an OTC wrapper with the agreed ATP-failure logic.",
  wrapper: {
    functionName: "ZHC_GET_OTC_KPIS",
    wrapperKpiId: "atp_check_failures",
  },
};

const customPoCreationErrors: NonExecutableKpiDefinition = {
  id: "po_creation_errors",
  title: "PO Creation Errors",
  category: "Business Process KPIs",
  maturity: "custom_abap_required",
  summary:
    "PO-creation failure is workflow and process specific and should be packaged in SAP.",
  source: { kind: "derived", objects: ["EKKO", "workflow", "custom RFC"] },
  blocker: "Requires a P2P wrapper with the approved PO-error rules.",
  wrapper: {
    functionName: "ZHC_GET_P2P_KPIS",
    wrapperKpiId: "po_creation_errors",
  },
};

const customInvoiceMatchFailures: NonExecutableKpiDefinition = {
  id: "invoice_match_failures",
  title: "Invoice Match Failures",
  category: "Business Process KPIs",
  maturity: "custom_abap_required",
  summary:
    "3-way match exceptions require finance-approved business logic, not a simple raw-document filter.",
  source: { kind: "derived", objects: ["RBKP", "EKBE", "custom RFC"] },
  blocker: "Requires a P2P wrapper with the approved invoice-match logic.",
  wrapper: {
    functionName: "ZHC_GET_P2P_KPIS",
    wrapperKpiId: "invoice_match_failures",
  },
};

const customPaymentRunErrors: NonExecutableKpiDefinition = {
  id: "payment_run_errors",
  title: "Payment Run Errors",
  category: "Business Process KPIs",
  maturity: "custom_abap_required",
  summary:
    "Payment-run failure semantics are process-specific and should be packaged in SAP.",
  source: { kind: "derived", objects: ["REGUH", "REGUP", "custom RFC"] },
  blocker: "Requires a finance wrapper with the approved payment-run error logic.",
  wrapper: {
    functionName: "ZHC_GET_FINANCE_KPIS",
    wrapperKpiId: "payment_run_errors",
  },
};

const customAssetInconsistencies: NonExecutableKpiDefinition = {
  id: "asset_inconsistencies",
  title: "Asset Inconsistencies",
  category: "Business Process KPIs",
  maturity: "custom_abap_required",
  summary:
    "Asset reconciliation needs packaged integrity rules across master and value tables.",
  source: { kind: "derived", objects: ["ANLA", "ANLC", "custom RFC"] },
  blocker: "Requires a finance wrapper for asset integrity checks.",
  wrapper: {
    functionName: "ZHC_GET_FINANCE_KPIS",
    wrapperKpiId: "asset_inconsistencies",
  },
};

const customReconciliationImbalanceAlerts: NonExecutableKpiDefinition = {
  id: "reconciliation_imbalance_alerts",
  title: "Reconciliation Imbalance Alerts",
  category: "Business Process KPIs",
  maturity: "custom_abap_required",
  summary:
    "Financial imbalance alerts require control logic and should be produced by a finance wrapper.",
  source: { kind: "derived", objects: ["FAGLFLEXT", "custom RFC"] },
  blocker: "Requires a finance wrapper for GL imbalance detection.",
  wrapper: {
    functionName: "ZHC_GET_FINANCE_KPIS",
    wrapperKpiId: "reconciliation_imbalance_alerts",
  },
};

const excludedServiceNow: NonExecutableKpiDefinition = {
  id: "servicenow_ticket_volume",
  title: "Total Ticket Volume",
  category: "Incident & Support KPIs",
  maturity: "excluded",
  summary: "ServiceNow KPIs are outside SAP MCP scope.",
  source: { kind: "rfc", objects: ["ServiceNow REST"] },
  blocker: "Build a separate ServiceNow connector or MCP server.",
};

const averageSystemRestartFrequency: ExecutableKpiDefinition = {
  id: "average_system_restart_frequency",
  title: "Average System Restart Frequency",
  category: "System Connectivity & Availability",
  maturity: "implemented",
  summary: "Restart-event frequency inferred from system log messages.",
  source: { kind: "rfc", objects: ["RSLG_GET_MESSAGES"] },
  notes: [
    "This implementation matches restart-like messages in the returned SM21 payload. Override the 'restart_terms' dimension if your system uses different wording.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const result = await helpers.callFunction("RSLG_GET_MESSAGES", {});
    const records = filterRecordsToWindow(
      collectRecords(result, ["MESSAGES", "ET_MESSAGES", "SYSTEM_LOG", "LOG"]),
      window,
    );
    const restartTerms = (
      input.dimensions?.restart_terms ??
      "RESTART,STARTUP,STARTED,SHUTDOWN,STOPPED,INSTANCE START,SERVER START"
    )
      .split(",")
      .map((value) => value.trim().toUpperCase())
      .filter((value) => value.length > 0);
    const matchedEvents = records.filter((record) =>
      recordContainsTerms(record, restartTerms),
    ).length;
    const daySpan = Math.max(
      (new Date(window.to).getTime() - new Date(window.from).getTime()) /
        86_400_000,
      1 / 24,
    );

    return countResult(
      this,
      roundValue(matchedEvents / daySpan),
      [
        `Matched restart events: ${matchedEvents}.`,
        `Terms used: ${restartTerms.join(", ")}.`,
        ...(this.notes ?? []),
      ],
      window,
    );
  },
};

const licenseUtilizationPct: ExecutableKpiDefinition = {
  id: "license_utilization_pct",
  title: "License Utilization %",
  category: "System Connectivity & Availability",
  unit: "percent",
  maturity: "implemented",
  summary: "Licensed-user utilization derived from SAP licensing data.",
  source: { kind: "rfc", objects: ["SLIC_GET_INSTALLATIONS"] },
  notes: [
    "The KPI first looks for direct utilization percentages, then falls back to used-vs-licensed user counts when both are exposed by SLIC_GET_INSTALLATIONS.",
  ],
  async execute(helpers) {
    const result = await helpers.callFunction("SLIC_GET_INSTALLATIONS", {});
    const directPercentages = collectMetricValuesFromRecords(result, {
      preferredKeys: ["INSTALLATIONS", "ET_INSTALLATIONS", "LICENSES"],
      exactKeys: [
        "UTILIZATION_PCT",
        "UTILIZATION_PERCENT",
        "LICENSE_UTILIZATION_PCT",
        "LICENSE_PERCENT",
      ],
      regexes: [/(LICENSE|UTILI).*(PCT|PERCENT)/],
    }).filter((value) => value >= 0 && value <= 100);

    if (directPercentages.length > 0) {
      return countResult(
        this,
        roundValue(average(directPercentages)),
        [
          `Utilization records averaged: ${directPercentages.length}.`,
          ...(this.notes ?? []),
        ],
      );
    }

    const usedValues = collectMetricValuesFromRecords(result, {
      preferredKeys: ["INSTALLATIONS", "ET_INSTALLATIONS", "LICENSES"],
      exactKeys: [
        "USED_USERS",
        "CURRENT_USERS",
        "ACTIVE_USERS",
        "MEASURED_USERS",
        "CONSUMED_USERS",
      ],
      regexes: [/(USED|ACTIVE|MEASURED|CONSUMED).*(USER|LICENSE)/],
    });
    const licensedValues = collectMetricValuesFromRecords(result, {
      preferredKeys: ["INSTALLATIONS", "ET_INSTALLATIONS", "LICENSES"],
      exactKeys: [
        "LICENSED_USERS",
        "TOTAL_LICENSES",
        "MAX_USERS",
        "LICENSE_CAPACITY",
      ],
      regexes: [/(LICENSED|TOTAL|MAX|CAPACITY).*(USER|LICENSE)/],
    }).filter((value) => value > 0);

    if (usedValues.length === 0 || licensedValues.length === 0) {
      throw new Error(
        "SLIC_GET_INSTALLATIONS did not expose recognizable license utilization fields.",
      );
    }

    const used = usedValues.reduce((sum, value) => sum + value, 0);
    const licensed = licensedValues.reduce((sum, value) => sum + value, 0);

    return countResult(
      this,
      roundValue((used / licensed) * 100),
      [
        `Used users/licenses observed: ${used}.`,
        `Licensed capacity observed: ${licensed}.`,
        ...(this.notes ?? []),
      ],
    );
  },
};

const updateTaskResponseTime: ExecutableKpiDefinition = {
  id: "update_task_response_time",
  title: "Update Task Response Time",
  category: "System Performance",
  unit: "ms",
  maturity: "implemented",
  summary: "Update-task response time derived from SAP workload aggregates.",
  source: { kind: "rfc", objects: ["SWNC_COLLECTOR_GET_AGGREGATES"] },
  notes: [
    "This reads SWNC workload aggregates instead of raw VBHDR queue rows because SWNC gives a cleaner response-time surface when available.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const result = await helpers.callFunction(
      "SWNC_COLLECTOR_GET_AGGREGATES",
      buildSwncParameters(window, input),
    );
    const values = collectNumericValues(result, {
      preferredKeys: ["ASTAT", "ASHITL_RESPTIME", "HITLIST_RESPTIME"],
      exactKeys: [
        "AVG_UPD_RESPTI",
        "AVGUPDRESPTI",
        "UPDATE_RESPTI",
        "UPDATE_RESPONSE_TIME",
        "AVG_UPDATE_RESPONSE_TIME",
      ],
      regexes: [/(UPD|UPDATE).*(RESP|RESPONSE)/],
    }).filter((value) => value > 0);

    if (values.length === 0) {
      throw new Error(
        "SWNC response did not expose a recognizable update-task response-time metric.",
      );
    }

    return countResult(
      this,
      roundValue(average(values)),
      this.notes ?? [],
      window,
    );
  },
};

const cpuUtilizationPct: ExecutableKpiDefinition = {
  id: "cpu_utilization_pct",
  title: "CPU Utilization %",
  category: "System Performance",
  unit: "percent",
  maturity: "implemented",
  summary: "Application-server CPU utilization from SAP monitoring.",
  source: { kind: "rfc", objects: ["BAPI_SYSTEM_MON_GETSYSINFO"] },
  notes: [
    "Direct CPU utilization percentages are preferred. If the RFC only exposes idle or used-vs-total counters, the KPI derives utilization from those values.",
  ],
  async execute(helpers) {
    const result = await helpers.callFunction("BAPI_SYSTEM_MON_GETSYSINFO", {});
    const directPercentages = collectMetricValuesFromRecords(result, {
      preferredKeys: ["SYSTEM_INFO", "SERVERS", "INSTANCES", "ET_SYSINFO"],
      exactKeys: [
        "CPU_UTILIZATION",
        "CPU_USAGE_PCT",
        "CPU_PERCENT",
        "CPU_PCT",
      ],
      regexes: [/CPU.*(UTIL|USAGE|PCT|PERCENT)/, /(UTIL|USAGE).*(CPU)/],
    }).filter((value) => value >= 0 && value <= 100);

    if (directPercentages.length > 0) {
      return countResult(
        this,
        roundValue(average(directPercentages)),
        [
          `CPU records averaged: ${directPercentages.length}.`,
          ...(this.notes ?? []),
        ],
      );
    }

    const idlePercentages = collectMetricValuesFromRecords(result, {
      preferredKeys: ["SYSTEM_INFO", "SERVERS", "INSTANCES", "ET_SYSINFO"],
      exactKeys: ["CPU_IDLE_PCT", "CPU_IDLE_PERCENT"],
      regexes: [/CPU.*IDLE/],
    }).filter((value) => value >= 0 && value <= 100);

    if (idlePercentages.length > 0) {
      return countResult(
        this,
        roundValue(100 - average(idlePercentages)),
        [
          `Derived from idle percentages across ${idlePercentages.length} records.`,
          ...(this.notes ?? []),
        ],
      );
    }

    throw new Error(
      "BAPI_SYSTEM_MON_GETSYSINFO did not expose a recognizable CPU utilization metric.",
    );
  },
};

const memoryUtilizationPct: ExecutableKpiDefinition = {
  id: "memory_utilization_pct",
  title: "Memory Utilization %",
  category: "System Performance",
  unit: "percent",
  maturity: "implemented",
  summary: "Application-layer memory utilization from SAP monitoring.",
  source: { kind: "rfc", objects: ["BAPI_SYSTEM_MON_GETSYSINFO"] },
  notes: [
    "Direct memory-utilization percentages are preferred. Otherwise the KPI derives utilization from used-vs-total or free-vs-total memory values.",
  ],
  async execute(helpers) {
    const result = await helpers.callFunction("BAPI_SYSTEM_MON_GETSYSINFO", {});
    const directPercentages = collectMetricValuesFromRecords(result, {
      preferredKeys: ["SYSTEM_INFO", "SERVERS", "INSTANCES", "ET_SYSINFO"],
      exactKeys: [
        "MEM_UTILIZATION",
        "MEMORY_UTILIZATION",
        "MEM_USAGE_PCT",
        "MEMORY_PCT",
      ],
      regexes: [/(MEM|MEMORY).*(UTIL|USAGE|PCT|PERCENT)/],
    }).filter((value) => value >= 0 && value <= 100);

    if (directPercentages.length > 0) {
      return countResult(
        this,
        roundValue(average(directPercentages)),
        [
          `Memory records averaged: ${directPercentages.length}.`,
          ...(this.notes ?? []),
        ],
      );
    }

    const usedValues = collectMetricValuesFromRecords(result, {
      preferredKeys: ["SYSTEM_INFO", "SERVERS", "INSTANCES", "ET_SYSINFO"],
      exactKeys: ["MEM_USED", "MEMORY_USED", "USED_MEMORY"],
      regexes: [/(USED).*(MEM|MEMORY)/],
    });
    const totalValues = collectMetricValuesFromRecords(result, {
      preferredKeys: ["SYSTEM_INFO", "SERVERS", "INSTANCES", "ET_SYSINFO"],
      exactKeys: ["MEM_TOTAL", "MEMORY_TOTAL", "TOTAL_MEMORY"],
      regexes: [/(TOTAL).*(MEM|MEMORY)/],
    }).filter((value) => value > 0);

    if (usedValues.length > 0 && totalValues.length > 0) {
      const used = usedValues.reduce((sum, value) => sum + value, 0);
      const total = totalValues.reduce((sum, value) => sum + value, 0);

      return countResult(
        this,
        roundValue((used / total) * 100),
        [
          `Derived from used/total memory across ${totalValues.length} records.`,
          ...(this.notes ?? []),
        ],
      );
    }

    const freeValues = collectMetricValuesFromRecords(result, {
      preferredKeys: ["SYSTEM_INFO", "SERVERS", "INSTANCES", "ET_SYSINFO"],
      exactKeys: ["MEM_FREE", "MEMORY_FREE", "FREE_MEMORY"],
      regexes: [/(FREE).*(MEM|MEMORY)/],
    }).filter((value) => value >= 0);

    if (freeValues.length > 0 && totalValues.length > 0) {
      const free = freeValues.reduce((sum, value) => sum + value, 0);
      const total = totalValues.reduce((sum, value) => sum + value, 0);

      return countResult(
        this,
        roundValue(((total - free) / total) * 100),
        [
          `Derived from free/total memory across ${totalValues.length} records.`,
          ...(this.notes ?? []),
        ],
      );
    }

    throw new Error(
      "BAPI_SYSTEM_MON_GETSYSINFO did not expose a recognizable memory utilization metric.",
    );
  },
};

const systemLogErrors: ExecutableKpiDefinition = {
  id: "system_log_errors",
  title: "System Log Errors (SM21)",
  category: "System Performance",
  unit: "count",
  maturity: "implemented",
  summary: "System log error count from SM21 payloads.",
  source: { kind: "rfc", objects: ["RSLG_GET_MESSAGES"] },
  notes: [
    "This counts records whose severity looks error-like or whose text contains error terms. Override the 'system_log_terms' dimension if your landscape uses different wording.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const result = await helpers.callFunction("RSLG_GET_MESSAGES", {});
    const records = filterRecordsToWindow(
      collectRecords(result, ["MESSAGES", "ET_MESSAGES", "SYSTEM_LOG", "LOG"]),
      window,
    );
    const errorTerms = (
      input.dimensions?.system_log_terms ??
      "ERROR,ABORT,FAILED,FAILURE,CRITICAL,SEVERE,EMERGENCY"
    )
      .split(",")
      .map((value) => value.trim().toUpperCase())
      .filter((value) => value.length > 0);
    const errorCount = records.filter((record) => {
      const hasErrorSeverity = getObjectEntries(record).some(([key, entry]) => {
        const normalizedKey = normalizeRecordKey(key);
        const severity = String(entry ?? "").trim().toUpperCase();

        return (
          /(TYPE|SEVERITY|LEVEL|MSGTY|MSGTYPE)/.test(normalizedKey) &&
          ["A", "E", "X", "C", "ERROR", "CRITICAL", "ABORT"].includes(severity)
        );
      });

      return hasErrorSeverity || recordContainsTerms(record, errorTerms);
    }).length;

    return countResult(
      this,
      errorCount,
      [
        `Error terms used: ${errorTerms.join(", ")}.`,
        ...(this.notes ?? []),
      ],
      window,
    );
  },
};

const gatewayErrors: ExecutableKpiDefinition = {
  id: "gateway_errors",
  title: "Gateway Errors",
  category: "System Performance",
  unit: "count",
  maturity: "implemented",
  summary: "Gateway-side communication errors from SAP gateway or ICM monitoring.",
  source: { kind: "rfc", objects: ["GW_GET_STATISTIC", "ICM_GET_MONITOR_INFO"] },
  notes: [
    "The KPI prefers GW_GET_STATISTIC and falls back to ICM_GET_MONITOR_INFO if gateway statistics are not available.",
  ],
  async execute(helpers) {
    let result: Record<string, unknown>;
    let sourceName = "GW_GET_STATISTIC";

    try {
      result = await helpers.callFunction("GW_GET_STATISTIC", {});
    } catch {
      sourceName = "ICM_GET_MONITOR_INFO";
      result = await helpers.callFunction("ICM_GET_MONITOR_INFO", {});
    }

    const numericValues = collectMetricValuesFromRecords(result, {
      preferredKeys: ["STATISTIC", "STATISTICS", "GATEWAY", "SERVICES"],
      exactKeys: [
        "ERRORS",
        "ERRCOUNT",
        "ERRCNT",
        "ERR_COUNT",
        "FAILED_CONNECTIONS",
        "CONNECTION_ERRORS",
        "GW_ERRORS",
      ],
      regexes: [/(GW|GATEWAY|CONN).*(ERR|FAIL)/, /(ERR|FAIL).*(GW|GATEWAY|CONN)/],
    });
    const value =
      numericValues.length > 0
        ? Math.round(numericValues.reduce((sum, current) => sum + current, 0))
        : collectRecords(result).filter((record) =>
            recordContainsTerms(record, ["ERROR", "FAIL", "REJECT", "RESET"]),
          ).length;

    return countResult(
      this,
      value,
      [`Monitoring source used: ${sourceName}.`, ...(this.notes ?? [])],
    );
  },
};

const lockTableOverflows: ExecutableKpiDefinition = {
  id: "lock_table_overflows",
  title: "Lock Table Overflows",
  category: "System Performance",
  unit: "count",
  maturity: "implemented",
  summary: "Enqueue overflow count from ENQUEUE_STATISTICS.",
  source: { kind: "rfc", objects: ["ENQUEUE_STATISTICS"] },
  notes: [
    "If ENQUEUE_STATISTICS exposes multiple rows, the KPI sums the best overflow-like counter from each row.",
  ],
  async execute(helpers) {
    const result = await helpers.callFunction("ENQUEUE_STATISTICS", {});
    const numericValues = collectMetricValuesFromRecords(result, {
      preferredKeys: ["STATISTIC", "STATISTICS", "LOCKS", "ENQUEUE"],
      exactKeys: [
        "OVERFLOWS",
        "OVERFLOW_CNT",
        "OVERFLOW_COUNT",
        "LOCK_TABLE_OVERFLOWS",
        "ENQUEUE_OVERFLOWS",
      ],
      regexes: [/OVERFLOW/, /OVFL/],
    });
    const value =
      numericValues.length > 0
        ? Math.round(numericValues.reduce((sum, current) => sum + current, 0))
        : collectRecords(result).filter((record) =>
            recordContainsTerms(record, ["OVERFLOW", "LOCK TABLE FULL"]),
          ).length;

    return countResult(this, value, this.notes ?? []);
  },
};

const failedApiCalls: ExecutableKpiDefinition = {
  id: "failed_api_calls",
  title: "Failed API Calls",
  category: "Integration & Interfaces",
  unit: "count",
  maturity: "implemented",
  summary: "Failed HTTP or API calls from SAP ICM monitoring.",
  source: { kind: "rfc", objects: ["ICM_GET_MONITOR_INFO"] },
  notes: [
    "The KPI sums the best failure-like counter per ICM monitoring record to avoid double-counting multiple error fields from one row.",
  ],
  async execute(helpers) {
    const result = await helpers.callFunction("ICM_GET_MONITOR_INFO", {});
    const numericValues = collectMetricValuesFromRecords(result, {
      preferredKeys: ["SERVICES", "STATISTIC", "STATISTICS", "CLIENTS", "HTTP"],
      exactKeys: [
        "FAILED_REQUESTS",
        "FAILED_CALLS",
        "HTTP_4XX",
        "HTTP_5XX",
        "ERRORS",
        "ERRCOUNT",
        "REJECTED_REQUESTS",
      ],
      regexes: [
        /(HTTP|API|REST).*(FAIL|ERR|REJECT|4XX|5XX)/,
        /(FAIL|ERR|REJECT|4XX|5XX).*(HTTP|API|REST)/,
      ],
    });
    const value =
      numericValues.length > 0
        ? Math.round(numericValues.reduce((sum, current) => sum + current, 0))
        : collectRecords(result).filter((record) =>
            recordContainsTerms(record, ["ERROR", "FAILED", "REJECT", "HTTP 5", "HTTP 4"]),
          ).length;

    return countResult(this, value, this.notes ?? []);
  },
};

const apiResponseTime: ExecutableKpiDefinition = {
  id: "api_response_time",
  title: "API Response Time",
  category: "Integration & Interfaces",
  unit: "ms",
  maturity: "implemented",
  summary: "API response time from SAP HTTP or workload monitoring.",
  source: { kind: "rfc", objects: ["SWNC_COLLECTOR_GET_AGGREGATES", "ICM_GET_MONITOR_INFO"] },
  notes: [
    "The KPI prefers ICM HTTP response metrics and falls back to SWNC response-time fields when the ICM payload does not expose a dedicated API metric.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);

    try {
      const icmResult = await helpers.callFunction("ICM_GET_MONITOR_INFO", {});
      const icmValues = collectNumericValues(icmResult, {
        preferredKeys: ["SERVICES", "STATISTIC", "STATISTICS", "HTTP"],
        exactKeys: [
          "AVG_RESPONSE_TIME",
          "AVERAGE_RESPONSE_TIME",
          "API_RESPONSE_TIME",
          "HTTP_RESPONSE_TIME",
          "RESP_TIME_MS",
        ],
        regexes: [
          /(API|HTTP).*(RESP|RESPONSE)/,
          /(RESP|RESPONSE).*(TIME|TI)/,
        ],
      }).filter((value) => value > 0);

      if (icmValues.length > 0) {
        return countResult(
          this,
          roundValue(average(icmValues)),
          ["Monitoring source used: ICM_GET_MONITOR_INFO.", ...(this.notes ?? [])],
          window,
        );
      }
    } catch {
      // Fall through to SWNC below.
    }

    const swncResult = await helpers.callFunction(
      "SWNC_COLLECTOR_GET_AGGREGATES",
      buildSwncParameters(window, input),
    );
    const swncValues = collectNumericValues(swncResult, {
      preferredKeys: ["ASTAT", "EXTSYSTEM", "FRONTEND", "ASHITL_RESPTIME"],
      exactKeys: [
        "AVG_RESPONSE_TIME",
        "AVERAGE_RESPONSE_TIME",
        "AVG_RESPTI",
        "RESPTI",
      ],
      regexes: [/(RESP|RESPONSE).*(TIME|TI)/],
    }).filter((value) => value > 0);

    if (swncValues.length === 0) {
      throw new Error(
        "Neither ICM nor SWNC exposed a recognizable API response-time metric.",
      );
    }

    return countResult(
      this,
      roundValue(average(swncValues)),
      ["Monitoring source used: SWNC_COLLECTOR_GET_AGGREGATES.", ...(this.notes ?? [])],
      window,
    );
  },
};

const replicationDelays: ExecutableKpiDefinition = {
  id: "replication_delays",
  title: "Replication Delays",
  category: "Data Consistency & Master Data",
  unit: "seconds",
  maturity: "implemented",
  summary: "Replication lag derived from the newest readable IUUC_REPL_CONTENT timestamp.",
  source: { kind: "table", objects: ["IUUC_REPL_CONTENT"] },
  notes: [
    "This is a snapshot lag. Scope it with the 'replication_tables' dimension if you only care about a subset of replicated objects.",
  ],
  async execute(helpers, input) {
    const scopedTables = new Set(
      (input.dimensions?.replication_tables ?? "")
        .split(",")
        .map((value) => value.trim().toUpperCase())
        .filter((value) => value.length > 0),
    );
    const { rows, fields } = await scanRowsWithFieldFallbacks(helpers, {
      table: "IUUC_REPL_CONTENT",
      candidateFields: [
        ["TABNAME", "LUPD_DATE", "LUPD_TIME", "CREATEDATE", "CREATETIME"],
        ["TABNAME", "UPDDATE", "UPDTIME", "CREATEDATE", "CREATETIME"],
        ["TABNAME", "UDATE", "UTIME", "AEDAT", "AEZET"],
        ["TABNAME", "DATE", "TIME", "TIMESTAMP"],
        ["TABNAME", "TIMESTAMP"],
      ],
      pageSize: 500,
      scanCap: 20000,
    });
    const scopedRows = rows.filter((row) => {
      const tableName = (row.TABNAME ?? "").trim().toUpperCase();
      return scopedTables.size === 0 || scopedTables.has(tableName);
    });
    const timestamps = scopedRows
      .map((row) =>
        extractRecordDateTime(row as unknown as Record<string, unknown>),
      )
      .filter((value): value is Date => value !== undefined);

    if (timestamps.length === 0) {
      throw new Error(
        `IUUC_REPL_CONTENT rows did not expose a recognizable replication timestamp. Tried fields: ${fields.join(", ")}.`,
      );
    }

    const newestTimestamp = timestamps.reduce((latest, current) =>
      current.getTime() > latest.getTime() ? current : latest,
    );
    const lagSeconds = Math.max(
      0,
      Math.round((Date.now() - newestTimestamp.getTime()) / 1000),
    );

    return countResult(
      this,
      lagSeconds,
      [
        `Rows inspected: ${scopedRows.length}.`,
        `Field set used: ${fields.join(", ")}.`,
        ...(this.notes ?? []),
      ],
    );
  },
};

const additionalPlannedDefinitions: NonExecutableKpiDefinition[] = [];

const additionalWrapperDefinitions: NonExecutableKpiDefinition[] = [
  wrapperDefinition({
    id: "job_restart_success_rate",
    title: "Job Restart Success Rate",
    category: "Job & Batch Monitoring",
    unit: "percent",
    summary: "Restart success rate should be packaged in SAP job logic.",
    source: { kind: "derived", objects: ["TBTCO", "custom RFC"] },
    blocker: "Requires a job wrapper for restart-correlation logic.",
    functionName: "ZHC_GET_JOB_KPIS",
  }),
  wrapperDefinition({
    id: "data_migration_reconciliation_errors",
    title: "Data Migration Reconciliation Errors",
    category: "Data Consistency & Master Data",
    summary: "Migration reconciliation must be sourced from the retained migration-control process.",
    source: { kind: "derived", objects: ["Migration cockpit", "custom RFC"] },
    blocker: "Requires a data-quality wrapper with migration-source awareness.",
    functionName: "ZHC_GET_DATA_QUALITY_KPIS",
  }),
  wrapperDefinition({
    id: "gr_posting_failures",
    title: "GR Posting Failures",
    category: "Business Process KPIs",
    summary: "Goods-receipt posting failure must be packaged with the approved movement logic.",
    source: { kind: "derived", objects: ["MKPF", "MSEG", "custom RFC"] },
    blocker: "Requires a P2P wrapper with GR-failure rules.",
    functionName: "ZHC_GET_P2P_KPIS",
  }),
  wrapperDefinition({
    id: "stuck_production_orders",
    title: "Stuck Production Orders",
    category: "Business Process KPIs",
    summary: "Production-order blockage requires status-plus-aging rules.",
    source: { kind: "derived", objects: ["AUFK", "AFKO", "custom RFC"] },
    blocker: "Requires a manufacturing wrapper.",
    functionName: "ZHC_GET_MANUFACTURING_KPIS",
  }),
  wrapperDefinition({
    id: "backflush_failures",
    title: "Backflush Failures",
    category: "Business Process KPIs",
    summary: "Backflush failure semantics should be packaged with manufacturing context.",
    source: { kind: "derived", objects: ["AFRU", "custom RFC"] },
    blocker: "Requires a manufacturing wrapper.",
    functionName: "ZHC_GET_MANUFACTURING_KPIS",
  }),
  wrapperDefinition({
    id: "service_calls",
    title: "Service Calls",
    category: "Business Process KPIs",
    summary: "Service-call counting depends on object-type scoping and process definition.",
    source: { kind: "derived", objects: ["QMEL", "custom RFC"] },
    blocker: "Requires a service-management wrapper.",
    functionName: "ZHC_GET_SERVICE_KPIS",
  }),
  wrapperDefinition({
    id: "techs_dispatched",
    title: "Techs Dispatched",
    category: "Business Process KPIs",
    summary: "Dispatch events require assignment logic, not a flat count.",
    source: { kind: "derived", objects: ["Dispatch objects", "custom RFC"] },
    blocker: "Requires a service-management wrapper.",
    functionName: "ZHC_GET_SERVICE_KPIS",
  }),
  wrapperDefinition({
    id: "parts_consumed",
    title: "Parts Consumed",
    category: "Business Process KPIs",
    summary: "Parts consumed must be linked to the service process definition.",
    source: { kind: "derived", objects: ["Service/material movements", "custom RFC"] },
    blocker: "Requires a service-management wrapper.",
    functionName: "ZHC_GET_SERVICE_KPIS",
  }),
  wrapperDefinition({
    id: "tax_reports",
    title: "Tax Reports",
    category: "Business Process KPIs",
    summary: "Tax report volume is localization-specific and should be packaged in SAP.",
    source: { kind: "derived", objects: ["Tax process", "custom RFC"] },
    blocker: "Requires a tax wrapper.",
    functionName: "ZHC_GET_TAX_KPIS",
  }),
  wrapperDefinition({
    id: "vat_corrections",
    title: "VAT Corrections",
    category: "Business Process KPIs",
    summary: "VAT correction logic is localization-specific and should be packaged in SAP.",
    source: { kind: "derived", objects: ["Tax process", "custom RFC"] },
    blocker: "Requires a tax wrapper.",
    functionName: "ZHC_GET_TAX_KPIS",
  }),
  wrapperDefinition({
    id: "audit_files",
    title: "Audit Files",
    category: "Business Process KPIs",
    summary: "Audit-file generation is a reporting process and should be packaged in SAP.",
    source: { kind: "derived", objects: ["Compliance process", "custom RFC"] },
    blocker: "Requires a tax or compliance wrapper.",
    functionName: "ZHC_GET_TAX_KPIS",
  }),
  wrapperDefinition({
    id: "equip_installed",
    title: "Equip. Installed",
    category: "Business Process KPIs",
    summary: "Installed-equipment counting needs a locked business definition before extraction.",
    source: { kind: "derived", objects: ["Equipment/install base", "custom RFC"] },
    blocker: "Requires an asset or service wrapper with an agreed business definition.",
    functionName: "ZHC_GET_EAM_KPIS",
  }),
];

export const KPI_DEFINITIONS: KpiDefinition[] = [
  failedJobCount,
  delayedJobCount,
  activeUserCount,
  workProcessUtilization,
  unauthorizedLoginAttempts,
  failedLoginAttempts,
  abapDumpFrequency,
  backgroundJobThroughput,
  longRunningJobCount,
  jobSuccessRate,
  totalIdocsProcessed,
  idocsInError,
  reprocessingSuccessRate,
  idocBacklogVolume,
  lockedUsers,
  rfcUserPasswordAge,
  inactiveUsers,
  postingErrors,
  unpostedBillingDocuments,
  apInvoices,
  arInvoices,
  glPosted,
  workOrders,
  notifications,
  purchaseOrdersCreated,
  materialsCreated,
  deliveryBlockRate,
  transportRequestBacklog,
  workItemBacklog,
  spoolQueueErrors,
  applicationServerUptimePerInstance,
  batchWindowUtilizationPct,
  numberRangeExhaustionPct,
  peakConcurrentUsers,
  dialogResponseTime,
  timeoutErrors,
  retryAttemptCount,
  queueLockFailures,
  mrpErrors,
  goodsReceipts,
  sapApplicationUptimePct,
  averageSystemRestartFrequency,
  licenseUtilizationPct,
  updateTaskResponseTime,
  cpuUtilizationPct,
  memoryUtilizationPct,
  systemLogErrors,
  gatewayErrors,
  lockTableOverflows,
  failedApiCalls,
  apiResponseTime,
  replicationDelays,
  customAuthorizationFailures,
  customSodConflicts,
  customEmergencyAccessSessions,
  customExpiredPasswordPct,
  customMissingMandatoryFields,
  customDuplicateEntries,
  customCviInconsistencies,
  customStuckSalesDocuments,
  customStuckDeliveryDocuments,
  customGrIrMismatch,
  customFailedSalesOrders,
  customAtpCheckFailures,
  customPoCreationErrors,
  customInvoiceMatchFailures,
  customPaymentRunErrors,
  excludedDbUptime,
  excludedHanaMemory,
  customPeriodEndClose,
  customAssetInconsistencies,
  customReconciliationImbalanceAlerts,
  excludedServiceNow,
  ...additionalPlannedDefinitions,
  ...additionalWrapperDefinitions,
].map((definition) => withResolvedTier(definition));
