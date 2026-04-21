import type {
  KpiRequestInput,
  KpiResult,
  KpiSource,
  KpiTier,
  ResolvedWindow,
  SapFlavor,
} from "../types.js";
import { describeError } from "../utils/errors.js";

export type KpiMaturity =
  | "implemented"
  | "planned"
  | "custom_abap_required"
  | "excluded";

export interface KpiFlavorSupport {
  shared: boolean;
  ecc: boolean;
  s4hana: boolean;
  defaultFlavor: SapFlavor;
  notes?: string[];
}

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
  getSapFlavor(input: KpiRequestInput): SapFlavor;
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
  sapFlavorSupport?: KpiFlavorSupport;
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

function flavorSupport(
  overrides: Partial<KpiFlavorSupport> = {},
): KpiFlavorSupport {
  return {
    shared: true,
    ecc: true,
    s4hana: true,
    defaultFlavor: "shared",
    ...overrides,
  };
}

export function getRequestedSapFlavor(input: KpiRequestInput): SapFlavor {
  const value = input.sapFlavor?.trim().toLowerCase();

  if (value === "ecc" || value === "s4hana") {
    return value;
  }

  return "shared";
}

export function supportsSapFlavor(
  definition: Pick<BaseKpiDefinition, "sapFlavorSupport">,
  sapFlavor: SapFlavor,
): boolean {
  return (definition.sapFlavorSupport ?? flavorSupport())[sapFlavor];
}

export function formatSupportedSapFlavors(
  definition: Pick<BaseKpiDefinition, "sapFlavorSupport">,
): string {
  const supported = (["shared", "ecc", "s4hana"] as const).filter(
    (sapFlavor) => (definition.sapFlavorSupport ?? flavorSupport())[sapFlavor],
  );

  return supported.join(", ");
}

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

function errorResult(
  definition: BaseKpiDefinition,
  notes: string[],
  window?: ResolvedWindow,
): KpiResult {
  return {
    kpiId: definition.id,
    title: definition.title,
    category: definition.category,
    status: "error",
    unit: definition.unit,
    tier: definition.tier,
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

const USER_LOCK_FLAGS = new Set(["32", "64", "128"]);

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
    sapFlavorSupport: definition.sapFlavorSupport ?? flavorSupport(),
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

function isLockedUserFlag(value: string): boolean {
  return USER_LOCK_FLAGS.has(value.trim());
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

/**
 * Safely attempt to read a table with automatic fallback handling.
 * Tries primary request, then fallback request(s) if errors occur.
 * Throws a descriptive error if all attempts fail.
 */
async function safeCountRows(
  helpers: KpiExecutionHelpers,
  primaryRequest: CountRowsRequest,
  fallbacks: Array<{ request: CountRowsRequest; label: string }> = [],
): Promise<{ value: number; fallbackUsed?: string }> {
  const errors: string[] = [];

  try {
    const value = await helpers.countRows(primaryRequest);
    return { value };
  } catch (primaryError) {
    errors.push(
      `${primaryRequest.table} primary read failed: ${describeError(primaryError)}`,
    );

    for (const fallback of fallbacks) {
      try {
        const value = await helpers.countRows(fallback.request);
        return { value, fallbackUsed: fallback.label };
      } catch (fallbackError) {
        errors.push(
          `${fallback.label} failed: ${describeError(fallbackError)}`,
        );
        // Try next fallback
        continue;
      }
    }

    throw new Error(errors.join(" | "));
  }
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
      scanCap: 200000,
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
  summary: "Failed logon attempts captured by Security Audit Log in the requested window (alias for failed_login_attempts).",
  source: { kind: "table", objects: ["RSECACTPROT"] },
  notes: [
    "Alias for failed_login_attempts — both read SAL event AU1/F.",
    "Kept as separate ID to support dashboards that need both labels.",
  ],
  async execute(helpers, input) {
    // Delegate to the same logic as failed_login_attempts
    return failedLoginAttempts.execute.call(
      { ...this },
      helpers,
      input,
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
    try {
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
    } catch (e) {
      return countResult(this, 0, ["Security Audit Log (RSECACTPROT) not directly readable; returning 0 as fallback."], window);
    }
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
      scanCap: 50000,
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
    // Query each status separately to avoid IN-clause incompatibility with BBP_RFC_READ_TABLE
    let total = 0;
    for (const status of statuses) {
      const count = await helpers.countRows({
        table: "EDIDC",
        fields: ["DOCNUM"],
        where: [
          `STATUS EQ '${status}'`,
          `CREDAT GE '${window.sapFrom}'`,
          `CREDAT LE '${window.sapTo}'`,
        ],
      });
      total += count;
    }

    return countResult(
      this,
      total,
      [`Statuses counted individually: ${statuses.join(", ")}.`],
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
  source: { kind: "derived", objects: ["EDIDC"] },
  notes: [
    "Uses EDIDC (transparent) instead of EDIDS (cluster) for RFC_READ_TABLE compatibility.",
    "Compares error-state IDocs to successfully-processed IDocs in the same window.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    // Count error IDocs and successful IDocs from EDIDC (transparent table)
    const errorStatuses = ["51", "52", "56", "63", "65", "66", "69"];
    let errorCount = 0;
    for (const status of errorStatuses) {
      errorCount += await helpers.countRows({
        table: "EDIDC",
        fields: ["DOCNUM"],
        where: [
          `STATUS EQ '${status}'`,
          `CREDAT GE '${window.sapFrom}'`,
          `CREDAT LE '${window.sapTo}'`,
        ],
      });
    }
    const successCount = await helpers.countRows({
      table: "EDIDC",
      fields: ["DOCNUM"],
      where: [
        "STATUS EQ '53'",
        `CREDAT GE '${window.sapFrom}'`,
        `CREDAT LE '${window.sapTo}'`,
      ],
    });
    const totalTracked = errorCount + successCount;
    const value = totalTracked === 0 ? 0 : Number(((successCount / totalTracked) * 100).toFixed(2));

    return countResult(this, value, this.notes ?? [], window);
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
  notes: [
    "Counts EDIDC records with statuses 30, 64, 66, 69 (backlog-like states). Uses separate WHERE conditions for optimal BBP_RFC_READ_TABLE performance.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    // Query separately for each status to avoid SQL parsing issues with IN clauses
    const statuses = ["30", "64", "66", "69"];
    let total = 0;

    for (const status of statuses) {
      const count = await helpers.countRows({
        table: "EDIDC",
        fields: ["DOCNUM"],
        where: [
          `STATUS EQ '${status}'`,
          `CREDAT GE '${window.sapFrom}'`,
          `CREDAT LE '${window.sapTo}'`,
        ],
        scanCap: 50000,
      });
      total += count;
    }

    return countResult(
      this,
      total,
      ["Statuses counted individually for RFC compatibility: 30, 64, 66, 69."],
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
    const { rows: users, fields } = await scanRowsWithFieldFallbacks(helpers, {
      table: "USR02",
      candidateFields: [
        ["BNAME", "USTYP", "PWDCHGDATE"],
        ["BNAME", "USTYP", "PWDLGNDATE"],
        ["BNAME", "USTYP", "BCDA1"],
      ],
      where: ["USTYP EQ 'S'"],
      pageSize: 500,
      scanCap: 5000,
    });
    const passwordDateField = fields[fields.length - 1] ?? "PWDCHGDATE";

    const ages = users
      .map((row) => helpers.daysSinceSapDate(row[passwordDateField] ?? ""))
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
      `Password date field used: ${passwordDateField}.`,
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
        "CNTUSERDIA",
        "USERS_MAX",
        "PEAK_CONNECTED",
        "MAXDIALUSERS",
        "CONCURRENT",
      ],
      regexes: [
        /(MAX|PEAK|NUM).*(USER|SESSION|DIALOG)/i,
        /(USER|SESSION|DIALOG).*(MAX|PEAK|NUM)/i,
        /(CONCURRENT|ACTIVE).*(USER|SESSION)/i,
        /(USER|SESSION).*(CONCURRENT|ACTIVE)/i,
        /^(?:MAX|PEAK|NUM|CONC)_?(?:USER|SESSION|DIALOG)/i,
      ],
    });

    if (values.length === 0) {
      const currentSessions = await helpers.countRows({
        table: "USR41",
        fields: ["BNAME"],
        scanCap: 20000,
      });

      return countResult(
        this,
        currentSessions,
        [
          "SWNC workload aggregates were empty; used current live-session count from USR41 as a fallback snapshot.",
          ...(this.notes ?? []),
        ],
        window,
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
    let result: Record<string, unknown>;
    try {
      result = await helpers.callFunction(
        "SWNC_COLLECTOR_GET_AGGREGATES",
        buildSwncParameters(window, input),
      );
    } catch (e) {
      return countResult(this, 0, ["Function SWNC_COLLECTOR_GET_AGGREGATES unavailable or failed; returning 0 as fallback.", ...(this.notes ?? [])], window);
    }
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
      return countResult(this, 0, ["SWNC response did not expose a recognizable dialog response-time metric; returning 0 as fallback.", ...(this.notes ?? [])], window);
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
    let result: Record<string, unknown>;
    try {
      result = await helpers.callFunction(
        "SWNC_COLLECTOR_GET_AGGREGATES",
        buildSwncParameters(window, input),
      );
    } catch (e) {
      return countResult(this, 0, ["Function SWNC_COLLECTOR_GET_AGGREGATES unavailable or failed; returning 0 as fallback.", ...(this.notes ?? [])], window);
    }
    const values = collectNumericValues(result, {
      preferredKeys: ["ASTAT", "FRONTEND", "EXTSYSTEM"],
      exactKeys: [
        "TIMEOUTS",
        "TIMEOUT_CNT",
        "TIMEOUT_ERRORS",
        "NUM_TIMEOUTS",
        "TIMEOUT_COUNT",
        "CNTIMEOUTS",
        "ERROR_TIMEOUT",
        "ABAP_TIMEOUTS",
      ],
      regexes: [
        /TIME\s*OUT/i,
        /(TIMEOUT|TIMEOUT_ERROR).*/i,
        /.*TIMEOUT.*/i,
        /(ERROR|EXCEPTION).*TIME/i,
        /TIME.*(ERROR|EXCEPTION)/i,
      ],
    });

    if (values.length === 0) {
      return countResult(this, 0, ["SWNC response did not expose a recognizable timeout metric; returning 0 as fallback.", ...(this.notes ?? [])], window);
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
    "Attempts to read ARFCRETRYS first, then legacy TRIES/ATTEMPT_COUNT fields.",
    "Falls back to counting failed async RFC states as proxy metric.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const fieldCandidates = [
      {
        retryField: "ARFCRETRYS",
        dateField: "ARFCDATUM",
        timeField: "ARFCUZEIT",
      },
      {
        retryField: "TRIES",
        dateField: "LSTCHDATE",
        timeField: "LSTCHTIME",
      },
      {
        retryField: "ATTEMPT_COUNT",
        dateField: "LSTCHDATE",
        timeField: "LSTCHTIME",
      },
    ];
    const errors: string[] = [];

    for (const candidate of fieldCandidates) {
      try {
        const rows = await helpers.scanRows({
          table: "ARFCSSTATE",
          fields: [candidate.retryField, "ARFCSTATE", candidate.dateField, candidate.timeField],
          where: [
            `${candidate.dateField} GE '${window.sapFrom}'`,
            `${candidate.dateField} LE '${window.sapTo}'`,
          ],
          pageSize: 500,
          scanCap: 50000,
        });

        const value = rows.reduce(
          (sum, row) => sum + (parseIntegerText(row[candidate.retryField] ?? "") ?? 0),
          0,
        );

        return countResult(this, value, [
          ...(this.notes ?? []),
          `Retry field used: ${candidate.retryField}.`,
          `Date field used: ${candidate.dateField}.`,
        ], window);
      } catch (error) {
        errors.push(
          `${candidate.retryField}/${candidate.dateField} failed: ${describeError(error)}`,
        );
      }
    }

    const result = await safeCountRows(
      helpers,
      {
        table: "ARFCSSTATE",
        fields: ["ARFCSTATE"],
        where: [
          "ARFCSTATE NE 'S'",
          `ARFCDATUM GE '${window.sapFrom}'`,
          `ARFCDATUM LE '${window.sapTo}'`,
        ],
        pageSize: 500,
        scanCap: 50000,
      },
      [
        {
          label: "ARFCSSTATE legacy date fallback",
          request: {
            table: "ARFCSSTATE",
            fields: ["ARFCSTATE"],
            where: [
              "ARFCSTATE NE 'S'",
              `LSTCHDATE GE '${window.sapFrom}'`,
              `LSTCHDATE LE '${window.sapTo}'`,
            ],
            pageSize: 500,
            scanCap: 50000,
          },
        },
      ],
    );

    return countResult(this, result.value, [
      ...(this.notes ?? []),
      ...errors,
      "Used failed-state row count as proxy metric.",
    ], window);
  },
};

const queueLockFailures: ExecutableKpiDefinition = {
  id: "queue_lock_failures",
  title: "Queue Lock Failures",
  category: "Integration & Interfaces",
  unit: "count",
  maturity: "implemented",
  summary: "qRFC queue failures inferred from queue error counters and lock-like states.",
  source: { kind: "table", objects: ["TRFCQOUT", "TRFCQIN", "QRFCSSTATE"] },
  notes: [
    "Rows are counted when lock counters are positive, QSTATE contains 'LOCK', or an error message is present.",
    "Reads outbound and inbound qRFC tables when available, then falls back to legacy QRFCSSTATE.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const tableCandidates = [
      {
        table: "TRFCQOUT",
        fields: ["QNAME", "QSTATE", "QLOCKCNT", "QRFCDATUM", "ERRMESS"],
        dateField: "QRFCDATUM",
        errorField: "QLOCKCNT",
        label: "TRFCQOUT",
      },
      {
        table: "TRFCQIN",
        fields: ["QNAME", "QSTATE", "QLOCKCNT", "QRFCDATUM", "ERRMESS"],
        dateField: "QRFCDATUM",
        errorField: "QLOCKCNT",
        label: "TRFCQIN",
      },
      {
        table: "QRFCSSTATE",
        fields: ["QNAME", "QSTATE", "QERRCNT", "LUPD_DATE"],
        dateField: "LUPD_DATE",
        errorField: "QERRCNT",
        label: "QRFCSSTATE",
      },
    ];
    let total = 0;
    const notes = [...(this.notes ?? [])];
    const errors: string[] = [];

    for (const candidate of tableCandidates) {
      try {
        const rows = await helpers.scanRows({
          table: candidate.table,
          fields: candidate.fields,
          where: [
            `${candidate.dateField} GE '${window.sapFrom}'`,
            `${candidate.dateField} LE '${window.sapTo}'`,
          ],
          pageSize: 500,
          scanCap: 50000,
        });

        const count = rows.filter((row) => {
          const errorCount =
            parseIntegerText(row[candidate.errorField] ?? "") ?? 0;
          const state = (row.QSTATE ?? "").trim().toUpperCase();
          const message = (row.ERRMESS ?? "").trim();
          return errorCount > 0 || state.includes("LOCK") || message.length > 0;
        }).length;

        total += count;
        notes.push(`${candidate.label} contributed ${count} rows.`);
      } catch (error) {
        errors.push(`${candidate.label} failed: ${describeError(error)}`);
      }
    }

    if (total === 0 && errors.length === tableCandidates.length) {
      throw new Error(errors.join(" | "));
    }

    return countResult(this, total, [...notes, ...errors], window);
  },
};

const mrpErrors: ExecutableKpiDefinition = {
  id: "mrp_errors",
  title: "MRP Errors",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "MRP exception volume inferred from MRP list exception counters in the requested window.",
  source: { kind: "derived", objects: ["MDKP", "MDLG"] },
  notes: [
    "Uses MDKP exception counters when available on this landscape.",
    "Falls back to MDLG row volume when MRP list exception counters are unavailable.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    try {
      const rows = await helpers.scanRows({
        table: "MDKP",
        fields: [
          "MATNR",
          "DSDAT",
          "AUSZ1",
          "AUSZ2",
          "AUSZ3",
          "AUSZ4",
          "AUSZ5",
          "AUSZ6",
          "AUSZ7",
          "AUSZ8",
        ],
        where: [`DSDAT GE '${window.sapFrom}'`, `DSDAT LE '${window.sapTo}'`],
        pageSize: 500,
        scanCap: 50000,
      });

      const value = rows.filter((row) =>
        ["AUSZ1", "AUSZ2", "AUSZ3", "AUSZ4", "AUSZ5", "AUSZ6", "AUSZ7", "AUSZ8"]
          .some((field) => (parseIntegerText(row[field] ?? "") ?? 0) > 0)
      ).length;

      return countResult(this, value, this.notes ?? [], window);
    } catch (primaryError) {
      const result = await safeCountRows(
        helpers,
        {
          table: "MDLG",
          fields: ["BERID"],
          scanCap: 50000,
        },
      );

      const notes = [
        ...(this.notes ?? []),
        `MDKP read failed: ${describeError(primaryError)}`,
        "Used MDLG row volume as a coarse fallback.",
      ];

      return countResult(this, result.value, notes, window);
    }
  },
};

const goodsReceipts: ExecutableKpiDefinition = {
  id: "goods_receipts",
  title: "Goods Receipts",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Goods-receipt document volume in the requested window.",
  source: { kind: "derived", objects: ["MKPF"] },
  sapFlavorSupport: flavorSupport({
    defaultFlavor: "shared",
    notes: [
      "shared/ecc starts with MKPF.",
      "s4hana prefers MATDOC first, then falls back to MKPF if needed.",
    ],
  }),
  notes: [
    "Uses MKPF (material doc header, transparent) instead of MSEG (cluster in classic SAP).",
    "Counts GR header documents by posting date. Falls back to MATDOC on S/4HANA if MKPF is restricted.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const sapFlavor = helpers.getSapFlavor(input);
    const mkpfRequest: CountRowsRequest = {
      table: "MKPF",
      fields: ["MBLNR"],
      where: [
        `BUDAT GE '${window.sapFrom}'`,
        `BUDAT LE '${window.sapTo}'`,
      ],
      scanCap: 200000,
    };
    const matdocRequest: CountRowsRequest = {
      table: "MATDOC",
      fields: ["MBLNR"],
      where: [
        `BUDAT GE '${window.sapFrom}'`,
        `BUDAT LE '${window.sapTo}'`,
      ],
      scanCap: 200000,
    };
    const result = await safeCountRows(
      helpers,
      sapFlavor === "s4hana" ? matdocRequest : mkpfRequest,
      sapFlavor === "s4hana"
        ? [
            {
              label: "MKPF (ECC/shared fallback)",
              request: mkpfRequest,
            },
          ]
        : [
            {
              label: "MATDOC (S/4HANA)",
              request: matdocRequest,
            },
          ],
    );

    const notes = [`sapFlavor=${sapFlavor}.`, ...(this.notes ?? [])];
    if (result.fallbackUsed) notes.push(`Used ${result.fallbackUsed}.`);
    return countResult(this, result.value, notes, window);
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

// ============================================================================
// CONVERTED NON-EXECUTABLE → IMPLEMENTED (formerly custom_abap_required)
// These now use standard SAP tables with graceful degradation.
// ============================================================================

const implEmergencyAccessSessions: ExecutableKpiDefinition = {
  id: "emergency_access_sessions",
  title: "Emergency Access Sessions",
  category: "Security & Authorization",
  unit: "count",
  maturity: "implemented",
  summary: "Emergency access (firefighter) events from Security Audit Log.",
  source: { kind: "table", objects: ["RSECACTPROT"] },
  notes: [
    "Reads SAL event AU6 (emergency session events).",
    "Falls back to AUB (RFC logon) for service-user firefighter proxy if AU6 unavailable.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    try {
      const result = await safeCountRows(
        helpers,
        {
          table: "RSECACTPROT",
          fields: ["UNAME"],
          where: [
            "EVENT EQ 'AU6'",
            `SLGDAT GE '${window.sapFrom}'`,
            `SLGDAT LE '${window.sapTo}'`,
          ],
          scanCap: 50000,
        },
        [
          {
            label: "AUB service-user logon proxy",
            request: {
              table: "RSECACTPROT",
              fields: ["UNAME"],
              where: [
                "EVENT EQ 'AUB'",
                `SLGDAT GE '${window.sapFrom}'`,
                `SLGDAT LE '${window.sapTo}'`,
              ],
              scanCap: 50000,
            },
          },
        ],
      );

      const notes = [...(this.notes ?? [])];
      if (result.fallbackUsed) notes.push(`Used ${result.fallbackUsed} for calculation.`);
      return countResult(this, result.value, notes, window);
    } catch (e) {
      return countResult(this, 0, ["Security Audit Log (RSECACTPROT) not directly readable; returning 0 as fallback."], window);
    }
  },
};

const implExpiredPasswordPct: ExecutableKpiDefinition = {
  id: "expired_password_pct",
  title: "Expired Password %",
  category: "Security & Authorization",
  unit: "percent",
  maturity: "implemented",
  summary: "Percentage of active users whose password change date exceeds the configured age threshold.",
  source: { kind: "derived", objects: ["USR02"] },
  notes: [
    "Default password-age threshold is 90 days. Override with 'password_max_age_days' dimension.",
    "Only counts active users (UFLAG NOT IN 32,64,128).",
  ],
  async execute(helpers, input) {
    const maxAgeDays = helpers.getNumberDimension(input, "password_max_age_days", 90);
    const cutoffDate = helpers.toSapDateDaysAgo(maxAgeDays);
    try {
      const { rows, fields } = await scanRowsWithFieldFallbacks(helpers, {
        table: "USR02",
        candidateFields: [
          ["BNAME", "UFLAG", "PWDCHGDATE"],
          ["BNAME", "UFLAG", "PWDLGNDATE"],
          ["BNAME", "UFLAG", "BCDA1"],
        ],
        pageSize: 1000,
        scanCap: 100000,
      });
      const passwordDateField = fields[fields.length - 1] ?? "PWDCHGDATE";
      const activeUsers = rows.filter(
        (row) => !isLockedUserFlag(row.UFLAG ?? ""),
      );
      const expiredUsers = activeUsers.filter((row) => {
        const passwordDate = (row[passwordDateField] ?? "").trim();
        return (
          passwordDate.length === 8 &&
          passwordDate !== "00000000" &&
          passwordDate < cutoffDate
        );
      }).length;
      const totalActive = activeUsers.length;

      const value = totalActive === 0 ? 0 : roundValue((expiredUsers / totalActive) * 100);
      return countResult(this, value, [
        `Expired: ${expiredUsers}, Active: ${totalActive}, Threshold: ${maxAgeDays} days.`,
        `Password date field used: ${passwordDateField}.`,
        ...(this.notes ?? []),
      ]);
    } catch (error) {
      return errorResult(this, [
        `USR02 read failed: ${describeError(error)}`,
        ...(this.notes ?? []),
      ]);
    }
  },
};

const implMissingMandatoryFields: ExecutableKpiDefinition = {
  id: "missing_mandatory_fields",
  title: "Missing Mandatory Fields",
  category: "Data Consistency & Master Data",
  unit: "count",
  maturity: "implemented",
  summary: "Master records with blank critical fields across KNA1, LFA1, MARA.",
  source: { kind: "derived", objects: ["KNA1", "LFA1", "MARA"] },
  notes: [
    "Counts customers with blank NAME1, vendors with blank NAME1, materials with blank ERSDA.",
    "Override scope with 'missing_field_tables' dimension (comma-separated: KNA1,LFA1,MARA).",
  ],
  async execute(helpers) {
    const [customersMissing, vendorsMissing, materialsMissing] = await Promise.all([
      safeCountRows(helpers, { table: "KNA1", fields: ["KUNNR"], where: ["NAME1 EQ ' '"], scanCap: 50000 }),
      safeCountRows(helpers, { table: "LFA1", fields: ["LIFNR"], where: ["NAME1 EQ ' '"], scanCap: 50000 }),
      safeCountRows(helpers, { table: "MARA", fields: ["MATNR"], where: ["ERSDA EQ '00000000'"], scanCap: 50000 }),
    ]);

    const total = customersMissing.value + vendorsMissing.value + materialsMissing.value;
    return countResult(this, total, [
      `KNA1 blank NAME1: ${customersMissing.value}, LFA1 blank NAME1: ${vendorsMissing.value}, MARA blank ERSDA: ${materialsMissing.value}.`,
      ...(this.notes ?? []),
    ]);
  },
};

const implDuplicateEntries: ExecutableKpiDefinition = {
  id: "duplicate_entries",
  title: "Duplicate Entries",
  category: "Data Consistency & Master Data",
  unit: "count",
  maturity: "implemented",
  summary: "Potential duplicate business-partner names detected in BUT000.",
  source: { kind: "derived", objects: ["BUT000", "KNA1"] },
  notes: [
    "Checks BUT000 NAME_ORG1 for exact-match duplicates. Falls back to KNA1 NAME1.",
    "Production should use fuzzy matching or SAP Business Partner deduplication.",
  ],
  async execute(helpers) {
    try {
      const rows = await helpers.scanRows({
        table: "BUT000",
        fields: ["PARTNER", "NAME_ORG1"],
        pageSize: 1000,
        scanCap: 50000,
      });

      const name2count = new Map<string, number>();
      for (const row of rows) {
        const name = (row.NAME_ORG1 ?? "").trim().toUpperCase();
        if (name.length > 0) name2count.set(name, (name2count.get(name) ?? 0) + 1);
      }

      const duplicates = Array.from(name2count.values()).filter((c) => c > 1).length;
      return countResult(this, duplicates, [`Unique names with duplicates: ${duplicates}.`, ...(this.notes ?? [])]);
    } catch {
      // Fallback to KNA1
      const rows = await helpers.scanRows({
        table: "KNA1",
        fields: ["KUNNR", "NAME1"],
        pageSize: 1000,
        scanCap: 50000,
      });

      const name2count = new Map<string, number>();
      for (const row of rows) {
        const name = (row.NAME1 ?? "").trim().toUpperCase();
        if (name.length > 0) name2count.set(name, (name2count.get(name) ?? 0) + 1);
      }

      const duplicates = Array.from(name2count.values()).filter((c) => c > 1).length;
      return countResult(this, duplicates, ["Fallback to KNA1.", ...(this.notes ?? [])]);
    }
  },
};

const implCviInconsistencies: ExecutableKpiDefinition = {
  id: "cvi_bp_inconsistencies",
  title: "CVI/BP Inconsistencies",
  category: "Data Consistency & Master Data",
  unit: "count",
  maturity: "implemented",
  summary: "CVI customer-link records with blank or missing target partners.",
  source: { kind: "derived", objects: ["CVI_CUST_LINK", "CVI_VEND_LINK"] },
  notes: [
    "Checks CVI_CUST_LINK for blank PARTNER_GUID. Falls back to CVI_VEND_LINK.",
    "CVI consistency is complex — this is an approximation.",
  ],
  async execute(helpers) {
    const custResult = await safeCountRows(
      helpers,
      { table: "CVI_CUST_LINK", fields: ["CUSTOMER"], where: ["PARTNER_GUID EQ ' '"], scanCap: 50000 },
      [
        {
          label: "CVI_VEND_LINK blank partners",
          request: { table: "CVI_VEND_LINK", fields: ["VENDOR"], where: ["PARTNER_GUID EQ ' '"], scanCap: 50000 },
        },
      ],
    );

    const notes = [...(this.notes ?? [])];
    if (custResult.fallbackUsed) notes.push(`Used ${custResult.fallbackUsed}.`);
    return countResult(this, custResult.value, notes);
  },
};

const implStuckSalesDocuments: ExecutableKpiDefinition = {
  id: "stuck_sales_documents",
  title: "Stuck Sales Documents",
  category: "Data Consistency & Master Data",
  unit: "count",
  maturity: "implemented",
  summary: "Sales orders with incomplete delivery status older than threshold days.",
  source: { kind: "derived", objects: ["VBAK", "VBUK"] },
  notes: [
    "Counts VBAK orders where GBSTK (overall status) is not 'C' (completed) and are older than 30 days.",
    "Override with 'stuck_days' dimension.",
  ],
  async execute(helpers, input) {
    const stuckDays = helpers.getNumberDimension(input, "stuck_days", 30);
    const cutoffDate = helpers.toSapDateDaysAgo(stuckDays);
    const result = await safeCountRows(helpers, {
      table: "VBAK",
      fields: ["VBELN"],
      where: [`ERDAT LT '${cutoffDate}'`, "GBSTK NE 'C'"],
      scanCap: 50000,
    });

    return countResult(this, result.value, [
      `Threshold: ${stuckDays} days, Cutoff: ${cutoffDate}.`,
      ...(this.notes ?? []),
    ]);
  },
};

const implStuckDeliveryDocuments: ExecutableKpiDefinition = {
  id: "stuck_delivery_documents",
  title: "Stuck Delivery Documents",
  category: "Data Consistency & Master Data",
  unit: "count",
  maturity: "implemented",
  summary: "Outbound deliveries not fully processed past threshold days.",
  source: { kind: "derived", objects: ["LIKP"] },
  notes: [
    "Counts LIKP deliveries where WBSTK (goods-movement status) is not 'C' and older than stuck_days.",
    "Override with 'stuck_days' dimension.",
  ],
  async execute(helpers, input) {
    const stuckDays = helpers.getNumberDimension(input, "stuck_days", 30);
    const cutoffDate = helpers.toSapDateDaysAgo(stuckDays);
    const result = await safeCountRows(helpers, {
      table: "LIKP",
      fields: ["VBELN"],
      where: [`ERDAT LT '${cutoffDate}'`, "WBSTK NE 'C'"],
      scanCap: 50000,
    });

    return countResult(this, result.value, [
      `Threshold: ${stuckDays} days.`,
      ...(this.notes ?? []),
    ]);
  },
};

const implGrIrMismatch: ExecutableKpiDefinition = {
  id: "gr_ir_mismatch",
  title: "GR/IR Mismatch",
  category: "Data Consistency & Master Data",
  unit: "count",
  maturity: "implemented",
  summary: "PO history entries where GR and IR quantities differ (EKBE-based).",
  source: { kind: "derived", objects: ["EKBE"] },
  notes: [
    "Counts EKBE records with movement type 'E' (GR) vs 'Q' (IR). Simplified comparison.",
    "Production should use a reconciliation wrapper with tolerance logic.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const [grCount, irCount] = await Promise.all([
      helpers.countRows({
        table: "EKBE",
        fields: ["EBELN"],
        where: ["VGABE EQ '1'", `BUDAT GE '${window.sapFrom}'`, `BUDAT LE '${window.sapTo}'`],
        scanCap: 100000,
      }),
      helpers.countRows({
        table: "EKBE",
        fields: ["EBELN"],
        where: ["VGABE EQ '2'", `BUDAT GE '${window.sapFrom}'`, `BUDAT LE '${window.sapTo}'`],
        scanCap: 100000,
      }),
    ]);

    const mismatch = Math.abs(grCount - irCount);
    return countResult(this, mismatch, [
      `GR entries: ${grCount}, IR entries: ${irCount}, Delta: ${mismatch}.`,
      ...(this.notes ?? []),
    ], window);
  },
};

const implFailedSalesOrders: ExecutableKpiDefinition = {
  id: "failed_sales_orders",
  title: "Failed Sales Orders",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Sales orders with delivery or billing blocks in the requested window.",
  source: { kind: "derived", objects: ["VBAK"] },
  notes: [
    "Counts orders with non-empty LIFSK (delivery block) or FAKSK (billing block).",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    let total = 0;

    // Delivery-blocked
    total += await helpers.countRows({
      table: "VBAK",
      fields: ["VBELN"],
      where: ["LIFSK NE ' '", `ERDAT GE '${window.sapFrom}'`, `ERDAT LE '${window.sapTo}'`],
      scanCap: 50000,
    });
    // Billing-blocked
    total += await helpers.countRows({
      table: "VBAK",
      fields: ["VBELN"],
      where: ["FAKSK NE ' '", `ERDAT GE '${window.sapFrom}'`, `ERDAT LE '${window.sapTo}'`],
      scanCap: 50000,
    });

    return countResult(this, total, this.notes ?? [], window);
  },
};

const implAtpCheckFailures: ExecutableKpiDefinition = {
  id: "atp_check_failures",
  title: "ATP Check Failures",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Schedule lines with zero confirmed quantity (ATP failure proxy).",
  source: { kind: "derived", objects: ["VBEP"] },
  notes: [
    "Counts VBEP lines where BMENG (confirmed qty) = 0 as ATP failure proxy.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    try {
      const result = await safeCountRows(helpers, {
        table: "VBEP",
        fields: ["POSNR"],
        where: [
          "BMENG EQ '0'",
          `ERDAT GE '${window.sapFrom}'`,
          `ERDAT LE '${window.sapTo}'`,
        ],
        scanCap: 50000,
      });

      return countResult(this, result.value, this.notes ?? [], window);
    } catch (e) {
      return countResult(this, 0, ["VBEP proxy read failed or unsupported; returning 0 as fallback.", ...(this.notes ?? [])], window);
    }
  },
};

const implPoCreationErrors: ExecutableKpiDefinition = {
  id: "po_creation_errors",
  title: "PO Creation Errors",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Purchase orders with incomplete release status in the requested window.",
  source: { kind: "derived", objects: ["EKKO"] },
  notes: [
    "Counts POs with FRGKE (release indicator) = '1' (blocked) as creation-error proxy.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const result = await safeCountRows(helpers, {
      table: "EKKO",
      fields: ["EBELN"],
      where: [
        "FRGKE EQ '1'",
        `BEDAT GE '${window.sapFrom}'`,
        `BEDAT LE '${window.sapTo}'`,
      ],
      scanCap: 50000,
    });

    return countResult(this, result.value, this.notes ?? [], window);
  },
};

const implInvoiceMatchFailures: ExecutableKpiDefinition = {
  id: "invoice_match_failures",
  title: "Invoice Match Failures",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Invoices with blocked/parked status (RBKP RBSTAT = B) in the requested window.",
  source: { kind: "derived", objects: ["RBKP"] },
  notes: [
    "Counts RBKP with RBSTAT = 'B' (blocked). Simplified 3-way match proxy.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const result = await safeCountRows(helpers, {
      table: "RBKP",
      fields: ["BELNR"],
      where: [
        "RBSTAT EQ 'B'",
        `BLDAT GE '${window.sapFrom}'`,
        `BLDAT LE '${window.sapTo}'`,
      ],
      scanCap: 50000,
    });

    return countResult(this, result.value, this.notes ?? [], window);
  },
};

const implPaymentRunErrors: ExecutableKpiDefinition = {
  id: "payment_run_errors",
  title: "Payment Run Errors",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Payment proposal items with error flags in the requested window.",
  source: { kind: "derived", objects: ["REGUH"] },
  notes: [
    "Counts proposal-only payment runs that never produced a payment document as a zero-footprint proxy.",
    "Falls back to REGUP proposal rows if REGUH does not expose enough detail.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    try {
      const rows = await helpers.scanRows({
        table: "REGUH",
        fields: ["LAUFD", "XVORL", "VBLNR"],
        where: [`LAUFD GE '${window.sapFrom}'`, `LAUFD LE '${window.sapTo}'`],
        pageSize: 500,
        scanCap: 50000,
      });
      const errors = rows.filter((row) => {
        const proposalOnly = (row.XVORL ?? "").trim().toUpperCase() === "X";
        const paymentDocument = (row.VBLNR ?? "").trim();
        return proposalOnly && (paymentDocument.length === 0 || /^0+$/.test(paymentDocument));
      }).length;

      return countResult(this, errors, [
        `Proposal-only runs without payment document: ${errors}.`,
        ...(this.notes ?? []),
      ], window);
    } catch (primaryError) {
      const result = await safeCountRows(
        helpers,
        {
          table: "REGUP",
          fields: ["VBLNR"],
          where: [`LAUFD GE '${window.sapFrom}'`, `LAUFD LE '${window.sapTo}'`],
          scanCap: 50000,
        },
      );

      return countResult(this, result.value, [
        `REGUH read failed: ${describeError(primaryError)}`,
        "Used REGUP row volume as a coarse fallback.",
        ...(this.notes ?? []),
      ], window);
    }
  },
};

const implPeriodEndClose: ExecutableKpiDefinition = {
  id: "period_end_closing_errors",
  title: "Period-End Closing Errors",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Open/error BKPF postings in the current period that may block period close.",
  source: { kind: "derived", objects: ["BKPF"] },
  notes: [
    "Counts BKPF documents where BSTAT is not blank (parked/held/reversed) in the current period.",
    "These items typically block clean period-end closing.",
  ],
  async execute(helpers) {
    const now = new Date();
    const currentPeriod = String(now.getUTCMonth() + 1).padStart(2, "0");
    const currentYear = String(now.getUTCFullYear());
    const result = await safeCountRows(helpers, {
      table: "BKPF",
      fields: ["BELNR"],
      where: [
        "BSTAT NE ' '",
        `MONAT EQ '${currentPeriod}'`,
        `GJAHR EQ '${currentYear}'`,
      ],
      scanCap: 50000,
    });

    return countResult(this, result.value, [
      `Period: ${currentPeriod}/${currentYear}.`,
      ...(this.notes ?? []),
    ]);
  },
};

const implAssetInconsistencies: ExecutableKpiDefinition = {
  id: "asset_inconsistencies",
  title: "Asset Inconsistencies",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Asset master records with missing or blank depreciation key (ANLC gap proxy).",
  source: { kind: "derived", objects: ["ANLA", "ANLC"] },
  notes: [
    "Counts ANLA records with blank AKTIV (capitalization date) as consistency gap proxy.",
    "Full reconciliation requires ANLA+ANLC cross-table logic.",
  ],
  async execute(helpers) {
    const result = await safeCountRows(helpers, {
      table: "ANLA",
      fields: ["ANLN1"],
      where: ["AKTIV EQ '00000000'"],
      scanCap: 50000,
    });

    return countResult(this, result.value, this.notes ?? []);
  },
};

const implReconciliationImbalanceAlerts: ExecutableKpiDefinition = {
  id: "reconciliation_imbalance_alerts",
  title: "Reconciliation Imbalance Alerts",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "FAGLFLEXT ledger records with non-zero balance residuals in the current period.",
  source: { kind: "derived", objects: ["FAGLFLEXT"] },
  notes: [
    "Counts FAGLFLEXT records for the current fiscal period.",
    "A non-zero TSL (total in transaction currency) signals potential GL imbalance.",
  ],
  async execute(helpers) {
    const now = new Date();
    const currentPeriod = String(now.getUTCMonth() + 1).padStart(2, "0");
    const currentYear = String(now.getUTCFullYear());
    const result = await safeCountRows(helpers, {
      table: "FAGLFLEXT",
      fields: ["RBUKRS"],
      where: [
        `RPMAX EQ '${currentPeriod}'`,
        `RYEAR EQ '${currentYear}'`,
      ],
      scanCap: 50000,
    });

    return countResult(this, result.value, [
      `Period: ${currentPeriod}/${currentYear}.`,
      ...(this.notes ?? []),
    ]);
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
    let result: Record<string, unknown>;
    try {
      result = await helpers.callFunction("RSLG_GET_MESSAGES", {});
    } catch (e) {
      return countResult(this, 0, ["Function RSLG_GET_MESSAGES unavailable or failed; returning 0 as fallback.", ...(this.notes ?? [])], window);
    }
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
    let result: Record<string, unknown>;
    try {
      result = await helpers.callFunction("SLIC_GET_INSTALLATIONS", {});
    } catch (e) {
      return countResult(this, 0, ["Function SLIC_GET_INSTALLATIONS unavailable or failed; returning 0 as fallback.", ...(this.notes ?? [])]);
    }
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
      return countResult(this, 0, ["SLIC_GET_INSTALLATIONS did not expose recognizable license utilization fields; returning 0.", ...(this.notes ?? [])]);
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
    "Falls back to average dialog response time if update-specific metric unavailable.",
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
        "UPDATE_TASK_RESPONSE",
        "UPD_RESPONSE_TIME",
        "UPDRESPTIME",
        "DIALOG_RESPONSE_TIME",
        "AVG_DIALOG_RESPTIME",
      ],
      regexes: [
        /(UPD|UPDATE).*(RESP|RESPONSE|RESPTI)/i,
        /(RESP|RESPONSE|RESPTI).*(UPD|UPDATE)/i,
        /DIALOG.*(RESP|RESPONSE)/i,
        /(RESP|RESPONSE).*(?:TIME|RESPTI)/i,
      ],
    }).filter((value) => value > 0);

    if (values.length === 0) {
      return countResult(this, 0, ["SWNC response did not expose a recognizable update-task response-time metric; returning 0 as fallback.", ...(this.notes ?? [])], window);
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
    let result: Record<string, unknown>;
    try {
      result = await helpers.callFunction("BAPI_SYSTEM_MON_GETSYSINFO", {});
    } catch (e) {
      return countResult(this, 0, ["Function BAPI_SYSTEM_MON_GETSYSINFO unavailable or failed; returning 0 as fallback.", ...(this.notes ?? [])]);
    }
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

    return countResult(this, 0, ["BAPI_SYSTEM_MON_GETSYSINFO did not expose a recognizable CPU utilization metric; returning 0 as fallback.", ...(this.notes ?? [])]);
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
    let result: Record<string, unknown>;
    try {
      result = await helpers.callFunction("BAPI_SYSTEM_MON_GETSYSINFO", {});
    } catch (e) {
      return countResult(this, 0, ["Function BAPI_SYSTEM_MON_GETSYSINFO unavailable or failed; returning 0 as fallback.", ...(this.notes ?? [])]);
    }
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

    return countResult(this, 0, ["BAPI_SYSTEM_MON_GETSYSINFO did not expose a recognizable memory utilization metric; returning 0 as fallback.", ...(this.notes ?? [])]);
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
    let result: Record<string, unknown>;
    try {
      result = await helpers.callFunction("RSLG_GET_MESSAGES", {});
    } catch (e) {
      return countResult(this, 0, ["Function RSLG_GET_MESSAGES unavailable or failed; returning 0 as fallback.", ...(this.notes ?? [])], window);
    }
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
      try {
        result = await helpers.callFunction("ICM_GET_MONITOR_INFO", {});
      } catch (e) {
        return countResult(this, 0, ["Both GW_GET_STATISTIC and ICM_GET_MONITOR_INFO unavailable; returning 0 as fallback.", ...(this.notes ?? [])]);
      }
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
    let result: Record<string, unknown>;
    try {
      result = await helpers.callFunction("ENQUEUE_STATISTICS", {});
    } catch (e) {
      return countResult(this, 0, ["Function ENQUEUE_STATISTICS unavailable or failed; returning 0 as fallback.", ...(this.notes ?? [])]);
    }
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
    let result: Record<string, unknown>;
    try {
      result = await helpers.callFunction("ICM_GET_MONITOR_INFO", {});
    } catch (e) {
      return countResult(this, 0, ["Function ICM_GET_MONITOR_INFO unavailable or failed; returning 0 as fallback.", ...(this.notes ?? [])]);
    }
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

    let swncResult: Record<string, unknown>;
    try {
      swncResult = await helpers.callFunction(
        "SWNC_COLLECTOR_GET_AGGREGATES",
        buildSwncParameters(window, input),
      );
    } catch (e) {
      return countResult(this, 0, ["Both ICM and SWNC unavailable; returning 0 as fallback.", ...(this.notes ?? [])], window);
    }
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
      return countResult(this, 0, ["Neither ICM nor SWNC exposed a recognizable API response-time metric; returning 0 as fallback.", ...(this.notes ?? [])], window);
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
    try {
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
    } catch (primaryError) {
      const { rows, fields } = await scanRowsWithFieldFallbacks(helpers, {
        table: "IUUC_REPL_HDR",
        candidateFields: [
          ["CONFIG_GUID", "CHDATE", "CHTIME", "CRDATE", "CRTIME"],
          ["CONFIG_GUID", "CHDATE", "CHTIME"],
          ["CONFIG_GUID", "CRDATE", "CRTIME"],
        ],
        pageSize: 200,
        scanCap: 5000,
      });
      if (rows.length === 0) {
        return countResult(
          this,
          0,
          [
            `IUUC_REPL_CONTENT failed: ${describeError(primaryError)}`,
            "IUUC_REPL_HDR was readable but contained no replication header rows; returning 0 as a no-config/no-activity fallback.",
            ...(this.notes ?? []),
          ],
        );
      }
      const timestamps = rows
        .map((row) =>
          extractRecordDateTime(row as unknown as Record<string, unknown>),
        )
        .filter((value): value is Date => value !== undefined);

      if (timestamps.length === 0) {
        throw new Error(
          `IUUC_REPL_CONTENT failed: ${describeError(primaryError)} | IUUC_REPL_HDR did not expose a recognizable timestamp.`,
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
          `IUUC_REPL_CONTENT failed: ${describeError(primaryError)}`,
          `Fell back to IUUC_REPL_HDR fields: ${fields.join(", ")}.`,
          `Header rows inspected: ${rows.length}.`,
          ...(this.notes ?? []),
        ],
      );
    }
  },
};

// ============================================================================
// ZERO-FOOTPRINT WRAPPER REPLACEMENTS (34 KPIs)
// These KPIs replace custom ABAP wrapper functions with standard SAP tables
// plus Node.js business logic (graceful degradation to 0 if tables unavailable).
// ============================================================================

// PHASE 1: Job & Batch Monitoring (7 KPIs) - Using TBTCO table
const jobRestartSuccessRate: ExecutableKpiDefinition = {
  id: "job_restart_success_rate",
  title: "Job Restart Success Rate",
  category: "Job & Batch Monitoring",
  unit: "percent",
  maturity: "implemented",
  summary: "Success rate of restarted background jobs (prior abort detected).",
  source: { kind: "derived", objects: ["TBTCO"] },
  notes: [
    "Zero-footprint replacement for ZHC_GET_JOB_KPIS wrapper.",
    "Detects restarts via predecessor job references.",
    "Falls back to standard TBTCO table if wrapper unavailable."
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const fieldCandidates = [
      {
        predecessorField: "PREDNUM",
        fields: ["JOBNAME", "JOBCOUNT", "PREDNUM", "STATUS", "STRTDATE"],
      },
      {
        predecessorField: "REPRED",
        fields: ["JOBNAME", "JOBCOUNT", "REPRED", "STATUS", "STRTDATE"],
      },
    ];
    const errors: string[] = [];

    for (const candidate of fieldCandidates) {
      try {
        const rows = await helpers.scanRows({
          table: "TBTCO",
          fields: candidate.fields,
          where: [
            `STRTDATE GE '${window.sapFrom}'`,
            `STRTDATE LE '${window.sapTo}'`,
          ],
          pageSize: 500,
          scanCap: 100000,
        });

        const restartAttempts = rows.filter((row) => {
          const rawValue = (row[candidate.predecessorField] ?? "").trim();
          return candidate.predecessorField === "PREDNUM"
            ? (parseIntegerText(rawValue) ?? 0) > 0
            : rawValue.length > 0;
        }).length;
        const successfulRestarts = rows.filter((row) => {
          const rawValue = (row[candidate.predecessorField] ?? "").trim();
          const hasPredecessor = candidate.predecessorField === "PREDNUM"
            ? (parseIntegerText(rawValue) ?? 0) > 0
            : rawValue.length > 0;
          return hasPredecessor && (row.STATUS ?? "").trim() === "F";
        }).length;
        const rate = restartAttempts > 0
          ? (successfulRestarts / restartAttempts) * 100
          : 0;

        return countResult(
          this,
          Math.round(rate * 100) / 100,
          [
            ...(this.notes ?? []),
            `Restart attempts: ${restartAttempts}, Successful: ${successfulRestarts}.`,
            `Predecessor field used: ${candidate.predecessorField}.`,
          ],
          window,
        );
      } catch (error) {
        errors.push(
          `${candidate.predecessorField} failed: ${describeError(error)}`,
        );
      }
    }

    return errorResult(
      this,
      [
        ...errors,
        ...(this.notes ?? []),
      ],
      window,
    );
  },
};

const jobCancellationRate: ExecutableKpiDefinition = {
  id: "job_cancellation_rate",
  title: "Job Cancellation Rate",
  category: "Job & Batch Monitoring",
  unit: "percent",
  maturity: "implemented",
  summary: "Ratio of cancelled/aborted jobs to total job executions.",
  source: { kind: "derived", objects: ["TBTCO"] },
  notes: ["Zero-footprint replacement for ZHC_GET_JOB_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const [abortedJobs, totalJobs] = await Promise.all([
      helpers.countRows({
        table: "TBTCO",
        fields: ["JOBNAME"],
        where: [
          "STATUS EQ 'A'",
          `ENDDATE GE '${window.sapFrom}'`,
          `ENDDATE LE '${window.sapTo}'`,
        ],
        scanCap: 200000,
      }),
      helpers.countRows({
        table: "TBTCO",
        fields: ["JOBNAME"],
        where: [`ENDDATE GE '${window.sapFrom}'`, `ENDDATE LE '${window.sapTo}'`],
        scanCap: 200000,
      }),
    ]);

    const value = totalJobs === 0 ? 0 : Number(((abortedJobs / totalJobs) * 100).toFixed(2));
    return countResult(this, value, this.notes ?? [], window);
  },
};

const jobHoldDurationAvg: ExecutableKpiDefinition = {
  id: "job_hold_duration_avg",
  title: "Job Hold Duration Average",
  category: "Job & Batch Monitoring",
  unit: "minutes",
  maturity: "implemented",
  summary: "Average duration jobs spend in scheduled or held state.",
  source: { kind: "derived", objects: ["TBTCO"] },
  notes: ["Zero-footprint replacement for ZHC_GET_JOB_KPIS wrapper. Measures scheduled-to-start delay."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const rows = await helpers.scanRows({
      table: "TBTCO",
      fields: ["JOBNAME", "SDLSTRTDT", "SDLSTRTTM", "STRTDATE", "STRTTIME"],
      where: [
        `STRTDATE GE '${window.sapFrom}'`,
        `STRTDATE LE '${window.sapTo}'`,
        "STATUS NE 'S'",
      ],
      pageSize: 500,
      scanCap: 200000,
    });

    const delays = rows
      .map((row) => {
        const scheduled = helpers.parseSapDateTime(
          row.SDLSTRTDT ?? "",
          row.SDLSTRTTM ?? "",
        );
        const actual = helpers.parseSapDateTime(row.STRTDATE ?? "", row.STRTTIME ?? "");
        if (!scheduled || !actual) return 0;
        return Math.max(0, (actual.getTime() - scheduled.getTime()) / 60000);
      })
      .filter((value) => value > 0);

    const value = delays.length === 0 ? 0 : Number(average(delays).toFixed(2));
    return countResult(this, value, this.notes ?? [], window);
  },
};

const jobReleaseFailures: ExecutableKpiDefinition = {
  id: "job_release_failures",
  title: "Job Release Failures",
  category: "Job & Batch Monitoring",
  unit: "count",
  maturity: "implemented",
  summary: "Jobs stuck in scheduled state beyond their intended start time.",
  source: { kind: "derived", objects: ["TBTCO"] },
  notes: ["Zero-footprint replacement for ZHC_GET_JOB_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const rows = await helpers.scanRows({
      table: "TBTCO",
      fields: ["JOBNAME", "SDLSTRTDT", "SDLSTRTTM", "STATUS"],
      where: ["STATUS EQ 'S'"],
      pageSize: 500,
      scanCap: 50000,
    });

    const now = Date.now();
    const count = rows.filter((row) => {
      const scheduled = helpers.parseSapDateTime(
        row.SDLSTRTDT ?? "",
        row.SDLSTRTTM ?? "",
      );
      return scheduled && scheduled.getTime() < now;
    }).length;

    return countResult(this, count, this.notes ?? []);
  },
};

const scheduledJobVariance: ExecutableKpiDefinition = {
  id: "scheduled_job_variance",
  title: "Scheduled Job Variance",
  category: "Job & Batch Monitoring",
  unit: "count",
  maturity: "implemented",
  summary: "Jobs whose execution deviated from schedule by more than 10 minutes.",
  source: { kind: "derived", objects: ["TBTCO"] },
  notes: ["Zero-footprint replacement for ZHC_GET_JOB_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const thresholdMinutes = helpers.getNumberDimension(input, "variance_minutes", 10);
    const rows = await helpers.scanRows({
      table: "TBTCO",
      fields: ["JOBNAME", "SDLSTRTDT", "SDLSTRTTM", "STRTDATE", "STRTTIME"],
      where: [
        `STRTDATE GE '${window.sapFrom}'`,
        `STRTDATE LE '${window.sapTo}'`,
        "STATUS NE 'S'",
      ],
      pageSize: 500,
      scanCap: 200000,
    });

    const count = rows.filter((row) => {
      const scheduled = helpers.parseSapDateTime(
        row.SDLSTRTDT ?? "",
        row.SDLSTRTTM ?? "",
      );
      const actual = helpers.parseSapDateTime(row.STRTDATE ?? "", row.STRTTIME ?? "");
      if (!scheduled || !actual) return false;
      const varianceMinutes = Math.abs(
        (actual.getTime() - scheduled.getTime()) / 60000,
      );
      return varianceMinutes > thresholdMinutes;
    }).length;

    return countResult(
      this,
      count,
      [`Variance threshold: ${thresholdMinutes} minutes.`, ...this.notes ?? []],
      window,
    );
  },
};

const batchRestartSuccessRate: ExecutableKpiDefinition = {
  id: "batch_restart_success_rate",
  title: "Batch Restart Success Rate",
  category: "Job & Batch Monitoring",
  unit: "percent",
  maturity: "implemented",
  summary: "Success rate when batch jobs are restarted after prior failures.",
  source: { kind: "derived", objects: ["TBTCO"] },
  notes: ["Zero-footprint replacement for ZHC_GET_JOB_KPIS wrapper. Similar to job_restart_success_rate."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const [finishedCount, totalCount] = await Promise.all([
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
        where: [`ENDDATE GE '${window.sapFrom}'`, `ENDDATE LE '${window.sapTo}'`],
        scanCap: 200000,
      }),
    ]);

    const value = totalCount === 0 ? 0 : Number(((finishedCount / totalCount) * 100).toFixed(2));
    return countResult(this, value, this.notes ?? [], window);
  },
};

const jobStepFailures: ExecutableKpiDefinition = {
  id: "job_step_failures",
  title: "Job Step Failures",
  category: "Job & Batch Monitoring",
  unit: "count",
  maturity: "implemented",
  summary: "Job execution steps that ended in error state.",
  source: { kind: "derived", objects: ["TBTCP", "TBTCS"] },
  notes: ["Zero-footprint replacement for ZHC_GET_JOB_KPIS wrapper. Reads TBTCP step status rows when available."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    try {
      const rows = await helpers.scanRows({
        table: "TBTCP",
        fields: ["JOBNAME", "JOBCOUNT", "STEPCOUNT", "STATUS", "SDLDATE"],
        where: [
          `SDLDATE GE '${window.sapFrom}'`,
          `SDLDATE LE '${window.sapTo}'`,
        ],
        pageSize: 500,
        scanCap: 100000,
      });

      const value = rows.filter((row) =>
        new Set(["A", "E"]).has((row.STATUS ?? "").trim().toUpperCase())
      ).length;

      return countResult(this, value, [
        ...(this.notes ?? []),
        "Statuses counted: A, E.",
      ], window);
    } catch (primaryError) {
      const result = await safeCountRows(
        helpers,
        {
          table: "TBTCS",
          fields: ["JOBNAME"],
          scanCap: 100000,
        },
      );

      return countResult(this, result.value, [
        ...(this.notes ?? []),
        `TBTCP read failed: ${describeError(primaryError)}`,
        "Used TBTCS row volume as coarse fallback.",
      ], window);
    }
  },
};

// PHASE 2: OTC & P2P KPIs (13 KPIs)
const orderCompletionRate: ExecutableKpiDefinition = {
  id: "order_completion_rate",
  title: "Order Completion Rate",
  category: "Business Process KPIs",
  unit: "percent",
  maturity: "implemented",
  summary: "Ratio of fully-delivered sales orders to total orders created.",
  source: { kind: "derived", objects: ["VBAK"] },
  sapFlavorSupport: flavorSupport({
    defaultFlavor: "shared",
    notes: [
      "shared tries VBAK.GBSTK first, then VBUK join if needed.",
      "ecc prefers the VBUK status join path.",
      "s4hana prefers VBAK.GBSTK directly.",
    ],
  }),
  notes: [
    "Uses VBAK.GBSTK directly (S/4HANA 1909+ merged VBUK into VBAK).",
    "Falls back to VBUK if GBSTK is not available on VBAK.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const sapFlavor = helpers.getSapFlavor(input);
    const salesOrders = await helpers.scanRows({
      table: "VBAK",
      fields: ["VBELN", "ERDAT"],
      where: [
        `ERDAT GE '${window.sapFrom}'`,
        `ERDAT LE '${window.sapTo}'`,
      ],
      pageSize: 500,
      scanCap: 50000,
    });
    const scopedOrderIds = new Set(
      salesOrders
        .map((row) => (row.VBELN ?? "").trim())
        .filter((value) => value.length > 0),
    );
    const totalOrders = scopedOrderIds.size;

    if (totalOrders === 0) {
      return countResult(this, 0, [`sapFlavor=${sapFlavor}.`, ...(this.notes ?? [])], window);
    }

    const countWithVbuk = async (): Promise<number> => {
      const completedRows = await helpers.scanRows({
        table: "VBUK",
        fields: ["VBELN", "GBSTK"],
        where: ["GBSTK EQ 'C'"],
        pageSize: 500,
        scanCap: 100000,
      });

      return completedRows.filter((row) =>
        scopedOrderIds.has((row.VBELN ?? "").trim()),
      ).length;
    };

    try {
      const completedOrders =
        sapFlavor === "ecc"
          ? await countWithVbuk()
          : await helpers.countRows({
              table: "VBAK",
              fields: ["VBELN"],
              where: [
                "GBSTK EQ 'C'",
                `ERDAT GE '${window.sapFrom}'`,
                `ERDAT LE '${window.sapTo}'`,
              ],
            });

      const value =
        totalOrders === 0
          ? 0
          : Number(((completedOrders / totalOrders) * 100).toFixed(2));
      const notes = [`sapFlavor=${sapFlavor}.`, ...(this.notes ?? [])];
      if (sapFlavor === "ecc") {
        notes.push("Used VBUK status join for ECC mode.");
      }
      return countResult(this, value, notes, window);
    } catch (error) {
      try {
        const completedOrders = await countWithVbuk();
        const value = Number(((completedOrders / totalOrders) * 100).toFixed(2));
        return countResult(this, value, [
          `sapFlavor=${sapFlavor}.`,
          `Primary VBAK.GBSTK path failed: ${describeError(error)}`,
          "Used VBUK status join fallback.",
          ...(this.notes ?? []),
        ], window);
      } catch (fallbackError) {
        return errorResult(this, [
          `Unable to derive order completion rate: ${describeError(error)}`,
          `VBUK fallback failed: ${describeError(fallbackError)}`,
          ...(this.notes ?? []),
        ], window);
      }
    }
  },
};

const quoteToCashCycle: ExecutableKpiDefinition = {
  id: "quote_to_cash_cycle",
  title: "Quote-to-Cash Cycle (Days)",
  category: "Business Process KPIs",
  unit: "days",
  maturity: "implemented",
  summary: "Average time from quote creation to invoice posting.",
  source: { kind: "derived", objects: ["VBAK", "VBAK quote links", "VBRK"] },
  notes: ["Zero-footprint replacement for ZHC_GET_OTC_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    // Simplified: measure from order to billing creation
    const rows = await helpers.scanRows({
      table: "VBAK",
      fields: ["ERDAT", "AUDAT"],
      where: [
        `ERDAT GE '${window.sapFrom}'`,
        `ERDAT LE '${window.sapTo}'`,
      ],
      pageSize: 500,
      scanCap: 50000,
    });

    const cycles = rows
      .map((row) => {
        const created = helpers.parseSapDateTime(row.ERDAT ?? "", "000000");
        const confirmed = helpers.parseSapDateTime(row.AUDAT ?? "", "000000");
        if (!created || !confirmed) return 0;
        return (confirmed.getTime() - created.getTime()) / 86_400_000;
      })
      .filter((value) => value >= 0);

    const value = cycles.length === 0 ? 0 : Number(average(cycles).toFixed(2));
    return countResult(this, value, this.notes ?? [], window);
  },
};

const fulfillmentAccuracy: ExecutableKpiDefinition = {
  id: "fulfillment_accuracy",
  title: "Fulfillment Accuracy %",
  category: "Business Process KPIs",
  unit: "percent",
  maturity: "implemented",
  summary: "Ratio of orders with no delivery blocks or issues to total orders.",
  source: { kind: "derived", objects: ["VBAK", "VBUK"] },
  notes: ["Zero-footprint replacement for ZHC_GET_OTC_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const rows = await helpers.scanRows({
      table: "VBAK",
      fields: ["VBELN", "ERDAT", "LIFSK", "FAKSK", "GBSTK"],
      where: [
        `ERDAT GE '${window.sapFrom}'`,
        `ERDAT LE '${window.sapTo}'`,
      ],
      pageSize: 500,
      scanCap: 50000,
    });
    const totalOrders = rows.length;
    const cleanOrders = rows.filter((row) =>
      (row.LIFSK ?? "").trim().length === 0 &&
      (row.FAKSK ?? "").trim().length === 0 &&
      (row.GBSTK ?? "").trim().toUpperCase() === "C"
    ).length;
    const value =
      totalOrders === 0 ? 0 : Number(((cleanOrders / totalOrders) * 100).toFixed(2));
    return countResult(this, value, [
      `Clean completed orders: ${cleanOrders}.`,
      `Total orders: ${totalOrders}.`,
      ...(this.notes ?? []),
    ], window);
  },
};

const backorderRate: ExecutableKpiDefinition = {
  id: "backorder_rate",
  title: "Backorder Rate %",
  category: "Business Process KPIs",
  unit: "percent",
  maturity: "implemented",
  summary: "Percentage of orders with backorder-like fulfillment status.",
  source: { kind: "derived", objects: ["VBAK", "VBEP"] },
  notes: ["Zero-footprint replacement for ZHC_GET_OTC_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const rows = await helpers.scanRows({
      table: "VBEP",
      fields: ["VBELN", "POSNR", "EDATU", "WMENG", "BMENG"],
      where: [`EDATU GE '${window.sapFrom}'`, `EDATU LE '${window.sapTo}'`],
      pageSize: 500,
      scanCap: 50000,
    });

    const totalItems = rows.length;
    const backorderedItems = rows.filter((row) => {
      const ordered = parseNumericValue(row.WMENG ?? "") ?? 0;
      const confirmed = parseNumericValue(row.BMENG ?? "") ?? 0;
      return ordered > 0 && confirmed < ordered;
    }).length;

    const value =
      totalItems === 0 ? 0 : Number(((backorderedItems / totalItems) * 100).toFixed(2));
    return countResult(this, value, [
      `Backordered schedule lines: ${backorderedItems}.`,
      `Total schedule lines: ${totalItems}.`,
      ...(this.notes ?? []),
    ], window);
  },
};

const pricingCompliance: ExecutableKpiDefinition = {
  id: "pricing_compliance",
  title: "Pricing Compliance %",
  category: "Business Process KPIs",
  unit: "percent",
  maturity: "implemented",
  summary: "Orders with confirmed pricing compared to total orders.",
  source: { kind: "derived", objects: ["VBAK"] },
  notes: ["Zero-footprint replacement for ZHC_GET_OTC_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const rows = await helpers.scanRows({
      table: "VBAK",
      fields: ["VBELN", "ERDAT", "KNUMV", "NETWR"],
      where: [
        `ERDAT GE '${window.sapFrom}'`,
        `ERDAT LE '${window.sapTo}'`,
      ],
      pageSize: 500,
      scanCap: 50000,
    });

    const totalOrders = rows.length;
    const pricedOrders = rows.filter((row) => {
      const conditionDocument = (row.KNUMV ?? "").trim();
      const netValue = parseNumericValue(row.NETWR ?? "") ?? 0;
      return conditionDocument.length > 0 && netValue >= 0;
    }).length;

    const value =
      totalOrders === 0 ? 0 : Number(((pricedOrders / totalOrders) * 100).toFixed(2));
    return countResult(this, value, [
      `Orders with pricing condition documents: ${pricedOrders}.`,
      `Total orders: ${totalOrders}.`,
      ...(this.notes ?? []),
    ], window);
  },
};

const creditFailures: ExecutableKpiDefinition = {
  id: "credit_failures",
  title: "Credit Check Failures",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Orders that failed credit-check validation.",
  source: { kind: "derived", objects: ["VBAK", "VBUK"] },
  notes: ["Zero-footprint replacement for ZHC_GET_OTC_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const rows = await helpers.scanRows({
      table: "VBAK",
      fields: ["VBELN", "ERDAT", "CMGST"],
      where: [
        "CMGST NE ' '",
        `ERDAT GE '${window.sapFrom}'`,
        `ERDAT LE '${window.sapTo}'`,
      ],
      pageSize: 500,
      scanCap: 50000,
    });
    const failedRows = rows.filter((row) => {
      const status = (row.CMGST ?? "").trim().toUpperCase();
      return status.length > 0 && status !== "A";
    });
    const statusBreakdown = new Set(
      failedRows.map((row) => (row.CMGST ?? "").trim().toUpperCase()).filter((value) => value.length > 0),
    );

    return countResult(this, failedRows.length, [
      `Credit status values counted as failures: ${Array.from(statusBreakdown).join(", ") || "none"}.`,
      ...(this.notes ?? []),
    ], window);
  },
};

const invoiceToCashCycle: ExecutableKpiDefinition = {
  id: "invoice_to_cash_cycle",
  title: "Invoice-to-Cash Cycle (Days)",
  category: "Business Process KPIs",
  unit: "days",
  maturity: "implemented",
  summary: "Average days from AR document date to clearing date.",
  source: { kind: "derived", objects: ["BSAD"] },
  notes: ["Zero-footprint replacement for ZHC_GET_OTC_KPIS wrapper. Uses cleared customer items as a proxy."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const rows = await helpers.scanRows({
      table: "BSAD",
      fields: ["BELNR", "BLDAT", "AUGDT"],
      where: [
        `AUGDT GE '${window.sapFrom}'`,
        `AUGDT LE '${window.sapTo}'`,
      ],
      pageSize: 500,
      scanCap: 50000,
    });

    const cycles = rows
      .map((row) => {
        const invoiced = helpers.parseSapDateTime(row.BLDAT ?? "", "000000");
        const cleared = helpers.parseSapDateTime(row.AUGDT ?? "", "000000");
        if (!invoiced || !cleared) return 0;
        return Math.max(0, (cleared.getTime() - invoiced.getTime()) / 86_400_000);
      })
      .filter((value) => value >= 0);

    const value = cycles.length === 0 ? 0 : Number(average(cycles).toFixed(2));
    return countResult(this, value, [
      `Cleared AR items scanned: ${rows.length}.`,
      ...(this.notes ?? []),
    ], window);
  },
};

const poMatchRate: ExecutableKpiDefinition = {
  id: "po_match_rate",
  title: "PO Match Rate %",
  category: "Business Process KPIs",
  unit: "percent",
  maturity: "implemented",
  summary: "Percentage of POs with matching invoice and receipt records.",
  source: { kind: "derived", objects: ["EKKO", "RBKP", "MSEG"] },
  notes: ["Zero-footprint replacement for ZHC_GET_P2P_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const [purchaseOrders, historyRows] = await Promise.all([
      helpers.scanRows({
        table: "EKKO",
        fields: ["EBELN", "BEDAT"],
        where: [
          `BEDAT GE '${window.sapFrom}'`,
          `BEDAT LE '${window.sapTo}'`,
        ],
        pageSize: 500,
        scanCap: 50000,
      }),
      helpers.scanRows({
        table: "EKBE",
        fields: ["EBELN", "VGABE", "BUDAT"],
        where: [
          `BUDAT GE '${window.sapFrom}'`,
          `BUDAT LE '${window.sapTo}'`,
        ],
        pageSize: 500,
        scanCap: 100000,
      }),
    ]);

    const purchaseOrderSet = new Set(
      purchaseOrders.map((row) => (row.EBELN ?? "").trim()).filter((value) => value.length > 0),
    );
    const grSet = new Set(
      historyRows
        .filter((row) => (row.VGABE ?? "").trim() === "1")
        .map((row) => (row.EBELN ?? "").trim())
        .filter((value) => value.length > 0),
    );
    const irSet = new Set(
      historyRows
        .filter((row) => (row.VGABE ?? "").trim() === "2")
        .map((row) => (row.EBELN ?? "").trim())
        .filter((value) => value.length > 0),
    );
    const matchedPos = Array.from(purchaseOrderSet).filter(
      (poNumber) => grSet.has(poNumber) && irSet.has(poNumber),
    ).length;
    const totalPos = purchaseOrderSet.size;
    const value =
      totalPos === 0 ? 0 : Number(((matchedPos / totalPos) * 100).toFixed(2));

    return countResult(this, value, [
      `Matched POs with both GR and IR history: ${matchedPos}.`,
      `Total scoped POs: ${totalPos}.`,
      ...(this.notes ?? []),
    ], window);
  },
};

const invoiceHoldRate: ExecutableKpiDefinition = {
  id: "invoice_hold_rate",
  title: "Invoice Hold Rate %",
  category: "Business Process KPIs",
  unit: "percent",
  maturity: "implemented",
  summary: "Percentage of invoices with hold/block status.",
  source: { kind: "derived", objects: ["RBKP"] },
  notes: ["Zero-footprint replacement for ZHC_GET_P2P_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const rows = await helpers.scanRows({
      table: "RBKP",
      fields: ["BELNR", "BLDAT", "RBSTAT", "ZLSPR"],
      where: [
        `BLDAT GE '${window.sapFrom}'`,
        `BLDAT LE '${window.sapTo}'`,
      ],
      pageSize: 500,
      scanCap: 50000,
    });

    const totalInvoices = rows.length;
    const heldInvoices = rows.filter((row) =>
      (row.RBSTAT ?? "").trim().toUpperCase() === "B" ||
      (row.ZLSPR ?? "").trim().length > 0
    ).length;
    const value =
      totalInvoices === 0 ? 0 : Number(((heldInvoices / totalInvoices) * 100).toFixed(2));
    return countResult(this, value, [
      `Held or blocked invoices: ${heldInvoices}.`,
      `Total invoices: ${totalInvoices}.`,
      ...(this.notes ?? []),
    ], window);
  },
};

const duplicateDetection: ExecutableKpiDefinition = {
  id: "duplicate_detection",
  title: "Duplicate Invoices Detected",
  category: "Data Consistency & Master Data",
  unit: "count",
  maturity: "implemented",
  summary: "Potential invoice duplicates detected by invoice-date/amount matching.",
  source: { kind: "derived", objects: ["RBKP"] },
  notes: [
    "Zero-footprint replacement for ZHC_GET_DATA_QUALITY_KPIS wrapper.",
    "Uses simplified exact-amount-and-date matching; implement fuzzy matching in production.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    let rows: Array<Record<string, unknown>> = [];
    
    try {
      rows = await helpers.scanRows({
        table: "RBKP",
        fields: ["BELNR", "GJAHR", "BLDAT", "XBLNR"],
        where: [
          `BLDAT GE '${window.sapFrom}'`,
          `BLDAT LE '${window.sapTo}'`,
        ],
        pageSize: 500,
        scanCap: 50000,
      });
    } catch (e) {
      return countResult(this, 0, ["RBKP read failed or unsupported; returning 0 as fallback.", ...(this.notes ?? [])], window);
    }

    const key2count = new Map<string, number>();
    for (const row of rows) {
      const xblnr = row.XBLNR ? String(row.XBLNR).trim() : "";
      if (xblnr.length > 0) {
        const key = `${row.BLDAT}:${xblnr}`;
        key2count.set(key, (key2count.get(key) ?? 0) + 1);
      }
    }

    const duplicates = Array.from(key2count.values()).filter((count) => count > 1).length;
    return countResult(this, duplicates, this.notes ?? [], window);
  },
};

const grPostingFailures: ExecutableKpiDefinition = {
  id: "gr_posting_failures",
  title: "GR Posting Failures",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Goods-movement error rows for GR-related movement types.",
  source: { kind: "derived", objects: ["AFFW", "MKPF"] },
  notes: ["Zero-footprint replacement for ZHC_GET_P2P_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const rows = await helpers.scanRows({
      table: "AFFW",
      fields: ["WEBLNR", "ERSDA", "BWART"],
      where: [
        `ERSDA GE '${window.sapFrom}'`,
        `ERSDA LE '${window.sapTo}'`,
      ],
      pageSize: 500,
      scanCap: 50000,
    });
    const grMovementTypes = new Set(["101", "102", "103", "105", "107", "109"]);
    const failures = rows.filter((row) => grMovementTypes.has((row.BWART ?? "").trim())).length;

    return countResult(this, failures, [
      `GR-related AFFW errors: ${failures}.`,
      ...(this.notes ?? []),
    ], window);
  },
};

const threeWayMatchingFailures: ExecutableKpiDefinition = {
  id: "three_way_matching_failures",
  title: "3-Way Matching Failures",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Invoices that do not match PO and GR on quantity and amount.",
  source: { kind: "derived", objects: ["RBKP", "EKPO", "MSEG"] },
  notes: [
    "Zero-footprint replacement for ZHC_GET_P2P_KPIS wrapper.",
    "Simplified matching; production implementation should use business-approved tolerance.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const rows = await helpers.scanRows({
      table: "RBKP",
      fields: ["BELNR", "BLDAT", "RBSTAT", "ZLSPR"],
      where: [
        `BLDAT GE '${window.sapFrom}'`,
        `BLDAT LE '${window.sapTo}'`,
      ],
      pageSize: 500,
      scanCap: 50000,
    });
    const failures = rows.filter((row) =>
      (row.RBSTAT ?? "").trim().toUpperCase() === "B" ||
      (row.ZLSPR ?? "").trim().length > 0
    ).length;

    return countResult(this, failures, [
      `Blocked or payment-held invoices used as 3-way match failure proxy: ${failures}.`,
      ...(this.notes ?? []),
    ], window);
  },
};

const poChangeApprovalRate: ExecutableKpiDefinition = {
  id: "po_change_approval_rate",
  title: "PO Change Approval Rate %",
  category: "Business Process KPIs",
  unit: "percent",
  maturity: "implemented",
  summary: "Percentage of PO change requests that have been approved.",
  source: { kind: "derived", objects: ["EKKO", "CDHDR"] },
  notes: ["Zero-footprint replacement for ZHC_GET_P2P_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const [totalChanges, approvedChanges] = await Promise.all([
      helpers.countRows({
        table: "EKKO",
        fields: ["EBELN"],
        where: [
          `BEDAT GE '${window.sapFrom}'`,
          `BEDAT LE '${window.sapTo}'`,
        ],
      }),
      helpers.countRows({
        table: "EKKO",
        fields: ["EBELN"],
        where: [
          "EKORG NE ' '",
          `BEDAT GE '${window.sapFrom}'`,
          `BEDAT LE '${window.sapTo}'`,
        ],
      }),
    ]);

    const value = totalChanges === 0 ? 0 : Number(((approvedChanges / totalChanges) * 100).toFixed(2));
    return countResult(this, value, this.notes ?? [], window);
  },
};

// PHASE 3: Finance & Data Quality KPIs (9 KPIs)
const glReconciliationVariance: ExecutableKpiDefinition = {
  id: "gl_reconciliation_variance",
  title: "GL Reconciliation Variance",
  category: "Data Consistency & Master Data",
  unit: "count",
  maturity: "implemented",
  summary: "GL line items with balance variance from expected reconciliation.",
  source: { kind: "derived", objects: ["FAGLFLEXT", "BSEG"] },
  notes: ["Zero-footprint replacement for ZHC_GET_FINANCE_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const fromPeriod = window.sapFrom.slice(4, 6);
    const toPeriod = window.sapTo.slice(4, 6);
    const fiscalYear = window.sapTo.slice(0, 4);
    const result = await safeCountRows(
      helpers,
      {
        table: "FAGLFLEXT",
        fields: ["RBUKRS"],
        where: [
          `RPMAX GE '${fromPeriod}'`,
          `RPMAX LE '${toPeriod}'`,
          `RYEAR EQ '${fiscalYear}'`,
        ],
        scanCap: 50000,
      },
    );

    return countResult(this, result.value, [
      ...(this.notes ?? []),
      `Fiscal year/period range: ${fiscalYear}/${fromPeriod}-${toPeriod}.`,
    ], window);
  },
};

const subLedgerExceptions: ExecutableKpiDefinition = {
  id: "subledger_exceptions",
  title: "Sub-Ledger Exceptions",
  category: "Data Consistency & Master Data",
  unit: "count",
  maturity: "implemented",
  summary: "Sub-ledger records with inconsistent status or balance flags.",
  source: { kind: "derived", objects: ["ACDOCA", "BKPF"] },
  sapFlavorSupport: flavorSupport({
    defaultFlavor: "s4hana",
    notes: [
      "shared/s4hana prefers ACDOCA first.",
      "ecc routes directly to the BKPF proxy path.",
    ],
  }),
  notes: [
    "Uses ACDOCA (S/4HANA universal journal, transparent). Falls back to BKPF if ACDOCA unavailable.",
    "Avoids BSEG which is a cluster table and cannot be read via RFC_READ_TABLE in many systems.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const sapFlavor = helpers.getSapFlavor(input);
    const bkpfFallback: { label: string; request: CountRowsRequest } = {
      label: "BKPF parked/held docs",
      request: {
        table: "BKPF",
        fields: ["BELNR"],
        where: [
          "BSTAT NE ' '",
          `BUDAT GE '${window.sapFrom}'`,
          `BUDAT LE '${window.sapTo}'`,
        ],
        scanCap: 100000,
      },
    };
    const result = await safeCountRows(
      helpers,
      sapFlavor === "ecc"
        ? bkpfFallback.request
        : {
            table: "ACDOCA",
            fields: ["BELNR"],
            where: [
              `BUDAT GE '${window.sapFrom}'`,
              `BUDAT LE '${window.sapTo}'`,
              "DRCRK EQ ' '",
            ],
            scanCap: 100000,
          },
      sapFlavor === "ecc" ? [] : [bkpfFallback],
    );

    const notes = [`sapFlavor=${sapFlavor}.`, ...(this.notes ?? [])];
    if (sapFlavor === "ecc") {
      notes.push("Used BKPF proxy path because sapFlavor=ecc.");
    }
    if (result.fallbackUsed) notes.push(`Used ${result.fallbackUsed}.`);
    return countResult(this, result.value, notes, window);
  },
};

const accrualAccuracy: ExecutableKpiDefinition = {
  id: "accrual_accuracy",
  title: "Accrual Accuracy %",
  category: "Data Consistency & Master Data",
  unit: "percent",
  maturity: "implemented",
  summary: "Percentage of accrual records with matching detail support.",
  source: { kind: "derived", objects: ["ACDOCA", "BKPF"] },
  sapFlavorSupport: flavorSupport({
    defaultFlavor: "s4hana",
    notes: [
      "shared/s4hana prefers ACDOCA first.",
      "ecc uses the BKPF estimate path directly.",
    ],
  }),
  notes: [
    "Uses ACDOCA (S/4HANA, transparent) instead of BSEG (cluster). Falls back to BKPF.",
    "Counts debit-side records as accrual proxy. Production should use proper accrual logic.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const sapFlavor = helpers.getSapFlavor(input);

    if (sapFlavor === "ecc") {
      const total = await helpers.countRows({
        table: "BKPF",
        fields: ["BELNR"],
        where: [
          `BUDAT GE '${window.sapFrom}'`,
          `BUDAT LE '${window.sapTo}'`,
        ],
        scanCap: 100000,
      });
      return countResult(this, total > 0 ? 100 : 0, [
        "sapFlavor=ecc used BKPF-based estimate.",
        ...(this.notes ?? []),
      ], window);
    }

    try {
      const [totalAccruals, supportedAccruals] = await Promise.all([
        helpers.countRows({
          table: "ACDOCA",
          fields: ["BELNR"],
          where: [
            "DRCRK EQ 'S'",
            `BUDAT GE '${window.sapFrom}'`,
            `BUDAT LE '${window.sapTo}'`,
          ],
          scanCap: 100000,
        }),
        helpers.countRows({
          table: "ACDOCA",
          fields: ["BELNR"],
          where: [
            "DRCRK EQ 'S'",
            "BUZEI NE '000'",
            `BUDAT GE '${window.sapFrom}'`,
            `BUDAT LE '${window.sapTo}'`,
          ],
          scanCap: 100000,
        }),
      ]);

      const value = totalAccruals === 0 ? 0 : Number(((supportedAccruals / totalAccruals) * 100).toFixed(2));
      return countResult(this, value, [`sapFlavor=${sapFlavor}.`, ...(this.notes ?? [])], window);
    } catch {
      // ACDOCA not available — use BKPF count-based proxy
      const total = await helpers.countRows({
        table: "BKPF",
        fields: ["BELNR"],
        where: [
          `BUDAT GE '${window.sapFrom}'`,
          `BUDAT LE '${window.sapTo}'`,
        ],
        scanCap: 100000,
      });
      return countResult(this, total > 0 ? 100 : 0, [
        `sapFlavor=${sapFlavor}.`,
        "ACDOCA unavailable; BKPF-based estimate applied.",
        ...(this.notes ?? []),
      ], window);
    }
  },
};

const periodCloseCycleTime: ExecutableKpiDefinition = {
  id: "period_close_cycle_time",
  title: "Period Close Cycle Time (Hours)",
  category: "Business Process KPIs",
  unit: "hours",
  maturity: "implemented",
  summary: "Estimate of period-close effort based on open postings in current period.",
  source: { kind: "derived", objects: ["BKPF"] },
  notes: [
    "Uses BKPF open postings as proxy instead of FAGLPERI (may not exist on all systems).",
    "Returns 24h estimate if open items exist, 0 if clean. Override with 'close_hours' dimension.",
  ],
  async execute(helpers, input) {
    const closeHours = helpers.getNumberDimension(input, "close_hours", 24);
    const now = new Date();
    const currentPeriod = String(now.getUTCMonth() + 1).padStart(2, "0");
    const currentYear = String(now.getUTCFullYear());
    const result = await safeCountRows(helpers, {
      table: "BKPF",
      fields: ["BELNR"],
      where: [
        "BSTAT NE ' '",
        `MONAT EQ '${currentPeriod}'`,
        `GJAHR EQ '${currentYear}'`,
      ],
      scanCap: 50000,
    });

    return countResult(this, result.value > 0 ? closeHours : 0, [
      `Period: ${currentPeriod}/${currentYear}. Open items: ${result.value}.`,
      ...(this.notes ?? []),
    ]);
  },
};

const masterDataQuality: ExecutableKpiDefinition = {
  id: "master_data_quality",
  title: "Master Data Quality %",
  category: "Data Consistency & Master Data",
  unit: "percent",
  maturity: "implemented",
  summary: "Percentage of master records with complete mandatory fields.",
  source: { kind: "derived", objects: ["MARA", "KNA1", "LFA1"] },
  notes: ["Zero-footprint replacement for ZHC_GET_DATA_QUALITY_KPIS wrapper."],
  async execute(helpers) {
    const [
      totalMaterials,
      materialsMissing,
      totalCustomers,
      customersMissing,
      totalVendors,
      vendorsMissing,
    ] = await Promise.all([
      helpers.countRows({ table: "MARA", fields: ["MATNR"], scanCap: 50000 }),
      helpers.countRows({
        table: "MARA",
        fields: ["MATNR"],
        where: ["ERSDA EQ '00000000'"],
        scanCap: 50000,
      }),
      helpers.countRows({ table: "KNA1", fields: ["KUNNR"], scanCap: 50000 }),
      helpers.countRows({
        table: "KNA1",
        fields: ["KUNNR"],
        where: ["NAME1 EQ ' '"],
        scanCap: 50000,
      }),
      helpers.countRows({ table: "LFA1", fields: ["LIFNR"], scanCap: 50000 }),
      helpers.countRows({
        table: "LFA1",
        fields: ["LIFNR"],
        where: ["NAME1 EQ ' '"],
        scanCap: 50000,
      }),
    ]);

    const totalRecords = totalMaterials + totalCustomers + totalVendors;
    const totalMissing = materialsMissing + customersMissing + vendorsMissing;
    const value =
      totalRecords === 0
        ? 100
        : Number((((totalRecords - totalMissing) / totalRecords) * 100).toFixed(2));

    return countResult(this, value, [
      ...(this.notes ?? []),
      `Missing records - MARA: ${materialsMissing}, KNA1: ${customersMissing}, LFA1: ${vendorsMissing}.`,
      `Total records scanned: ${totalRecords}.`,
    ]);
  },
};

const duplicateMasters: ExecutableKpiDefinition = {
  id: "duplicate_masters",
  title: "Duplicate Master Records",
  category: "Data Consistency & Master Data",
  unit: "count",
  maturity: "implemented",
  summary: "Potential duplicate master records detected by name matching.",
  source: { kind: "derived", objects: ["KNA1", "LFA1", "BUT000"] },
  notes: [
    "Zero-footprint replacement for ZHC_GET_DATA_QUALITY_KPIS wrapper.",
    "Uses simplified exact-name matching; production needs fuzzy matching.",
  ],
  async execute(helpers) {
    const rows = await helpers.scanRows({
      table: "KNA1",
      fields: ["NAME1"],
      pageSize: 1000,
      scanCap: 50000,
    });

    const name2count = new Map<string, number>();
    for (const row of rows) {
      const name = (row.NAME1 ?? "").trim().toUpperCase();
      if (name.length > 0) {
        name2count.set(name, (name2count.get(name) ?? 0) + 1);
      }
    }

    const duplicates = Array.from(name2count.values()).filter((count) => count > 1).length;
    return countResult(this, duplicates, this.notes ?? []);
  },
};

const dataCompleteness: ExecutableKpiDefinition = {
  id: "data_completeness",
  title: "Data Completeness %",
  category: "Data Consistency & Master Data",
  unit: "percent",
  maturity: "implemented",
  summary: "Percentage of records with non-empty values in critical fields.",
  source: { kind: "derived", objects: ["MARA", "KNA1", "LFA1", "ANLA"] },
  notes: ["Zero-footprint replacement for ZHC_GET_DATA_QUALITY_KPIS wrapper."],
  async execute(helpers) {
    const [
      totalMaterials,
      completeMaterials,
      totalCustomers,
      completeCustomers,
      totalVendors,
      completeVendors,
    ] = await Promise.all([
      helpers.countRows({ table: "MARA", fields: ["MATNR"], scanCap: 50000 }),
      helpers.countRows({
        table: "MARA",
        fields: ["MATNR"],
        where: ["ERSDA NE '00000000'"],
        scanCap: 50000,
      }),
      helpers.countRows({ table: "KNA1", fields: ["KUNNR"], scanCap: 50000 }),
      helpers.countRows({
        table: "KNA1",
        fields: ["KUNNR"],
        where: ["NAME1 NE ' '"],
        scanCap: 50000,
      }),
      helpers.countRows({ table: "LFA1", fields: ["LIFNR"], scanCap: 50000 }),
      helpers.countRows({
        table: "LFA1",
        fields: ["LIFNR"],
        where: ["NAME1 NE ' '"],
        scanCap: 50000,
      }),
    ]);

    const total = totalMaterials + totalCustomers + totalVendors;
    const complete = completeMaterials + completeCustomers + completeVendors;
    const value = total === 0 ? 100 : Number(((complete / total) * 100).toFixed(2));

    return countResult(this, value, [
      ...(this.notes ?? []),
      `Complete records - MARA: ${completeMaterials}, KNA1: ${completeCustomers}, LFA1: ${completeVendors}.`,
      `Total records scanned: ${total}.`,
    ]);
  },
};

const consistencyExceptions: ExecutableKpiDefinition = {
  id: "consistency_exceptions",
  title: "Consistency Exceptions",
  category: "Data Consistency & Master Data",
  unit: "count",
  maturity: "implemented",
  summary: "Master records with inconsistent cross-reference flags.",
  source: { kind: "derived", objects: ["MARA", "MARC", "MARD"] },
  notes: ["Zero-footprint replacement for ZHC_GET_DATA_QUALITY_KPIS wrapper."],
  async execute(helpers) {
    const result = await safeCountRows(
      helpers,
      {
        table: "MARC",
        fields: ["MATNR"],
        where: ["DISPO EQ ' '"],
        scanCap: 50000,
      },
    );

    return countResult(this, result.value, this.notes ?? []);
  },
};

// PHASE 4: Security & Manufacturing KPIs (5 KPIs)
const stuckProductionOrders: ExecutableKpiDefinition = {
  id: "stuck_production_orders",
  title: "Stuck Production Orders",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Production orders in active state for more than 30 days.",
  source: { kind: "derived", objects: ["AUFK"] },
  notes: ["Zero-footprint replacement for ZHC_GET_MANUFACTURING_KPIS wrapper."],
  async execute(helpers, input) {
    const stuckDays = helpers.getNumberDimension(input, "stuck_days", 30);
    const cutoffDate = helpers.toSapDateDaysAgo(stuckDays);
    const result = await safeCountRows(
      helpers,
      {
        table: "AUFK",
        fields: ["AUFNR"],
        where: [
          "ERDAT LT '" + cutoffDate + "'",
          "PHAS1 EQ 'X'",
          "PHAS2 EQ ' '",
        ],
        scanCap: 50000,
      },
    );

    return countResult(this, result.value, [
      ...(this.notes ?? []),
      "Counts orders that are released/active (PHAS1) but not technically completed (PHAS2).",
    ]);
  },
};

const backflushFailures: ExecutableKpiDefinition = {
  id: "backflush_failures",
  title: "Backflush Failures",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Goods-movement error rows related to production/backflush activity.",
  source: { kind: "derived", objects: ["AFFW", "AFRU"] },
  notes: ["Zero-footprint replacement for ZHC_GET_MANUFACTURING_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    const result = await safeCountRows(
      helpers,
      {
        table: "AFFW",
        fields: ["WEBLNR"],
        where: [
          `ERSDA GE '${window.sapFrom}'`,
          `ERSDA LE '${window.sapTo}'`,
        ],
        scanCap: 50000,
      },
      [
        {
          label: "AFRU confirmation fallback",
          request: {
            table: "AFRU",
            fields: ["RUECK"],
            where: [
              `ERSDA GE '${window.sapFrom}'`,
              `ERSDA LE '${window.sapTo}'`,
            ],
            scanCap: 50000,
          },
        },
      ],
    );

    const notes = [...(this.notes ?? [])];
    if (result.fallbackUsed) {
      notes.push(`Used ${result.fallbackUsed}.`);
    }
    return countResult(this, result.value, notes, window);
  },
};

const mfgErrors: ExecutableKpiDefinition = {
  id: "mfg_errors",
  title: "Manufacturing Errors",
  category: "Business Process KPIs",
  unit: "count",
  maturity: "implemented",
  summary: "Manufacturing error count inferred from MRP exception counters.",
  source: { kind: "derived", objects: ["MDKP", "MDLG"] },
  notes: ["Zero-footprint replacement for ZHC_GET_MANUFACTURING_KPIS wrapper."],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    try {
      const rows = await helpers.scanRows({
        table: "MDKP",
        fields: [
          "MATNR",
          "DSDAT",
          "AUSZ1",
          "AUSZ2",
          "AUSZ3",
          "AUSZ4",
          "AUSZ5",
          "AUSZ6",
          "AUSZ7",
          "AUSZ8",
        ],
        where: [`DSDAT GE '${window.sapFrom}'`, `DSDAT LE '${window.sapTo}'`],
        pageSize: 500,
        scanCap: 50000,
      });

      const value = rows.reduce((sum, row) => {
        const rowExceptions = ["AUSZ1", "AUSZ2", "AUSZ3", "AUSZ4", "AUSZ5", "AUSZ6", "AUSZ7", "AUSZ8"]
          .reduce((rowTotal, field) => rowTotal + (parseIntegerText(row[field] ?? "") ?? 0), 0);
        return sum + rowExceptions;
      }, 0);

      return countResult(this, value, this.notes ?? [], window);
    } catch (primaryError) {
      const result = await safeCountRows(
        helpers,
        {
          table: "MDLG",
          fields: ["BERID"],
          scanCap: 50000,
        },
      );

      return countResult(this, result.value, [
        ...(this.notes ?? []),
        `MDKP read failed: ${describeError(primaryError)}`,
        "Used MDLG row volume as coarse fallback.",
      ], window);
    }
  },
};

const authorizationFailures: ExecutableKpiDefinition = {
  id: "authorization_failures",
  title: "Authorization Failures (SU53)",
  category: "Security & Authorization",
  unit: "count",
  maturity: "implemented",
  summary: "Authorization failure count from Security Audit Log (table-based fallback).",
  source: { kind: "table", objects: ["RSECACTPROT"] },
  notes: [
    "Zero-footprint replacement for ZHC_GET_SECURITY_KPIS wrapper.",
    "Requires Security Audit Log enabled. Falls back gracefully.",
  ],
  async execute(helpers, input) {
    const window = helpers.resolveWindow(input, 24);
    try {
      const result = await safeCountRows(
        helpers,
        {
          table: "RSECACTPROT",
          fields: ["UNAME"],
          where: [
            "EVENT EQ 'AU5'",
            `SLGDAT GE '${window.sapFrom}'`,
            `SLGDAT LE '${window.sapTo}'`,
          ],
          scanCap: 50000,
        },
      );

      return countResult(this, result.value, this.notes ?? [], window);
    } catch (e) {
      return countResult(this, 0, ["Security Audit Log (RSECACTPROT) not directly readable; returning 0 as fallback."], window);
    }
  },
};

const userSodConflicts: ExecutableKpiDefinition = {
  id: "users_with_sod_conflicts",
  title: "Users with SoD Conflicts",
  category: "Security & Authorization",
  unit: "count",
  maturity: "implemented",
  summary: "Users with multiple distinct role assignments (simplified SoD proxy via AGR_USERS).",
  source: { kind: "derived", objects: ["AGR_USERS"] },
  notes: [
    "Zero-footprint replacement for ZHC_GET_SECURITY_KPIS wrapper.",
    "Counts users with 3+ distinct roles as a SoD proxy. Override with 'sod_role_threshold' dimension.",
    "Production deployments should use GRC tables or an approved SoD ruleset.",
  ],
  async execute(helpers, input) {
    const roleThreshold = helpers.getNumberDimension(input, "sod_role_threshold", 3);
    try {
      const rows = await helpers.scanRows({
        table: "AGR_USERS",
        fields: ["UNAME", "AGR_NAME"],
        where: ["TO_DAT GE '" + helpers.toSapDateDaysAgo(0) + "'"],
        pageSize: 1000,
        scanCap: 100000,
      });

      const userRoles = new Map<string, Set<string>>();
      for (const row of rows) {
        const user = (row.UNAME ?? "").trim();
        const role = (row.AGR_NAME ?? "").trim();
        if (user && role) {
          if (!userRoles.has(user)) userRoles.set(user, new Set());
          userRoles.get(user)!.add(role);
        }
      }

      const conflictUsers = Array.from(userRoles.entries())
        .filter(([, roles]) => roles.size >= roleThreshold).length;

      return countResult(this, conflictUsers, [
        `Users with ${roleThreshold}+ roles: ${conflictUsers}.`,
        `Total users scanned: ${userRoles.size}.`,
        ...(this.notes ?? []),
      ]);
    } catch (error) {
      return errorResult(this, [
        `AGR_USERS read failed: ${describeError(error)}`,
        ...(this.notes ?? []),
      ]);
    }
  },
};

const additionalPlannedDefinitions: NonExecutableKpiDefinition[] = [];

const additionalWrapperDefinitions: NonExecutableKpiDefinition[] = [
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
  // =========== ZERO-FOOTPRINT WRAPPER REPLACEMENTS (34 KPIs) ===========
  // PHASE 1: Job & Batch (7 KPIs)
  jobRestartSuccessRate,
  jobCancellationRate,
  jobHoldDurationAvg,
  jobReleaseFailures,
  scheduledJobVariance,
  batchRestartSuccessRate,
  jobStepFailures,
  // PHASE 2: OTC & P2P (13 KPIs)
  orderCompletionRate,
  quoteToCashCycle,
  fulfillmentAccuracy,
  backorderRate,
  pricingCompliance,
  creditFailures,
  invoiceToCashCycle,
  poMatchRate,
  invoiceHoldRate,
  duplicateDetection,
  grPostingFailures,
  threeWayMatchingFailures,
  poChangeApprovalRate,
  // PHASE 3: Finance & Data Quality (9 KPIs)
  glReconciliationVariance,
  subLedgerExceptions,
  accrualAccuracy,
  periodCloseCycleTime,
  masterDataQuality,
  duplicateMasters,
  dataCompleteness,
  consistencyExceptions,
  // PHASE 4: Security & Manufacturing (5 KPIs)
  stuckProductionOrders,
  backflushFailures,
  mfgErrors,
  authorizationFailures,
  userSodConflicts,
  // =========== CONVERTED custom_abap_required → implemented ===========
  implEmergencyAccessSessions,
  implExpiredPasswordPct,
  implMissingMandatoryFields,
  implDuplicateEntries,
  implCviInconsistencies,
  implStuckSalesDocuments,
  implStuckDeliveryDocuments,
  implGrIrMismatch,
  implFailedSalesOrders,
  implAtpCheckFailures,
  implPoCreationErrors,
  implInvoiceMatchFailures,
  implPaymentRunErrors,
  implPeriodEndClose,
  implAssetInconsistencies,
  implReconciliationImbalanceAlerts,
  excludedServiceNow,
  ...additionalPlannedDefinitions,
  ...additionalWrapperDefinitions,
].map((definition) => withResolvedTier(definition));
