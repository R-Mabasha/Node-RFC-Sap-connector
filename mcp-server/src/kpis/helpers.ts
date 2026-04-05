// ---------------------------------------------------------------------------
// helpers.ts — Shared types, interfaces, and helper functions for KPI
// definitions. Every KPI category file imports from this module.
// ---------------------------------------------------------------------------

import type { KpiRequestInput, KpiResult, KpiSource, KpiTier, ResolvedWindow } from "../types.js";

// ── KPI definition types ───────────────────────────────────────────────────

/** Fields shared by every KPI definition regardless of maturity. */
export interface BaseKpiDefinition {
  id: string;
  title: string;
  category: string;
  unit?: string;
  summary: string;
  source: KpiSource;
  notes?: string[];
  /** Recommended polling tier for the scheduler. */
  tier: KpiTier;
}

/** Wrapper spec: links a KPI to a custom ZHC_* RFC function module. */
export interface WrapperSpec {
  functionName: string;
  wrapperKpiId: string;
}

/** KPI with live execute() logic — maturity is "implemented". */
export interface ExecutableKpiDefinition extends BaseKpiDefinition {
  maturity: "implemented";
  execute: (helpers: KpiExecutionHelpers, input: KpiRequestInput) => Promise<KpiResult>;
}

/** KPI without execute() — planned, excluded, or needs custom ABAP wrapper. */
export interface NonExecutableKpiDefinition extends BaseKpiDefinition {
  maturity: "planned" | "excluded" | "custom_abap_required";
  blocker: string;
  wrapper?: WrapperSpec;
}

export type KpiDefinition = ExecutableKpiDefinition | NonExecutableKpiDefinition;

// ── Execution helpers interface ────────────────────────────────────────────

export interface CountRowsRequest {
  table: string;
  fields: string[];
  where?: string[];
  pageSize?: number;
  scanCap?: number;
}

export interface ScanRowsRequest {
  table: string;
  fields: string[];
  where?: string[];
  pageSize?: number;
  scanCap?: number;
}

/**
 * Helpers injected into every executable KPI's execute() method.
 * These abstract away the SAP client so definitions stay pure business logic.
 */
export interface KpiExecutionHelpers {
  /** Count rows matching a WHERE clause via paginated table reads. */
  countRows: (request: CountRowsRequest) => Promise<number>;
  /** Return all matching rows via paginated table reads. */
  scanRows: (request: ScanRowsRequest) => Promise<Array<Record<string, string>>>;
  /** Call a standard RFC function module directly (e.g., TH_WPINFO). */
  callFunction: (functionName: string, parameters?: Record<string, unknown>) => Promise<Record<string, unknown>>;
  /** Resolve from/to timestamps into a ResolvedWindow with SAP date strings. */
  resolveWindow: (input: KpiRequestInput, fallbackHours?: number) => ResolvedWindow;
  /** Extract a numeric dimension from the request or use a default. */
  getNumberDimension: (input: KpiRequestInput, key: string, defaultValue: number) => number;
  /** Return a SAP date string N days in the past. */
  toSapDateDaysAgo: (days: number) => string;
  /** Parse SAP date+time into a JS Date. */
  parseSapDateTime: (dateValue: string, timeValue: string) => Date | undefined;
  /** Calculate days since a SAP date string. */
  daysSinceSapDate: (dateValue: string) => number | undefined;
}

// ── Result builder helpers ─────────────────────────────────────────────────

/** Build a standard KpiResult from a count or percent value. */
export function countResult(
  def: ExecutableKpiDefinition,
  value: number,
  notes: string[],
  window?: ResolvedWindow,
): KpiResult {
  return {
    kpiId: def.id,
    title: def.title,
    category: def.category,
    status: "ok",
    unit: def.unit,
    value,
    tier: def.tier,
    window,
    source: def.source,
    notes,
  };
}

/** Shortcut to create a planned (not-yet-implemented) KPI definition. */
export function plannedDef(opts: {
  id: string;
  title: string;
  category: string;
  summary: string;
  source: KpiSource;
  tier: KpiTier;
  unit?: string;
  blocker: string;
}): NonExecutableKpiDefinition {
  return { ...opts, maturity: "planned", notes: [] };
}

/** Shortcut to create a wrapper-backed KPI definition. */
export function wrapperDef(opts: {
  id: string;
  title: string;
  category: string;
  summary: string;
  source: KpiSource;
  tier: KpiTier;
  unit?: string;
  blocker: string;
  functionName: string;
}): NonExecutableKpiDefinition {
  return {
    id: opts.id,
    title: opts.title,
    category: opts.category,
    unit: opts.unit,
    summary: opts.summary,
    source: opts.source,
    tier: opts.tier,
    maturity: "custom_abap_required",
    blocker: opts.blocker,
    wrapper: { functionName: opts.functionName, wrapperKpiId: opts.id },
    notes: [],
  };
}

// ── RFC response extraction helpers ────────────────────────────────────────

/** Safely extract an array table from an RFC result by key name. */
export function extractTable(result: Record<string, unknown>, keys: string[]): Array<Record<string, unknown>> {
  for (const key of keys) {
    const value = result[key];
    if (Array.isArray(value)) {
      return value.filter((e): e is Record<string, unknown> => typeof e === "object" && e !== null);
    }
  }
  return [];
}

/** Safely extract a string field from a record. */
export function extractStr(record: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const v = record[key];
    if (typeof v === "string" && v.trim().length > 0) return v.trim();
  }
  return "";
}

/** Safely extract a numeric field from a record. */
export function extractNum(record: Record<string, unknown>, keys: string[]): number {
  for (const key of keys) {
    const v = record[key];
    if (typeof v === "number" && Number.isFinite(v)) return v;
    if (typeof v === "string") { const n = Number(v); if (Number.isFinite(n)) return n; }
  }
  return 0;
}
