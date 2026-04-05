// ---------------------------------------------------------------------------
// types.ts — Central type definitions for the Hypercare SAP MCP server.
// All shared interfaces, request/response shapes, and config types live here.
// ---------------------------------------------------------------------------

// ── App-level configuration ────────────────────────────────────────────────

export interface AppConfig {
  host: string;
  port: number;
  sap: SapConfig;
}

export interface SapConfig {
  /** node-rfc connection parameters (ashost/sysnr/client/user/passwd or dest). */
  connectionParameters?: Record<string, string>;
  /** How the connection was resolved: none, destination-based, or direct. */
  connectionMode: "none" | "destination" | "direct";
  /** Filesystem paths where env values were loaded from. */
  configSources: string[];
  /** Non-fatal warnings about config (e.g., missing .env, swapped params). */
  configWarnings: string[];
  /** Minimum idle connections in the node-rfc pool. */
  poolLow: number;
  /** Maximum connections in the node-rfc pool. */
  poolHigh: number;
  /** Per-call RFC timeout in milliseconds. */
  timeoutMs: number;
  /** Allowlisted SAP tables for generic table reads. */
  allowedTables: string[];
  /** Allowlisted RFC function modules (standard + ZHC_* wrappers). */
  allowedFunctions: string[];
  /** Ordered list of table-reader FMs to try (S/4 compatible). */
  tableReadFunctions: string[];
  /** Circuit breaker: consecutive failures before opening the breaker. */
  circuitBreakerThreshold: number;
  /** Circuit breaker: milliseconds to wait before retrying after breaker opens. */
  circuitBreakerResetMs: number;
}

// ── SAP table-read request/response ────────────────────────────────────────

export interface TableReadRequest {
  table: string;
  fields: string[];
  where?: string[];
  rowCount?: number;
  rowSkips?: number;
  delimiter?: string;
  readerFunction?: string;
}

export interface TableReadResult {
  table: string;
  fields: string[];
  rows: Array<Record<string, string>>;
  rowCount: number;
  truncated: boolean;
  readerFunction: string;
}

// ── SAP client interface ───────────────────────────────────────────────────

export interface SapDiagnostics {
  tableReadFunctions: string[];
  activeTableReadFunction?: string;
  /** Circuit breaker state: closed (healthy), open (failing), half-open (probing). */
  circuitBreakerState: "closed" | "open" | "half-open";
  /** Total RFC calls made since server start. */
  totalCalls: number;
  /** Total RFC call failures since server start. */
  totalFailures: number;
}

export interface SapClient {
  ping(): Promise<boolean>;
  call(functionName: string, parameters?: Record<string, unknown>): Promise<Record<string, unknown>>;
  readTable(request: TableReadRequest): Promise<TableReadResult>;
  getDiagnostics(): SapDiagnostics;
  close(): Promise<void>;
}

// ── KPI request/response types ─────────────────────────────────────────────

export interface KpiRequestInput {
  from?: string;
  to?: string;
  dimensions?: Record<string, string>;
}

export interface ResolvedWindow {
  from: string;
  to: string;
  /** SAP-format date string YYYYMMDD for the start of the window. */
  sapFrom: string;
  /** SAP-format date string YYYYMMDD for the end of the window. */
  sapTo: string;
}

/**
 * Polling tier tells the scheduler how frequently to call this KPI.
 * - realtime: every 1-2 minutes (live snapshots like active users, work processes)
 * - frequent: every 5 minutes (response times, CPU, running jobs)
 * - batch:    every 30 minutes (job counts, IDoc counts, document volumes)
 * - daily:    once per day (password age, inactive users, growth rates)
 */
export type KpiTier = "realtime" | "frequent" | "batch" | "daily";

export type KpiStatus =
  | "ok"
  | "planned"
  | "custom_abap_required"
  | "excluded"
  | "error";

export interface KpiSource {
  /** How this KPI fetches data: table read, RFC function call, or derived logic. */
  kind: "table" | "rfc" | "derived";
  /** SAP objects involved (tables, FMs, view names). */
  objects: string[];
}

export interface KpiResult {
  kpiId: string;
  title: string;
  category: string;
  status: KpiStatus;
  unit?: string;
  value?: number;
  /** Polling tier recommendation for the scheduler. */
  tier?: KpiTier;
  window?: ResolvedWindow;
  source: KpiSource;
  notes: string[];
}
