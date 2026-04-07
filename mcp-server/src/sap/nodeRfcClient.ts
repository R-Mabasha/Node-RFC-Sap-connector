// ---------------------------------------------------------------------------
// nodeRfcClient.ts — SAP RFC client with connection pooling, circuit breaker,
// allowlist enforcement, and table-reader fallback chain.
//
// Connection lifecycle:
//   1. Pool is created lazily on first call.
//   2. Each call acquires a connection, executes, and releases.
//   3. Circuit breaker opens after N consecutive failures,
//      preventing further calls until reset timeout expires.
//   4. Pool is closed on server shutdown via close().
//
// Security model:
//   - Only allowlisted function modules can be called.
//   - Only allowlisted tables can be read.
//   - Row counts are capped to prevent memory exhaustion.
// ---------------------------------------------------------------------------

import type { SapClient, SapConfig, SapDiagnostics, TableReadRequest, TableReadResult } from "../types.js";
import {
  describeError,
  getSapErrorKey,
  isBusyResourceError,
  shouldTripCircuitBreaker,
} from "../utils/errors.js";

// ── node-rfc type stubs (avoids hard compile-time dependency) ──────────────

type NodeRfcPool = {
  acquire(): Promise<NodeRfcHandle>;
  closeAll(): Promise<void>;
};

type NodeRfcHandle = {
  ping(): Promise<boolean>;
  call(
    functionName: string,
    parameters: Record<string, unknown>,
  ): Promise<Record<string, unknown>>;
  release(): Promise<void>;
};

type NodeRfcModule = {
  Pool: new (configuration: {
    connectionParameters: Record<string, string>;
    clientOptions?: {
      timeout?: number;
      stateless?: boolean;
    };
    poolOptions?: {
      low: number;
      high: number;
    };
  }) => NodeRfcPool;
};

// ── Constants ──────────────────────────────────────────────────────────────

/** Maximum rows returned from a single table-read call. */
const MAX_ROWCOUNT = 1000;

/** Default field delimiter for table-read parsing. */
const DEFAULT_DELIMITER = "|";

/** Retry delays for transient "device or resource busy" SAP errors. */
const BUSY_RETRY_DELAYS_MS = [250, 750];

// ── Circuit breaker ────────────────────────────────────────────────────────

type CircuitBreakerState = "closed" | "open" | "half-open";

interface CircuitBreaker {
  state: CircuitBreakerState;
  consecutiveFailures: number;
  lastFailureTime: number;
  /** Consecutive failures needed to trip the breaker. */
  threshold: number;
  /** Milliseconds to wait before moving from open → half-open. */
  resetMs: number;
}

function createCircuitBreaker(threshold: number, resetMs: number): CircuitBreaker {
  return {
    state: "closed",
    consecutiveFailures: 0,
    lastFailureTime: 0,
    threshold,
    resetMs,
  };
}

/**
 * Check if the breaker allows a call to proceed.
 * If open, check if enough time has passed to move to half-open.
 */
function canCallThrough(breaker: CircuitBreaker): boolean {
  if (breaker.state === "closed") {
    return true;
  }

  if (breaker.state === "open") {
    const elapsed = Date.now() - breaker.lastFailureTime;
    if (elapsed >= breaker.resetMs) {
      // Transition to half-open: allow one probe call
      breaker.state = "half-open";
      return true;
    }
    return false;
  }

  // half-open: allow exactly one probe call (already transitioned)
  return true;
}

/** Record a successful call — resets the breaker to closed. */
function recordSuccess(breaker: CircuitBreaker): void {
  breaker.state = "closed";
  breaker.consecutiveFailures = 0;
}

/** Record a failed call — increments failures and may trip the breaker. */
function recordFailure(breaker: CircuitBreaker): void {
  breaker.consecutiveFailures += 1;
  breaker.lastFailureTime = Date.now();

  if (breaker.consecutiveFailures >= breaker.threshold) {
    breaker.state = "open";
  }
}

// ── Helpers ────────────────────────────────────────────────────────────────

function normalizeIdentifier(value: string): string {
  return value.trim().toUpperCase();
}

/** Extract the DATA array from an RFC_READ_TABLE-style response. */
function extractDataRows(result: Record<string, unknown>): unknown[] {
  const preferredKeys = ["DATA", "ET_DATA", "OUT_DATA", "RESULT_DATA"];

  for (const key of preferredKeys) {
    const value = result[key];
    if (Array.isArray(value)) {
      return value;
    }
  }

  for (const [key, value] of Object.entries(result)) {
    if (/DATA/i.test(key) && Array.isArray(value)) {
      return value;
    }
  }

  return [];
}

/** Extract the text content from a single DATA row entry. */
function extractRowText(entry: unknown): string {
  if (typeof entry === "string") {
    return entry;
  }

  if (!entry || typeof entry !== "object") {
    return "";
  }

  const record = entry as Record<string, unknown>;
  const candidateKeys = ["WA", "LINE", "DATA", "TABLINE", "ROW"];

  for (const key of candidateKeys) {
    const value = record[key];
    if (typeof value === "string") {
      return value;
    }
  }

  return "";
}

function joinErrorMessages(errors: Array<{ functionName: string; error: unknown }>): string {
  return errors
    .map(({ functionName, error }) => `${functionName}: ${describeError(error)}`)
    .join(" | ");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function shouldCompactWhereClauses(
  functionName: string,
  where: string[] | undefined,
): boolean {
  if (!where || where.length <= 1) {
    return false;
  }

  const normalizedName = normalizeIdentifier(functionName);
  const supportsSingleClauseOnly = new Set([
    "BBP_RFC_READ_TABLE",
    "/BUI/RFC_READ_TABLE",
  ]);

  if (!supportsSingleClauseOnly.has(normalizedName)) {
    return false;
  }

  return where.every((clause) => {
    const trimmed = clause.trim().toUpperCase();

    return (
      trimmed.length > 0 &&
      !/^(AND|OR)\b/.test(trimmed) &&
      !/\b(AND|OR)$/.test(trimmed)
    );
  });
}

export function normalizeTableReadWhere(
  functionName: string,
  where: string[] | undefined,
): string[] {
  const normalized = (where ?? [])
    .map((clause) => clause.trim())
    .filter((clause) => clause.length > 0);

  if (!shouldCompactWhereClauses(functionName, normalized)) {
    return normalized;
  }

  return [normalized.join(" AND ")];
}

// ── Main client class ──────────────────────────────────────────────────────

export class NodeRfcSapClient implements SapClient {
  private readonly tableReadFunctions: string[];
  private readonly unavailableTableReadFunctions = new Set<string>();
  private poolPromise?: Promise<NodeRfcPool>;
  private activeTableReadFunction?: string;
  private tableReadResolutionPromise?: Promise<void>;
  private tableReadTail: Promise<void> = Promise.resolve();

  /** Circuit breaker prevents hammering a down SAP system. */
  private readonly breaker: CircuitBreaker;

  /** Simple call-count metrics for diagnostics. */
  private totalCalls = 0;
  private totalFailures = 0;

  constructor(private readonly config: SapConfig) {
    this.tableReadFunctions = config.tableReadFunctions.map(normalizeIdentifier);
    this.breaker = createCircuitBreaker(
      config.circuitBreakerThreshold,
      config.circuitBreakerResetMs,
    );
  }

  // ── Public interface ───────────────────────────────────────────────────

  async ping(): Promise<boolean> {
    if (!this.config.connectionParameters) {
      return false;
    }

    const result = await this.withCircuitBreaker(() =>
      this.withClient((client) => client.ping()),
    );

    if (result !== true) {
      throw new Error("RFC ping returned false.");
    }

    return true;
  }

  async call(
    functionName: string,
    parameters: Record<string, unknown> = {},
  ): Promise<Record<string, unknown>> {
    this.requireConnectionParameters();

    const normalizedName = normalizeIdentifier(functionName);

    return this.withCircuitBreaker(() =>
      this.withClient((client) => client.call(normalizedName, parameters)),
    );
  }

  getDiagnostics(): SapDiagnostics {
    return {
      tableReadFunctions: this.tableReadFunctions,
      activeTableReadFunction: this.activeTableReadFunction,
      circuitBreakerState: this.breaker.state,
      totalCalls: this.totalCalls,
      totalFailures: this.totalFailures,
    };
  }

  async readTable(request: TableReadRequest): Promise<TableReadResult> {
    this.requireConnectionParameters();

    const table = normalizeIdentifier(request.table);
    const fields = request.fields.map(normalizeIdentifier);
    const rowCount = Math.min(Math.max(request.rowCount ?? 200, 1), MAX_ROWCOUNT);
    const rowSkips = Math.max(request.rowSkips ?? 0, 0);
    const delimiter = request.delimiter ?? DEFAULT_DELIMITER;
    const requestedReader = request.readerFunction
      ? normalizeIdentifier(request.readerFunction)
      : undefined;

    if (requestedReader && !this.tableReadFunctions.includes(requestedReader)) {
      throw new Error(
        `Table-reader function '${requestedReader}' is not configured for this MCP server.`,
      );
    }

    if (!requestedReader) {
      await this.ensureResolvedTableReadFunction();
    }

    const functionCandidates = requestedReader
      ? [requestedReader]
      : this.getOrderedTableReadFunctions();
    const errors: Array<{ functionName: string; error: unknown }> = [];

    for (const functionName of functionCandidates) {
      try {
        const result = await this.callTableReader(functionName, {
          table,
          fields,
          where: request.where,
          rowCount,
          rowSkips,
          delimiter,
        });
        const rows = this.parseTableRows(result, fields, delimiter);

        this.activeTableReadFunction = functionName;

        return {
          table,
          fields,
          rows,
          rowCount: rows.length,
          truncated: rows.length === rowCount,
          readerFunction: functionName,
        };
      } catch (error) {
        if (getSapErrorKey(error) === "FU_NOT_FOUND") {
          this.unavailableTableReadFunctions.add(functionName);
        }
        errors.push({ functionName, error });
      }
    }

    throw new Error(
      `Unable to read table '${table}' with the configured table-reader functions. ${joinErrorMessages(
        errors,
      )}`,
    );
  }

  async close(): Promise<void> {
    if (!this.poolPromise) {
      return;
    }

    const pool = await this.poolPromise;
    await pool.closeAll();
    this.poolPromise = undefined;
  }

  // ── Circuit breaker wrapper ────────────────────────────────────────────

  private async withCircuitBreaker<T>(fn: () => Promise<T>): Promise<T> {
    if (!canCallThrough(this.breaker)) {
      const waitSec = Math.ceil(
        (this.breaker.resetMs - (Date.now() - this.breaker.lastFailureTime)) / 1000,
      );
      throw new Error(
        `Circuit breaker is OPEN after ${this.breaker.consecutiveFailures} consecutive failures. ` +
        `Retry in ~${waitSec}s. SAP may be unreachable.`,
      );
    }

    this.totalCalls += 1;

    try {
      const result = await fn();
      recordSuccess(this.breaker);
      return result;
    } catch (error) {
      this.totalFailures += 1;
      if (shouldTripCircuitBreaker(error)) {
        recordFailure(this.breaker);
      } else {
        recordSuccess(this.breaker);
      }
      throw error;
    }
  }

  private async withTransientRetries<T>(fn: () => Promise<T>): Promise<T> {
    let lastError: unknown;

    for (let attempt = 0; attempt <= BUSY_RETRY_DELAYS_MS.length; attempt += 1) {
      try {
        return await fn();
      } catch (error) {
        lastError = error;

        if (
          !isBusyResourceError(error) ||
          attempt === BUSY_RETRY_DELAYS_MS.length
        ) {
          throw error;
        }

        await sleep(BUSY_RETRY_DELAYS_MS[attempt] ?? 0);
      }
    }

    throw lastError;
  }

  // ── Pool management ────────────────────────────────────────────────────

  private async getPool(): Promise<NodeRfcPool> {
    this.requireConnectionParameters();

    if (!this.poolPromise) {
      this.poolPromise = this.createPool();
    }

    return this.poolPromise;
  }

  private async createPool(): Promise<NodeRfcPool> {
    const connectionParameters = this.requireConnectionParameters();
    const moduleName = "node-rfc";

    let nodeRfc: NodeRfcModule;

    try {
      nodeRfc = (await import(moduleName)) as unknown as NodeRfcModule;
    } catch (error) {
      throw new Error(
        `Unable to load node-rfc. Install node-rfc after the SAP NW RFC SDK is available. Root cause: ${describeError(
          error,
        )}`,
      );
    }

    return new nodeRfc.Pool({
      connectionParameters,
      clientOptions: {
        timeout: this.config.timeoutMs,
        stateless: true,
      },
      poolOptions: {
        low: this.config.poolLow,
        high: this.config.poolHigh,
      },
    });
  }

  private requireConnectionParameters(): Record<string, string> {
    if (!this.config.connectionParameters) {
      throw new Error(
        "SAP connection is not configured. Set SAP_DEST or provide direct SAP_* connection variables.",
      );
    }

    return this.config.connectionParameters;
  }

  // ── Table-reader internals ─────────────────────────────────────────────

  private getOrderedTableReadFunctions(): string[] {
    const candidates = this.tableReadFunctions.filter(
      (functionName) => !this.unavailableTableReadFunctions.has(functionName),
    );

    if (this.activeTableReadFunction && candidates.includes(this.activeTableReadFunction)) {
      return [
        this.activeTableReadFunction,
        ...candidates.filter(
          (functionName) => functionName !== this.activeTableReadFunction,
        ),
      ];
    }

    return candidates;
  }

  private async ensureResolvedTableReadFunction(): Promise<void> {
    if (this.activeTableReadFunction || this.tableReadFunctions.length <= 1) {
      return;
    }

    if (!this.tableReadResolutionPromise) {
      this.tableReadResolutionPromise = this.resolveTableReadFunction();
    }

    try {
      await this.tableReadResolutionPromise;
    } finally {
      this.tableReadResolutionPromise = undefined;
    }
  }

  private async resolveTableReadFunction(): Promise<void> {
    const probe = this.getTableReadProbeRequest();

    if (!probe) {
      return;
    }

    const candidates = this.getOrderedTableReadFunctions();

    for (const functionName of candidates) {
      try {
        await this.callTableReader(functionName, probe);
        this.activeTableReadFunction = functionName;
        return;
      } catch (error) {
        if (getSapErrorKey(error) === "FU_NOT_FOUND") {
          this.unavailableTableReadFunctions.add(functionName);
        }
      }
    }
  }

  private getTableReadProbeRequest():
    | {
        table: string;
        fields: string[];
        where?: string[];
        rowCount: number;
        rowSkips: number;
        delimiter: string;
      }
    | undefined {
    // Use T000 (Client table) as universal probe - exists in all SAP systems
    return {
      table: "T000",
      fields: ["MANDT"],
      rowCount: 1,
      rowSkips: 0,
      delimiter: DEFAULT_DELIMITER,
    };
  }

  private async callTableReader(
    functionName: string,
    request: {
      table: string;
      fields: string[];
      where?: string[];
      rowCount: number;
      rowSkips: number;
      delimiter: string;
    },
  ): Promise<Record<string, unknown>> {
    if (this.getOrderedTableReadFunctions().length === 0) {
      throw new Error(
        "No SAP table-reader functions are configured. Set SAP_TABLE_READ_FUNCTIONS.",
      );
    }

    const normalizedWhere = normalizeTableReadWhere(
      functionName,
      request.where,
    );

    return this.withSerializedTableRead(() =>
      this.withCircuitBreaker(() =>
        this.withTransientRetries(() =>
          this.withClient((client) =>
            client.call(functionName, {
              QUERY_TABLE: request.table,
              DELIMITER: request.delimiter,
              ROWCOUNT: request.rowCount,
              ROWSKIPS: request.rowSkips,
              FIELDS: request.fields.map((field) => ({ FIELDNAME: field })),
              OPTIONS: normalizedWhere.map((line) => ({ TEXT: line })),
            }),
          ),
        ),
      ),
    );
  }

  private parseTableRows(
    result: Record<string, unknown>,
    fields: string[],
    delimiter: string,
  ): Array<Record<string, string>> {
    const dataRows = extractDataRows(result);

    return dataRows.map((entry) => {
      const values = extractRowText(entry).split(delimiter);

      return Object.fromEntries(
        fields.map((field, index) => [field, (values[index] ?? "").trim()]),
      );
    });
  }

  private async withClient<T>(
    fn: (client: NodeRfcHandle) => Promise<T>,
  ): Promise<T> {
    const pool = await this.getPool();
    const client = await pool.acquire();

    try {
      return await fn(client);
    } finally {
      await client.release();
    }
  }

  private async withSerializedTableRead<T>(fn: () => Promise<T>): Promise<T> {
    const previous = this.tableReadTail.catch(() => undefined);
    let release: (() => void) | undefined;

    this.tableReadTail = new Promise<void>((resolve) => {
      release = resolve;
    });

    await previous;

    try {
      return await fn();
    } finally {
      release?.();
    }
  }
}
