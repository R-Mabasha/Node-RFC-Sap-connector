// ---------------------------------------------------------------------------
// executor.ts — KPI execution engine.
//
// Responsibilities:
//   1. Resolves KPI IDs against the registry.
//   2. Delegates to the correct execution path:
//      - Executable KPIs → call their execute() method with helpers.
//      - Wrapper-backed KPIs → invoke the ZHC_* RFC and parse the response.
//      - Planned/excluded KPIs → return metadata-only results.
//   3. Provides helpers (countRows, scanRows, callFunction, etc.) to KPI definitions.
//   4. Caches wrapper RFC responses within a single runMany() batch
//      to avoid redundant calls (multiple KPIs share the same wrapper FM).
// ---------------------------------------------------------------------------

import type { KpiRequestInput, KpiResult, SapClient } from "../types.js";
import {
  daysSinceSapDate,
  getNumberDimension,
  parseSapDateTime,
  resolveWindow,
  toSapDateDaysAgo,
} from "../utils/dates.js";
import {
  KPI_DEFINITIONS,
  type CountRowsRequest,
  type ExecutableKpiDefinition,
  type KpiDefinition,
  type KpiExecutionHelpers,
  type NonExecutableKpiDefinition,
  type ScanRowsRequest,
} from "./definitions.js";
import {
  buildWrapperCallParameters,
  isWrapperBackedDefinition,
  parseWrapperResponse,
} from "../wrappers/catalog.js";
import { describeError } from "../utils/errors.js";

// ── Constants ──────────────────────────────────────────────────────────────

/** Default page size for paginated table reads. */
const DEFAULT_PAGE_SIZE = 500;

/** Maximum total rows to scan before aborting (prevents runaway queries). */
const DEFAULT_SCAN_CAP = 10000;

/** Batch execution is intentionally serialized to avoid BBP_RFC_READ_TABLE contention. */
const DEFAULT_RUN_MANY_CONCURRENCY = 1;

// ── Type guards ────────────────────────────────────────────────────────────

function isExecutable(
  definition: KpiDefinition,
): definition is ExecutableKpiDefinition {
  return definition.maturity === "implemented";
}

// ── Main KPI executor ──────────────────────────────────────────────────────

export class KpiExecutor {
  /** Fast lookup of definitions by their unique KPI ID. */
  private readonly definitionsById = new Map(
    KPI_DEFINITIONS.map((definition) => [definition.id, definition]),
  );

  /**
   * Helpers injected into every executable KPI's execute() method.
   * These abstract away the SAP client and utility functions so that
   * KPI definitions stay focused on business logic only.
   */
  private readonly helpers: KpiExecutionHelpers = {
    // -- Table read helpers --
    countRows: (request) => this.countRows(request),
    scanRows: (request) => this.scanRows(request),

    // -- RFC function call helper (for standard SAP FMs like TH_WPINFO) --
    callFunction: (functionName, parameters) =>
      this.sapClient.call(functionName, parameters),

    // -- Date/dimension helpers --
    resolveWindow: (input, fallbackHours) =>
      resolveWindow(input, fallbackHours),
    getNumberDimension: (input, key, defaultValue) =>
      getNumberDimension(input, key, defaultValue),
    toSapDateDaysAgo: (days) => toSapDateDaysAgo(days),
    parseSapDateTime: (dateValue, timeValue) =>
      parseSapDateTime(dateValue, timeValue),
    daysSinceSapDate: (dateValue) => daysSinceSapDate(dateValue),
  };

  constructor(private readonly sapClient: SapClient) {}

  // ── Public API ─────────────────────────────────────────────────────────

  /** Return the full KPI registry (all definitions regardless of maturity). */
  listDefinitions(): KpiDefinition[] {
    return KPI_DEFINITIONS;
  }

  /**
   * Execute multiple KPIs in parallel.
   * Wrapper responses are cached within the batch to avoid duplicate RFC calls
   * when multiple KPIs share the same ZHC_* wrapper function module.
   */
  async runMany(kpiIds: string[], input: KpiRequestInput): Promise<KpiResult[]> {
    const wrapperCache = new Map<string, Promise<Map<string, KpiResult>>>();
    const results: KpiResult[] = [];

    for (let index = 0; index < kpiIds.length; index += DEFAULT_RUN_MANY_CONCURRENCY) {
      const batch = kpiIds.slice(index, index + DEFAULT_RUN_MANY_CONCURRENCY);
      const batchResults = await Promise.all(
        batch.map((kpiId) => this.runWithWrapperCache(kpiId, input, wrapperCache)),
      );

      results.push(...batchResults);
    }

    return results;
  }

  /** Execute a single KPI. */
  async run(kpiId: string, input: KpiRequestInput): Promise<KpiResult> {
    return this.runWithWrapperCache(kpiId, input, new Map());
  }

  // ── Execution dispatcher ───────────────────────────────────────────────

  private async runWithWrapperCache(
    kpiId: string,
    input: KpiRequestInput,
    wrapperCache: Map<string, Promise<Map<string, KpiResult>>>,
  ): Promise<KpiResult> {
    const definition = this.definitionsById.get(kpiId);

    // Unknown KPI ID → error result
    if (!definition) {
      return {
        kpiId,
        title: kpiId,
        category: "Unknown",
        status: "error",
        source: { kind: "derived", objects: [] },
        notes: [`KPI '${kpiId}' is not registered.`],
      };
    }

    // Path 1: Wrapper-backed KPI → invoke ZHC_* RFC
    if (isWrapperBackedDefinition(definition)) {
      try {
        const resultsByKpiId = await this.getWrapperResults(
          definition.wrapper.functionName,
          input,
          wrapperCache,
        );
        const wrapped = resultsByKpiId.get(definition.wrapper.wrapperKpiId);

        if (wrapped) {
          return {
            ...wrapped,
            kpiId: definition.id,
            title: definition.title,
            category: definition.category,
            unit: wrapped.unit ?? definition.unit,
            tier: definition.tier,
          };
        }

        return {
          kpiId: definition.id,
          title: definition.title,
          category: definition.category,
          status: definition.maturity,
          unit: definition.unit,
          tier: definition.tier,
          source: definition.source,
          notes: [
            definition.blocker,
            `Wrapper '${definition.wrapper.functionName}' returned no row for KPI '${definition.wrapper.wrapperKpiId}'.`,
            ...(definition.notes ?? []),
          ],
        };
      } catch (error) {
        return {
          kpiId: definition.id,
          title: definition.title,
          category: definition.category,
          status: definition.maturity,
          unit: definition.unit,
          tier: definition.tier,
          source: definition.source,
          notes: [
            definition.blocker,
            `Wrapper '${definition.wrapper.functionName}' invocation failed: ${describeError(error)}`,
            ...(definition.notes ?? []),
          ],
        };
      }
    }

    // Path 2: Planned/excluded KPI → return metadata-only (no execute)
    if (!isExecutable(definition)) {
      return {
        kpiId: definition.id,
        title: definition.title,
        category: definition.category,
        status: definition.maturity,
        unit: definition.unit,
        tier: definition.tier,
        source: definition.source,
        notes: [definition.blocker, ...(definition.notes ?? [])],
      };
    }

    // Path 3: Implemented KPI → call its execute() method
    try {
      return await definition.execute(this.helpers, input);
    } catch (error) {
      return {
        kpiId: definition.id,
        title: definition.title,
        category: definition.category,
        status: "error",
        unit: definition.unit,
        tier: definition.tier,
        source: definition.source,
        notes: [describeError(error)],
      };
    }
  }

  // ── Wrapper caching ────────────────────────────────────────────────────

  /**
   * Get or create a cached promise for a specific wrapper FM + input combination.
   * When multiple KPIs share the same wrapper (e.g., 4 security KPIs all use
   * ZHC_GET_SECURITY_KPIS), we call the wrapper exactly once per batch.
   */
  private getWrapperResults(
    functionName: string,
    input: KpiRequestInput,
    wrapperCache: Map<string, Promise<Map<string, KpiResult>>>,
  ): Promise<Map<string, KpiResult>> {
    const cacheKey = JSON.stringify({
      functionName,
      from: input.from,
      to: input.to,
      dimensions: input.dimensions ?? {},
    });

    const cached = wrapperCache.get(cacheKey);
    if (cached) {
      return cached;
    }

    const promise = this.callWrapper(functionName, input);
    wrapperCache.set(cacheKey, promise);
    return promise;
  }

  private async callWrapper(
    functionName: string,
    input: KpiRequestInput,
  ): Promise<Map<string, KpiResult>> {
    const window = resolveWindow(input, 24);
    const rawResult = await this.sapClient.call(
      functionName,
      buildWrapperCallParameters(input),
    );

    return parseWrapperResponse(functionName, rawResult, window).resultsByKpiId;
  }

  // ── Table read helpers ─────────────────────────────────────────────────

  /**
   * Count rows matching a WHERE clause using paginated table reads.
   * Paginates through the result set using rowSkips to handle large tables.
   * Throws if the scan cap is exceeded (signals that a wrapper is needed).
   */
  private async countRows(request: CountRowsRequest): Promise<number> {
    let total = 0;
    let rowSkips = 0;
    const pageSize = request.pageSize ?? DEFAULT_PAGE_SIZE;
    const scanCap = request.scanCap ?? DEFAULT_SCAN_CAP;

    while (true) {
      if (rowSkips >= scanCap) {
        throw new Error(
          `Scan cap reached while counting ${request.table}. Replace this KPI with a custom RFC wrapper.`,
        );
      }

      const result = await this.sapClient.readTable({
        table: request.table,
        fields: request.fields,
        where: request.where,
        rowCount: pageSize,
        rowSkips,
      });

      total += result.rows.length;
      rowSkips += result.rows.length;

      if (result.rows.length < pageSize) {
        return total;
      }
    }
  }

  /**
   * Return all row data matching a WHERE clause using paginated table reads.
   * Used when the KPI needs to inspect individual row values (e.g., durations, statuses).
   * Throws if the scan cap is exceeded.
   */
  private async scanRows(
    request: ScanRowsRequest,
  ): Promise<Array<Record<string, string>>> {
    const rows: Array<Record<string, string>> = [];
    let rowSkips = 0;
    const pageSize = request.pageSize ?? DEFAULT_PAGE_SIZE;
    const scanCap = request.scanCap ?? DEFAULT_SCAN_CAP;

    while (true) {
      if (rows.length >= scanCap) {
        throw new Error(
          `Scan cap reached while reading ${request.table}. Replace this KPI with a custom RFC wrapper.`,
        );
      }

      const result = await this.sapClient.readTable({
        table: request.table,
        fields: request.fields,
        where: request.where,
        rowCount: pageSize,
        rowSkips,
      });

      rows.push(...result.rows);
      rowSkips += result.rows.length;

      if (result.rows.length < pageSize) {
        return rows;
      }
    }
  }
}
