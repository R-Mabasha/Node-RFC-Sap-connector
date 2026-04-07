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

import type { KpiRequestInput, KpiResult, SapClient, SapFlavor } from "../types.js";
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
  formatSupportedSapFlavors,
  getRequestedSapFlavor,
  type KpiDefinition,
  type KpiExecutionHelpers,
  type NonExecutableKpiDefinition,
  type ScanRowsRequest,
  supportsSapFlavor,
} from "./definitions.js";
import {
  buildWrapperCallParameters,
  isWrapperBackedDefinition,
  parseWrapperResponse,
} from "../wrappers/catalog.js";
import {
  describeError,
  isBusyResourceError,
  isSapCapabilityError,
} from "../utils/errors.js";

// ── Constants ──────────────────────────────────────────────────────────────

/** Default page size for paginated table reads. */
const DEFAULT_PAGE_SIZE = 500;

/** Maximum total rows to scan before aborting (prevents runaway queries). */
const DEFAULT_SCAN_CAP = 10000;

/**
 * Batch execution concurrency.
 * The SAP-side table readers are contention-sensitive on this landscape, so
 * we default to sequential KPI execution and rely on TTL caching to keep
 * repeated dashboard polls inexpensive.
 */
const DEFAULT_RUN_MANY_CONCURRENCY = 1;

/** Retry delays for read-only KPI RFC calls when SAP reports transient contention. */
const READ_ONLY_RFC_RETRY_DELAYS_MS = [250, 750];

/** TTL cache per tier in milliseconds. */
const TIER_TTL_MS: Record<string, number> = {
  realtime: 60_000,       // 60s
  frequent: 300_000,      // 5min
  batch: 1_800_000,       // 30min
  daily: 86_400_000,      // 24h
};

// ── TTL result cache ───────────────────────────────────────────────────────

interface CachedResult {
  result: KpiResult;
  expiresAt: number;
}

const resultCache = new Map<string, CachedResult>();

function getCacheKey(kpiId: string, input: KpiRequestInput): string {
  return JSON.stringify({
    kpiId,
    from: input.from ?? "",
    to: input.to ?? "",
    sapFlavor: getRequestedSapFlavor(input),
    dimensions: input.dimensions ?? {},
  });
}

function getCachedResult(kpiId: string, input: KpiRequestInput): KpiResult | undefined {
  const key = getCacheKey(kpiId, input);
  const cached = resultCache.get(key);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.result;
  }
  if (cached) resultCache.delete(key);
  return undefined;
}

function setCachedResult(kpiId: string, input: KpiRequestInput, result: KpiResult, tier?: string): void {
  const ttl = TIER_TTL_MS[tier ?? "realtime"] ?? 60_000;
  const key = getCacheKey(kpiId, input);
  resultCache.set(key, { result, expiresAt: Date.now() + ttl });
}

// ── Type guards ────────────────────────────────────────────────────────────

function isExecutable(
  definition: KpiDefinition,
): definition is ExecutableKpiDefinition {
  return definition.maturity === "implemented";
}

// ── Main KPI executor ──────────────────────────────────────────────────────

/**
 * Compute window size in days from the request input.
 * Returns 1 for same-day or missing dates. Capped at 31 to prevent absurd multipliers.
 */
function computeWindowDays(input: KpiRequestInput): number {
  if (!input.from || !input.to) return 1;
  try {
    const fromMs = new Date(input.from).getTime();
    const toMs = new Date(input.to).getTime();
    if (Number.isNaN(fromMs) || Number.isNaN(toMs) || fromMs >= toMs) return 1;
    return Math.min(31, Math.max(1, Math.ceil((toMs - fromMs) / (24 * 60 * 60 * 1000))));
  } catch {
    return 1;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export class KpiExecutor {
  /**
   * Window-based scan cap multiplier.
   * Set per-request so that 7-day queries get 7x the scan cap.
   * Reset after each runMany/run call.
   */
  private scanCapMultiplier = 1;
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
      this.callReadOnlyFunction(functionName, parameters),

    // -- Date/dimension helpers --
    resolveWindow: (input, fallbackHours) =>
      resolveWindow(input, fallbackHours),
    getNumberDimension: (input, key, defaultValue) =>
      getNumberDimension(input, key, defaultValue),
    toSapDateDaysAgo: (days) => toSapDateDaysAgo(days),
    parseSapDateTime: (dateValue, timeValue) =>
      parseSapDateTime(dateValue, timeValue),
    daysSinceSapDate: (dateValue) => daysSinceSapDate(dateValue),
    getSapFlavor: (input) => getRequestedSapFlavor(input),
  };

  constructor(private readonly sapClient: SapClient) {}

  // ── Public API ─────────────────────────────────────────────────────────

  /** Return the full KPI registry (all definitions regardless of maturity). */
  listDefinitions(): KpiDefinition[] {
    return KPI_DEFINITIONS;
  }

  listDefinitionsForFlavor(sapFlavor: SapFlavor): KpiDefinition[] {
    return KPI_DEFINITIONS.filter((definition) =>
      supportsSapFlavor(definition, sapFlavor),
    );
  }

  /**
   * Execute multiple KPIs with configurable concurrency.
   * Uses TTL cache to avoid redundant RFC calls across repeated polls.
   * Wrapper responses are cached within the batch for shared ZHC_* FMs.
   */
  async runMany(kpiIds: string[], input: KpiRequestInput): Promise<KpiResult[]> {
    // Scale scan caps proportionally to the date window
    this.scanCapMultiplier = computeWindowDays(input);

    const wrapperCache = new Map<string, Promise<Map<string, KpiResult>>>();
    const results: KpiResult[] = [];

    for (let index = 0; index < kpiIds.length; index += DEFAULT_RUN_MANY_CONCURRENCY) {
      const batch = kpiIds.slice(index, index + DEFAULT_RUN_MANY_CONCURRENCY);
      const batchResults = await Promise.all(
        batch.map((kpiId) => this.runWithCache(kpiId, input, wrapperCache)),
      );

      results.push(...batchResults);
    }

    this.scanCapMultiplier = 1; // Reset after batch
    return results;
  }

  /** Execute a single KPI. */
  async run(kpiId: string, input: KpiRequestInput): Promise<KpiResult> {
    this.scanCapMultiplier = computeWindowDays(input);
    const result = await this.runWithCache(kpiId, input, new Map());
    this.scanCapMultiplier = 1;
    return result;
  }

  // ── Cache-wrapped execution ───────────────────────────────────────────

  private async callReadOnlyFunction(
    functionName: string,
    parameters?: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    let lastError: unknown;

    for (
      let attempt = 0;
      attempt <= READ_ONLY_RFC_RETRY_DELAYS_MS.length;
      attempt += 1
    ) {
      try {
        return await this.sapClient.call(functionName, parameters);
      } catch (error) {
        lastError = error;

        if (
          !isBusyResourceError(error) ||
          attempt === READ_ONLY_RFC_RETRY_DELAYS_MS.length
        ) {
          throw error;
        }

        await sleep(READ_ONLY_RFC_RETRY_DELAYS_MS[attempt] ?? 0);
      }
    }

    throw lastError;
  }

  private async runWithCache(
    kpiId: string,
    input: KpiRequestInput,
    wrapperCache: Map<string, Promise<Map<string, KpiResult>>>,
  ): Promise<KpiResult> {
    // Check TTL cache first
    const cached = getCachedResult(kpiId, input);
    if (cached) return cached;

    const result = await this.runWithWrapperCache(kpiId, input, wrapperCache);

    // Cache only healthy results so transient SAP failures can recover on retry.
    if (result.status === "ok") {
      const definition = this.definitionsById.get(kpiId);
      setCachedResult(kpiId, input, result, definition?.tier);
    }

    return result;
  }

  // ── Execution dispatcher ───────────────────────────────────────────────

  private async runWithWrapperCache(
    kpiId: string,
    input: KpiRequestInput,
    wrapperCache: Map<string, Promise<Map<string, KpiResult>>>,
  ): Promise<KpiResult> {
    const definition = this.definitionsById.get(kpiId);
    const requestedSapFlavor = getRequestedSapFlavor(input);

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

    if (!supportsSapFlavor(definition, requestedSapFlavor)) {
      return {
        kpiId: definition.id,
        title: definition.title,
        category: definition.category,
        status: "error",
        unit: definition.unit,
        tier: definition.tier,
        source: definition.source,
        notes: [
          `KPI '${definition.id}' does not support sapFlavor='${requestedSapFlavor}'.`,
          `Supported sapFlavor values: ${formatSupportedSapFlavors(definition)}.`,
        ],
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
      const description = describeError(error);
      return {
        kpiId: definition.id,
        title: definition.title,
        category: definition.category,
        status: "error",
        unit: definition.unit,
        tier: definition.tier,
        source: definition.source,
        notes: [
          isSapCapabilityError(error)
            ? `Unsupported or unavailable in this SAP system: ${description}`
            : description,
        ],
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
      sapFlavor: getRequestedSapFlavor(input),
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
   * If the scan cap is exceeded, returns the count so far (cap-and-return)
   * instead of throwing, so large date ranges degrade gracefully.
   */
  private async countRows(request: CountRowsRequest): Promise<number> {
    let total = 0;
    let rowSkips = 0;
    const pageSize = request.pageSize ?? DEFAULT_PAGE_SIZE;
    const baseCap = request.scanCap ?? DEFAULT_SCAN_CAP;
    const scanCap = baseCap * this.scanCapMultiplier;

    while (true) {
      if (rowSkips >= scanCap) {
        // Cap reached — return partial count instead of crashing
        console.warn(
          `[KPI] Scan cap (${scanCap}) reached for ${request.table}. Returning partial count: ${total}.`,
        );
        return total;
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
   * If the scan cap is exceeded, returns the rows collected so far
   * (cap-and-return) instead of throwing.
   */
  private async scanRows(
    request: ScanRowsRequest,
  ): Promise<Array<Record<string, string>>> {
    const rows: Array<Record<string, string>> = [];
    let rowSkips = 0;
    const pageSize = request.pageSize ?? DEFAULT_PAGE_SIZE;
    const baseCap = request.scanCap ?? DEFAULT_SCAN_CAP;
    const scanCap = baseCap * this.scanCapMultiplier;

    while (true) {
      if (rows.length >= scanCap) {
        console.warn(
          `[KPI] Scan cap (${scanCap}) reached for ${request.table}. Returning ${rows.length} partial rows.`,
        );
        return rows;
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
