import type { KpiDefinition, NonExecutableKpiDefinition, WrapperSpec } from "../kpis/definitions.js";
import type { KpiRequestInput, KpiResult, ResolvedWindow } from "../types.js";
import { resolveWindow } from "../utils/dates.js";

export interface WrapperCatalogKpi {
  kpiId: string;
  wrapperKpiId: string;
  title: string;
  category: string;
  blocker: string;
  summary: string;
  notes: string[];
}

export interface WrapperCatalogEntry {
  functionName: string;
  allowlisted: boolean;
  kpis: WrapperCatalogKpi[];
  categories: string[];
  blockers: string[];
}

export interface WrapperMessage {
  type?: string;
  id?: string;
  number?: string;
  message: string;
}

export interface ParsedWrapperResponse {
  functionName: string;
  schemaVersion?: string;
  rawKeys: string[];
  messages: WrapperMessage[];
  resultsByKpiId: Map<string, KpiResult>;
}

export interface WrapperProbeResult {
  ok: boolean;
  functionName: string;
  schemaVersion?: string;
  rawKeys: string[];
  expectedKpiIds: string[];
  returnedKpiIds: string[];
  missingKpiIds: string[];
  unexpectedKpiIds: string[];
  messageCount: number;
  errorMessages: string[];
  warningMessages: string[];
  results: KpiResult[];
}

function normalizeIdentifier(value: string): string {
  return value.trim().toUpperCase();
}

export function isWrapperBackedDefinition(
  definition: KpiDefinition,
): definition is NonExecutableKpiDefinition & { wrapper: WrapperSpec } {
  return "wrapper" in definition && definition.wrapper !== undefined;
}

function uniqueSorted(values: string[]): string[] {
  return [...new Set(values)].sort((left, right) => left.localeCompare(right));
}

export function buildWrapperCatalog(
  definitions: KpiDefinition[],
  allowedFunctions: string[] = [],
): WrapperCatalogEntry[] {
  const allowedSet = new Set(allowedFunctions.map(normalizeIdentifier));
  const catalog = new Map<string, WrapperCatalogEntry>();

  for (const definition of definitions) {
    if (!isWrapperBackedDefinition(definition)) {
      continue;
    }

    const functionName = normalizeIdentifier(definition.wrapper.functionName);
    const current =
      catalog.get(functionName) ??
      {
        functionName,
        allowlisted: allowedSet.has(functionName),
        kpis: [],
        categories: [],
        blockers: [],
      };

    current.kpis.push({
      kpiId: definition.id,
      wrapperKpiId: definition.wrapper.wrapperKpiId,
      title: definition.title,
      category: definition.category,
      blocker: definition.blocker,
      summary: definition.summary,
      notes: definition.notes ?? [],
    });
    current.categories.push(definition.category);
    current.blockers.push(definition.blocker);
    catalog.set(functionName, current);
  }

  return [...catalog.values()]
    .map((entry) => ({
      ...entry,
      kpis: [...entry.kpis].sort((left, right) =>
        left.kpiId.localeCompare(right.kpiId),
      ),
      categories: uniqueSorted(entry.categories),
      blockers: uniqueSorted(entry.blockers),
    }))
    .sort((left, right) => left.functionName.localeCompare(right.functionName));
}

function extractRecordTable(
  result: Record<string, unknown>,
  preferredKeys: string[],
): Array<Record<string, unknown>> {
  for (const key of preferredKeys) {
    const value = result[key];
    if (Array.isArray(value)) {
      return value.filter(
        (entry): entry is Record<string, unknown> =>
          typeof entry === "object" && entry !== null,
      );
    }
  }

  return [];
}

function extractString(record: Record<string, unknown>, keys: string[]): string | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
  }

  return undefined;
}

function extractNumber(record: Record<string, unknown>, keys: string[]): number | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string" && value.trim().length > 0) {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }

  return undefined;
}

function normalizeStatus(status: unknown): KpiResult["status"] {
  const value = String(status ?? "ok").trim().toLowerCase();

  switch (value) {
    case "ok":
    case "success":
      return "ok";
    case "planned":
      return "planned";
    case "custom_abap_required":
    case "wrapper":
      return "custom_abap_required";
    case "excluded":
      return "excluded";
    case "error":
    case "failed":
      return "error";
    default:
      return "ok";
  }
}

function parseMessages(result: Record<string, unknown>): WrapperMessage[] {
  const rows = extractRecordTable(result, ["ET_MESSAGES", "RETURN", "MESSAGES"]);

  return rows
    .map((row) => {
      const type = extractString(row, ["TYPE"]);
      const id = extractString(row, ["ID"]);
      const number = extractString(row, ["NUMBER", "NO"]);
      const message =
        extractString(row, ["MESSAGE", "TEXT", "MSGV1"]) ??
        JSON.stringify(row);

      return {
        type,
        id,
        number,
        message,
      };
    })
    .filter((message) => message.message.length > 0);
}

export function parseWrapperResponse(
  functionName: string,
  result: Record<string, unknown>,
  window?: ResolvedWindow,
): ParsedWrapperResponse {
  const rows = extractRecordTable(result, [
    "ET_KPIS",
    "ET_RESULTS",
    "RESULTS",
    "KPI_RESULTS",
  ]);
  const resultsByKpiId = new Map<string, KpiResult>();

  for (const row of rows) {
    const wrapperKpiId = extractString(row, ["KPI_ID", "KPIID", "ID"]);
    if (!wrapperKpiId) {
      continue;
    }

    const status = normalizeStatus(
      extractString(row, ["STATUS", "KPI_STATUS"]),
    );
    const unit = extractString(row, ["UNIT"]);
    const value = extractNumber(row, ["VALUE_NUM", "VALUE", "METRIC_VALUE"]);
    const title = extractString(row, ["TITLE", "KPI_TITLE"]) ?? wrapperKpiId;
    const category = extractString(row, ["CATEGORY"]) ?? "Wrapper KPI";
    const notes: string[] = [];

    const noteText = extractString(row, ["NOTE", "MESSAGE"]);
    if (noteText) {
      notes.push(noteText);
    }

    const notesJson = extractString(row, ["NOTES_JSON"]);
    if (notesJson) {
      try {
        const parsed = JSON.parse(notesJson);
        if (Array.isArray(parsed)) {
          notes.push(
            ...parsed.filter((item): item is string => typeof item === "string"),
          );
        } else if (typeof parsed === "string") {
          notes.push(parsed);
        }
      } catch {
        notes.push(notesJson);
      }
    }

    resultsByKpiId.set(wrapperKpiId, {
      kpiId: wrapperKpiId,
      title,
      category,
      status,
      unit,
      value,
      window,
      source: { kind: "rfc", objects: [functionName] },
      notes,
    });
  }

  return {
    functionName,
    schemaVersion: extractString(result, ["EV_SCHEMA_VERSION", "SCHEMA_VERSION"]),
    rawKeys: Object.keys(result).sort((left, right) => left.localeCompare(right)),
    messages: parseMessages(result),
    resultsByKpiId,
  };
}

export function buildWrapperCallParameters(
  input: KpiRequestInput,
): Record<string, unknown> {
  const window = resolveWindow(input, 24);
  const dimensions = Object.entries(input.dimensions ?? {}).map(
    ([name, value]) => ({
      NAME: name,
      VALUE: value,
    }),
  );

  return {
    IV_FROM_DATE: window.sapFrom,
    IV_TO_DATE: window.sapTo,
    IV_FROM_TS: window.from,
    IV_TO_TS: window.to,
    IV_DIMENSIONS_JSON: JSON.stringify(input.dimensions ?? {}),
    IT_DIMENSIONS: dimensions,
  };
}

export function buildWrapperProbeResult(options: {
  functionName: string;
  result: Record<string, unknown>;
  expectedKpiIds?: string[];
  window?: ResolvedWindow;
}): WrapperProbeResult {
  const parsed = parseWrapperResponse(
    options.functionName,
    options.result,
    options.window,
  );
  const expectedKpiIds = uniqueSorted(options.expectedKpiIds ?? []);
  const returnedKpiIds = uniqueSorted([...parsed.resultsByKpiId.keys()]);
  const returnedSet = new Set(returnedKpiIds);
  const expectedSet = new Set(expectedKpiIds);
  const missingKpiIds = expectedKpiIds.filter((kpiId) => !returnedSet.has(kpiId));
  const unexpectedKpiIds = returnedKpiIds.filter(
    (kpiId) => expectedKpiIds.length > 0 && !expectedSet.has(kpiId),
  );
  const errorMessages = parsed.messages
    .filter((message) => ["A", "E", "X"].includes(message.type ?? ""))
    .map((message) => message.message);
  const warningMessages = [
    ...parsed.messages
      .filter((message) => ["W", "I"].includes(message.type ?? ""))
      .map((message) => message.message),
  ];

  if (!parsed.schemaVersion) {
    warningMessages.push("Wrapper did not return EV_SCHEMA_VERSION.");
  }

  if (returnedKpiIds.length === 0) {
    warningMessages.push("Wrapper returned no ET_KPIS rows.");
  }

  return {
    ok:
      errorMessages.length === 0 &&
      missingKpiIds.length === 0 &&
      returnedKpiIds.length > 0,
    functionName: options.functionName,
    schemaVersion: parsed.schemaVersion,
    rawKeys: parsed.rawKeys,
    expectedKpiIds,
    returnedKpiIds,
    missingKpiIds,
    unexpectedKpiIds,
    messageCount: parsed.messages.length,
    errorMessages,
    warningMessages: uniqueSorted(warningMessages),
    results: [...parsed.resultsByKpiId.values()].sort((left, right) =>
      left.kpiId.localeCompare(right.kpiId),
    ),
  };
}
