// ---------------------------------------------------------------------------
// config.ts — Configuration loader for the Hypercare SAP MCP server.
//
// Resolution order:
//   1. .env / .env.local files (searched in cwd and package root)
//   2. process.env overrides file values
//   3. .env.example used as last-resort fallback (with a warning)
//
// SAP connection modes:
//   - "destination" — uses sapnwrfc.ini via SAP_DEST
//   - "direct"      — explicit host/sysnr/client/user/passwd
//   - "none"        — no SAP config found (server starts but RFC calls fail)
// ---------------------------------------------------------------------------

import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import * as z from "zod/v4";

import type { AppConfig } from "./types.js";

const DEFAULT_TABLE_READ_FUNCTIONS = "/BUI/RFC_READ_TABLE,BBP_RFC_READ_TABLE";
const DEFAULT_ALLOWED_TABLES = [
  "TBTCO", "TBTCS", "SNAP",
  "EDIDC", "EDIDS",
  "USR02", "USR41", "USREFUS",
  "RSECACTPROT",
  "VBAK", "VBRK", "VBUK", "VBEP", "LIKP",
  "EKKO", "EKBE", "RBKP", "MARA", "MKPF", "MSEG",
  "BKPF", "BSEG", "FAGLFLEXT", "REGUH", "REGUP",
  "AUFK", "AFKO", "AFRU", "QMEL", "IFLOT",
  "ARFCSSTATE", "QRFCSSTATE", "IUUC_REPL_CONTENT", "TRFCQOUT", "VBHDR",
  "MDLG",
  "CVI_CUST_LINK", "CVI_VEND_LINK",
  "BUT000", "KNA1", "LFA1",
  "E070", "E071",
  "SWWWIHEAD",
  "TSP01",
  "DBSTATTABC",
  "NRIV",
  "GRACSOBJECT", "GRACFFLOG",
].join(",");
const DEFAULT_ALLOWED_FUNCTIONS = [
  "RFC_SYSTEM_INFO",
  "TH_SERVER_LIST",
  "TH_WPINFO",
  "RSLG_GET_MESSAGES",
  "SWNC_COLLECTOR_GET_AGGREGATES",
  "ENQUEUE_STATISTICS",
  "ICM_GET_MONITOR_INFO",
  "GW_GET_STATISTIC",
  "SLIC_GET_INSTALLATIONS",
  "BAPI_SYSTEM_MON_GETSYSINFO",
  "ZHC_GET_SECURITY_KPIS",
  "ZHC_GET_OTC_KPIS",
  "ZHC_GET_P2P_KPIS",
  "ZHC_GET_FINANCE_KPIS",
  "ZHC_GET_DATA_QUALITY_KPIS",
  "ZHC_GET_JOB_KPIS",
  "ZHC_GET_MANUFACTURING_KPIS",
  "ZHC_GET_SERVICE_KPIS",
  "ZHC_GET_TAX_KPIS",
  "ZHC_GET_EAM_KPIS",
  "ZHC_GET_INFRASTRUCTURE_KPIS",
].join(",");

// ── Zod schema for environment variables ───────────────────────────────────

const envSchema = z.object({
  // -- Server --
  MCP_HOST: z.string().trim().default("127.0.0.1"),
  MCP_PORT: z.coerce.number().int().min(1).max(65535).default(3001),

  // -- SAP connection (destination-based) --
  SAP_DEST: z.string().trim().optional(),

  // -- SAP connection (direct) --
  SAP_ASHOST: z.string().trim().optional(),
  SAP_SYSNR: z.string().trim().optional(),
  SAP_CLIENT: z.string().trim().optional(),
  SAP_USER: z.string().trim().optional(),
  SAP_PASSWD: z.string().trim().optional(),
  SAP_LANG: z.string().trim().default("EN"),

  // -- Connection pool --
  SAP_POOL_LOW: z.coerce.number().int().min(0).default(0),
  SAP_POOL_HIGH: z.coerce.number().int().min(1).default(4),
  SAP_RFC_TIMEOUT_MS: z.coerce.number().int().min(1000).default(30000),

  // -- Circuit breaker --
  SAP_CB_THRESHOLD: z.coerce.number().int().min(1).default(5),
  SAP_CB_RESET_MS: z.coerce.number().int().min(5000).default(60000),

  // -- Table reader function chain (S/4 compatible alternatives) --
  SAP_TABLE_READ_FUNCTIONS: z.string().default(DEFAULT_TABLE_READ_FUNCTIONS),

  // -- Allowlisted SAP tables for generic reads --
  // Organized by domain for clarity:
  //   Jobs:        TBTCO, TBTCS
  //   Dumps:       SNAP
  //   IDocs:       EDIDC, EDIDS
  //   Users:       USR02, USR41, USREFUS
  //   Security:    RSECACTPROT
  //   SD/OTC:      VBAK, VBRK, VBUK, VBEP, LIKP
  //   MM/P2P:      EKKO, EKBE, RBKP, MARA, MKPF, MSEG
  //   Finance:     BKPF, BSEG, FAGLFLEXT, REGUH, REGUP
  //   PM/EAM:      AUFK, AFKO, AFRU, QMEL, IFLOT
  //   Integration: ARFCSSTATE, QRFCSSTATE, IUUC_REPL_CONTENT, TRFCQOUT, VBHDR
  //   MRP:         MDLG
  //   CVI:         CVI_CUST_LINK, CVI_VEND_LINK
  //   Master Data: BUT000, KNA1, LFA1
  //   Transport:   E070, E071
  //   Workflow:    SWWWIHEAD
  //   Spool:       TSP01
  //   DB Stats:    DBSTATTABC
  //   Number Range: NRIV
  //   GRC:         GRACSOBJECT, GRACFFLOG
  SAP_ALLOWED_TABLES: z.string().optional(),

  // -- Allowlisted RFC function modules --
  // Standard SAP FMs for system monitoring + custom ZHC_* wrappers
  SAP_ALLOWED_FUNCTIONS: z.string().optional(),
});

// ── Helpers ────────────────────────────────────────────────────────────────

interface LoadConfigOptions {
  cwd?: string;
  envSearchRoots?: string[];
}

interface LoadedEnvFiles {
  values: Record<string, string>;
  sources: string[];
  warnings: string[];
}

function splitCsv(value: string): string[] {
  return value
    .split(",")
    .map((entry) => entry.trim().toUpperCase())
    .filter((entry) => entry.length > 0);
}

function parseEnvValue(rawValue: string): string {
  const value = rawValue.trim();

  if (value.startsWith('"') && value.endsWith('"')) {
    return value.slice(1, -1).replaceAll("\\n", "\n").replaceAll('\\"', '"');
  }

  if (value.startsWith("'") && value.endsWith("'")) {
    return value.slice(1, -1);
  }

  return value;
}

function parseEnvFile(content: string): Record<string, string> {
  const values: Record<string, string> = {};

  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();

    if (line.length === 0 || line.startsWith("#")) {
      continue;
    }

    const normalizedLine = line.startsWith("export ")
      ? line.slice("export ".length)
      : line;
    const separatorIndex = normalizedLine.indexOf("=");

    if (separatorIndex <= 0) {
      continue;
    }

    const key = normalizedLine.slice(0, separatorIndex).trim();
    const value = normalizedLine.slice(separatorIndex + 1);

    if (key.length === 0) {
      continue;
    }

    values[key] = parseEnvValue(value);
  }

  return values;
}

function dedupeStrings(values: string[]): string[] {
  return [...new Set(values)];
}

function mergeCsvLists(...values: Array<string | undefined>): string[] {
  return dedupeStrings(
    values.flatMap((value) => (value ? splitCsv(value) : [])),
  );
}

function resolveEnvSearchRoots(
  cwd: string,
  explicitRoots?: string[],
): string[] {
  if (explicitRoots && explicitRoots.length > 0) {
    return dedupeStrings(explicitRoots.map((root) => resolve(root)));
  }

  const packageRoot = resolve(
    dirname(fileURLToPath(import.meta.url)),
    "..",
  );

  return dedupeStrings([resolve(cwd), packageRoot]);
}

function loadEnvFiles(searchRoots: string[]): LoadedEnvFiles {
  const preferredFiles = [".env", ".env.local"];
  const fallbackFiles = [".env.example"];
  const preferredPaths = searchRoots.flatMap((root) =>
    preferredFiles
      .map((fileName) => join(root, fileName))
      .filter((filePath) => existsSync(filePath)),
  );
  const fallbackPaths = searchRoots.flatMap((root) =>
    fallbackFiles
      .map((fileName) => join(root, fileName))
      .filter((filePath) => existsSync(filePath)),
  );
  const selectedPaths = preferredPaths.length > 0 ? preferredPaths : fallbackPaths;
  const warnings: string[] = [];

  if (selectedPaths.some((filePath) => filePath.endsWith(".env.example"))) {
    warnings.push(
      "Using '.env.example' as a runtime fallback. Move secrets to '.env' or '.env.local'.",
    );
  }

  const values = selectedPaths.reduce<Record<string, string>>((current, filePath) => {
    const content = readFileSync(filePath, "utf8");

    return {
      ...current,
      ...parseEnvFile(content),
    };
  }, {});

  return {
    values,
    sources: selectedPaths,
    warnings,
  };
}

// ── Validation ─────────────────────────────────────────────────────────────

function isLikelySystemNumber(value: string): boolean {
  return /^\d{2}$/.test(value.trim());
}

function isLikelySapClient(value: string): boolean {
  return /^\d{3}$/.test(value.trim());
}

function isLikelyHost(value: string): boolean {
  const trimmed = value.trim();
  return (
    /^[A-Za-z0-9.-]+$/.test(trimmed) &&
    !isLikelySystemNumber(trimmed)
  );
}

function validateDirectConnectionParameters(
  env: z.infer<typeof envSchema>,
): string[] {
  const providedFields = [
    "SAP_ASHOST",
    "SAP_SYSNR",
    "SAP_CLIENT",
    "SAP_USER",
    "SAP_PASSWD",
  ].filter((key) => {
    const value = env[key as keyof typeof env];
    return typeof value === "string" && value.trim().length > 0;
  });

  if (providedFields.length === 0) {
    return [];
  }

  const missingFields = [
    "SAP_ASHOST",
    "SAP_SYSNR",
    "SAP_CLIENT",
    "SAP_USER",
    "SAP_PASSWD",
  ].filter((key) => {
    const value = env[key as keyof typeof env];
    return !(typeof value === "string" && value.trim().length > 0);
  });

  if (missingFields.length > 0) {
    throw new Error(
      `Incomplete SAP direct connection configuration. Missing: ${missingFields.join(
        ", ",
      )}.`,
    );
  }

  if (
    isLikelySystemNumber(env.SAP_ASHOST ?? "") &&
    isLikelyHost(env.SAP_SYSNR ?? "")
  ) {
    throw new Error(
      `SAP_ASHOST '${env.SAP_ASHOST}' and SAP_SYSNR '${env.SAP_SYSNR}' look swapped. Expected SAP_ASHOST to be a host/IP and SAP_SYSNR to be a two-digit system number like '00'.`,
    );
  }

  if (!isLikelySystemNumber(env.SAP_SYSNR ?? "")) {
    throw new Error(
      `Invalid SAP_SYSNR '${env.SAP_SYSNR ?? ""}'. SAP system number should be a two-digit value like '00'.`,
    );
  }

  if (!isLikelySapClient(env.SAP_CLIENT ?? "")) {
    throw new Error(
      `Invalid SAP_CLIENT '${env.SAP_CLIENT ?? ""}'. SAP client should be a three-digit value like '100'.`,
    );
  }

  if (!isLikelyHost(env.SAP_ASHOST ?? "")) {
    throw new Error(
      `Invalid SAP_ASHOST '${env.SAP_ASHOST ?? ""}'. Provide the SAP application server hostname or IP address.`,
    );
  }

  const warnings: string[] = [];

  if (env.SAP_DEST) {
    warnings.push(
      "SAP_DEST is set, so direct SAP_* connection parameters will be ignored.",
    );
  }

  return warnings;
}

function resolveConnectionParameters(
  env: z.infer<typeof envSchema>,
): {
  connectionMode: "none" | "destination" | "direct";
  connectionParameters?: Record<string, string>;
} {
  if (env.SAP_DEST) {
    return {
      connectionMode: "destination",
      connectionParameters: { dest: env.SAP_DEST },
    };
  }

  if (
    env.SAP_ASHOST &&
    env.SAP_SYSNR &&
    env.SAP_CLIENT &&
    env.SAP_USER &&
    env.SAP_PASSWD
  ) {
    return {
      connectionMode: "direct",
      connectionParameters: {
        ashost: env.SAP_ASHOST,
        sysnr: env.SAP_SYSNR,
        client: env.SAP_CLIENT,
        user: env.SAP_USER,
        passwd: env.SAP_PASSWD,
        lang: env.SAP_LANG,
      },
    };
  }

  return {
    connectionMode: "none",
    connectionParameters: undefined,
  };
}

// ── Public API ─────────────────────────────────────────────────────────────

export function loadConfig(
  envInput: NodeJS.ProcessEnv = process.env,
  options: LoadConfigOptions = {},
): AppConfig {
  const cwd = options.cwd ?? process.cwd();
  const loadedEnv = loadEnvFiles(
    resolveEnvSearchRoots(cwd, options.envSearchRoots),
  );
  const env = envSchema.parse({
    ...loadedEnv.values,
    ...envInput,
  });
  const validationWarnings = validateDirectConnectionParameters(env);
  const resolvedConnection = resolveConnectionParameters(env);
  const configWarnings = [...loadedEnv.warnings, ...validationWarnings];

  if (resolvedConnection.connectionMode === "none") {
    configWarnings.push(
      "SAP connection is not configured. Set SAP_DEST or provide direct SAP_* variables.",
    );
  }

  return {
    host: env.MCP_HOST,
    port: env.MCP_PORT,
    sap: {
      connectionParameters: resolvedConnection.connectionParameters,
      connectionMode: resolvedConnection.connectionMode,
      configSources: loadedEnv.sources,
      configWarnings,
      poolLow: env.SAP_POOL_LOW,
      poolHigh: env.SAP_POOL_HIGH,
      timeoutMs: env.SAP_RFC_TIMEOUT_MS,
      tableReadFunctions: splitCsv(env.SAP_TABLE_READ_FUNCTIONS),
      allowedTables: mergeCsvLists(DEFAULT_ALLOWED_TABLES, env.SAP_ALLOWED_TABLES),
      allowedFunctions: mergeCsvLists(
        DEFAULT_ALLOWED_FUNCTIONS,
        env.SAP_ALLOWED_FUNCTIONS,
      ),
      circuitBreakerThreshold: env.SAP_CB_THRESHOLD,
      circuitBreakerResetMs: env.SAP_CB_RESET_MS,
    },
  };
}
