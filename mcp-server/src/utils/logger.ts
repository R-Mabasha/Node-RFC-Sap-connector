// ---------------------------------------------------------------------------
// logger.ts — Minimal structured logger with levels and safe serialization.
// Replaces raw console.log to support correlation IDs and production hygiene.
// ---------------------------------------------------------------------------

export type LogLevel = "debug" | "info" | "warn" | "error";

export interface LogMeta {
  [key: string]: unknown;
  requestId?: string;
  fingerprint?: string;
}

export interface Logger {
  debug(msg: string, meta?: LogMeta): void;
  info(msg: string, meta?: LogMeta): void;
  warn(msg: string, meta?: LogMeta): void;
  error(msg: string, meta?: LogMeta): void;
}

const LEVEL_RANK: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

function getMinLevel(): LogLevel {
  const env = process.env.MCP_LOG_LEVEL?.trim().toLowerCase() as LogLevel | undefined;
  if (env && env in LEVEL_RANK) return env;
  return "info";
}

function sanitizeValue(value: unknown): unknown {
  if (typeof value === "string") {
    return value
      .replace(/\"passwd\":\"[^"]*\"/g, '\"passwd\":\"***\"')
      .replace(/\"password\":\"[^"]*\"/g, '\"password\":\"***\"')
      .replace(/\"auth\":\"[^"]*\"/g, '\"auth\":\"***\"')
      .replace(/Bearer\s+[A-Za-z0-9_\-\.]+/g, "Bearer ***");
  }
  if (Array.isArray(value)) {
    return value.map(sanitizeValue);
  }
  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) {
      const lower = k.toLowerCase();
      if (lower.includes("passwd") || lower.includes("password") || lower.includes("secret") || lower.includes("token")) {
        out[k] = "***";
      } else {
        out[k] = sanitizeValue(v);
      }
    }
    return out;
  }
  return value;
}

function output(level: LogLevel, msg: string, meta?: LogMeta) {
  const min = getMinLevel();
  if (LEVEL_RANK[level] < LEVEL_RANK[min]) return;

  const timestamp = new Date().toISOString();
  const payload = {
    timestamp,
    level: level.toUpperCase(),
    message: msg,
    ...(meta ? sanitizeValue(meta) as LogMeta : {}),
  };

  const stream = level === "error" || level === "warn" ? process.stderr : process.stdout;
  stream.write(JSON.stringify(payload) + "\n");
}

export const defaultLogger: Logger = {
  debug: (msg, meta) => output("debug", msg, meta),
  info: (msg, meta) => output("info", msg, meta),
  warn: (msg, meta) => output("warn", msg, meta),
  error: (msg, meta) => output("error", msg, meta),
};

export function createChildLogger(parent: Logger, baseMeta: LogMeta): Logger {
  return {
    debug: (msg, meta) => parent.debug(msg, { ...baseMeta, ...meta }),
    info: (msg, meta) => parent.info(msg, { ...baseMeta, ...meta }),
    warn: (msg, meta) => parent.warn(msg, { ...baseMeta, ...meta }),
    error: (msg, meta) => parent.error(msg, { ...baseMeta, ...meta }),
  };
}
