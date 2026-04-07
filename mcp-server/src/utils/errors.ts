export function describeError(error: unknown): string {
  if (error instanceof Error) {
    const details = [
      "key" in error && typeof error.key === "string" ? `key=${error.key}` : undefined,
      "code" in error &&
      (typeof error.code === "string" || typeof error.code === "number")
        ? `code=${String(error.code)}`
        : undefined,
      "group" in error &&
      (typeof error.group === "string" || typeof error.group === "number")
        ? `group=${String(error.group)}`
        : undefined,
    ].filter((value): value is string => value !== undefined);

    return details.length > 0
      ? `${error.message} (${details.join(", ")})`
      : error.message;
  }

  if (typeof error === "object" && error !== null) {
    const record = error as Record<string, unknown>;
    const message =
      (typeof record.message === "string" && record.message) ||
      (typeof record.toString === "function" ? record.toString() : undefined) ||
      "Unknown error";
    const details = [
      typeof record.key === "string" ? `key=${record.key}` : undefined,
      typeof record.code === "string" || typeof record.code === "number"
        ? `code=${String(record.code)}`
        : undefined,
      typeof record.group === "string" || typeof record.group === "number"
        ? `group=${String(record.group)}`
        : undefined,
    ].filter((value): value is string => value !== undefined);

    return details.length > 0
      ? `${message} (${details.join(", ")})`
      : message;
  }

  return String(error);
}

const SAP_CAPABILITY_ERROR_KEYS = new Set([
  "FU_NOT_FOUND",
  "FIELD_NOT_VALID",
  "TABLE_NOT_AVAILABLE",
  "SAPSQL_PARSE_ERROR",
  "NOT_FOUND",
]);

const SAP_CAPABILITY_ERROR_PATTERNS = [
  /\bFU_NOT_FOUND\b/i,
  /\bFIELD_NOT_VALID\b/i,
  /\bTABLE_NOT_AVAILABLE\b/i,
  /\bSAPSQL_PARSE_ERROR\b/i,
  /\bnot found\b/i,
  /\bdoes not exist\b/i,
  /\bunknown field\b/i,
  /\bunknown column\b/i,
];

export function getSapErrorKey(error: unknown): string | undefined {
  if (!error || typeof error !== "object") {
    return undefined;
  }

  const key = (error as Record<string, unknown>).key;
  return typeof key === "string" ? key.toUpperCase() : undefined;
}

export function isBusyResourceError(error: unknown): boolean {
  return describeError(error).toLowerCase().includes("device or resource busy");
}

export function isSapCapabilityError(error: unknown): boolean {
  const key = getSapErrorKey(error);
  if (key && SAP_CAPABILITY_ERROR_KEYS.has(key)) {
    return true;
  }

  const message = describeError(error);
  return SAP_CAPABILITY_ERROR_PATTERNS.some((pattern) => pattern.test(message));
}

export function shouldTripCircuitBreaker(error: unknown): boolean {
  return !isSapCapabilityError(error);
}
