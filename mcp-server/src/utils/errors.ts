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
