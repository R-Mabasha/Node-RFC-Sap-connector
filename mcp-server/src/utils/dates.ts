import type { KpiRequestInput, ResolvedWindow } from "../types.js";

function parseDate(input: string): Date {
  const value = new Date(input);

  if (Number.isNaN(value.getTime())) {
    throw new Error(`Invalid date value: ${input}`);
  }

  return value;
}

function toSapDate(date: Date): string {
  return date.toISOString().slice(0, 10).replaceAll("-", "");
}

export function parseSapDate(dateValue: string): Date | undefined {
  const date = dateValue.trim();

  if (date.length !== 8 || date === "00000000") {
    return undefined;
  }

  const parsed = new Date(
    `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}T00:00:00Z`,
  );

  if (Number.isNaN(parsed.getTime())) {
    return undefined;
  }

  return parsed;
}

export function daysSinceSapDate(dateValue: string, now = new Date()): number | undefined {
  const parsed = parseSapDate(dateValue);

  if (!parsed) {
    return undefined;
  }

  return Math.floor((now.getTime() - parsed.getTime()) / (24 * 60 * 60 * 1000));
}

export function resolveWindow(
  input: KpiRequestInput,
  fallbackHours = 24,
): ResolvedWindow {
  const toDate = input.to ? parseDate(input.to) : new Date();
  const fromDate = input.from
    ? parseDate(input.from)
    : new Date(toDate.getTime() - fallbackHours * 60 * 60 * 1000);

  if (fromDate > toDate) {
    throw new Error("The 'from' timestamp must be earlier than or equal to 'to'.");
  }

  const windowMs = toDate.getTime() - fromDate.getTime();
  const windowDays = Math.max(1, Math.ceil(windowMs / (24 * 60 * 60 * 1000)));

  return {
    from: fromDate.toISOString(),
    to: toDate.toISOString(),
    sapFrom: toSapDate(fromDate),
    sapTo: toSapDate(toDate),
    windowDays,
  };
}

export function getNumberDimension(
  input: KpiRequestInput,
  key: string,
  defaultValue: number,
): number {
  const rawValue = input.dimensions?.[key];

  if (!rawValue) {
    return defaultValue;
  }

  const value = Number(rawValue);

  if (!Number.isFinite(value)) {
    throw new Error(`Dimension '${key}' must be numeric.`);
  }

  return value;
}

export function toSapDateDaysAgo(days: number): string {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() - days);
  return toSapDate(date);
}

export function parseSapDateTime(dateValue: string, timeValue: string): Date | undefined {
  const date = dateValue.trim();
  const time = timeValue.trim();

  if (date.length !== 8 || time.length < 6 || date === "00000000") {
    return undefined;
  }

  const isoValue = `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(
    6,
    8,
  )}T${time.slice(0, 2)}:${time.slice(2, 4)}:${time.slice(4, 6)}Z`;

  const parsed = new Date(isoValue);

  if (Number.isNaN(parsed.getTime())) {
    return undefined;
  }

  return parsed;
}
