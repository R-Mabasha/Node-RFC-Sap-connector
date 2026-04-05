import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";

import { loadConfig } from "../config.js";
import { KPI_DEFINITIONS } from "./definitions.js";

function withEmptyTempDir<T>(run: (dir: string) => T): T {
  const dir = mkdtempSync(join(tmpdir(), "hypercare-kpi-registry-"));

  try {
    return run(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

function looksLikeSapIdentifier(value: string): boolean {
  const trimmed = value.trim().toUpperCase();
  return /^[A-Z0-9_/]+$/.test(trimmed) && trimmed !== "NONE";
}

test("KPI registry ids stay unique", () => {
  const seen = new Set<string>();
  const duplicates: string[] = [];

  for (const definition of KPI_DEFINITIONS) {
    if (seen.has(definition.id)) {
      duplicates.push(definition.id);
    }
    seen.add(definition.id);
  }

  assert.deepEqual(duplicates, []);
});

test("implemented and planned KPIs remain compatible with the default allowlists", () => {
  const config = withEmptyTempDir((cwd) => loadConfig({}, { cwd }));
  const allowedTables = new Set(config.sap.allowedTables);
  const allowedFunctions = new Set(config.sap.allowedFunctions);
  const problems: Array<Record<string, string>> = [];

  for (const definition of KPI_DEFINITIONS) {
    if (definition.maturity === "excluded") {
      continue;
    }

    if ("wrapper" in definition && definition.wrapper) {
      const functionName = definition.wrapper.functionName.toUpperCase();
      if (!allowedFunctions.has(functionName)) {
        problems.push({
          kpiId: definition.id,
          type: "wrapper_not_allowlisted",
          dependency: functionName,
        });
      }
      continue;
    }

    if (definition.maturity !== "implemented" && definition.maturity !== "planned") {
      continue;
    }

    for (const objectName of definition.source.objects) {
      const dependency = objectName.trim().toUpperCase();

      if (!looksLikeSapIdentifier(dependency)) {
        continue;
      }

      const isCovered =
        allowedTables.has(dependency) || allowedFunctions.has(dependency);

      if (!isCovered) {
        problems.push({
          kpiId: definition.id,
          type: "dependency_not_allowlisted",
          dependency,
        });
      }
    }
  }

  assert.deepEqual(problems, []);
});

test("every KPI definition exposes a polling tier", () => {
  const missingTiers = KPI_DEFINITIONS
    .filter((definition) => !definition.tier)
    .map((definition) => definition.id);

  assert.deepEqual(missingTiers, []);
});

test("the remaining direct KPI backlog is empty", () => {
  const planned = KPI_DEFINITIONS
    .filter((definition) => definition.maturity === "planned")
    .map((definition) => definition.id);

  assert.deepEqual(planned, []);
});
