import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";

import { loadConfig } from "./config.js";

function withTempDir(run: (dir: string) => void) {
  const dir = mkdtempSync(join(tmpdir(), "hypercare-mcp-config-"));

  try {
    run(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test("loadConfig reads .env.example as fallback when .env is absent", () => {
  withTempDir((dir) => {
    writeFileSync(
      join(dir, ".env.example"),
      [
        "SAP_ASHOST=172.17.19.24",
        "SAP_SYSNR=00",
        "SAP_CLIENT=100",
        "SAP_USER=demo_user",
        "SAP_PASSWD=demo_password",
      ].join("\n"),
      "utf8",
    );

    const config = loadConfig({}, { cwd: dir, envSearchRoots: [dir] });

    assert.equal(config.sap.connectionMode, "direct");
    assert.equal(Boolean(config.sap.connectionParameters), true);
    assert.deepEqual(config.sap.configSources, [join(dir, ".env.example")]);
    assert.equal(
      config.sap.configWarnings.includes(
        "Using '.env.example' as a runtime fallback. Move secrets to '.env' or '.env.local'.",
      ),
      true,
    );
  });
});

test("loadConfig prefers .env over .env.example and process env overrides files", () => {
  withTempDir((dir) => {
    writeFileSync(join(dir, ".env"), "SAP_DEST=ENV_DEST\nMCP_PORT=3001\n", "utf8");
    writeFileSync(
      join(dir, ".env.example"),
      "SAP_DEST=EXAMPLE_DEST\nMCP_PORT=3999\n",
      "utf8",
    );

    const config = loadConfig({ MCP_PORT: "3019" }, { cwd: dir, envSearchRoots: [dir] });

    assert.equal(config.sap.connectionMode, "destination");
    assert.deepEqual(config.sap.connectionParameters, { dest: "ENV_DEST" });
    assert.equal(config.port, 3019);
    assert.deepEqual(config.sap.configSources, [join(dir, ".env")]);
  });
});

test("loadConfig rejects swapped SAP_ASHOST and SAP_SYSNR values", () => {
  withTempDir((dir) => {
    writeFileSync(
      join(dir, ".env.example"),
      [
        "SAP_ASHOST=00",
        "SAP_SYSNR=172.17.19.24",
        "SAP_CLIENT=100",
        "SAP_USER=demo_user",
        "SAP_PASSWD=demo_password",
      ].join("\n"),
      "utf8",
    );

    assert.throws(
      () => loadConfig({}, { cwd: dir, envSearchRoots: [dir] }),
      /look swapped|Invalid SAP_SYSNR/,
    );
  });
});

test("loadConfig can find env files from the package root when cwd differs", () => {
  withTempDir((dir) => {
    const cwd = join(dir, "workspace");
    const packageRoot = join(dir, "mcp-server");

    mkdirSync(cwd, { recursive: true });
    mkdirSync(packageRoot, { recursive: true });
    writeFileSync(
      join(packageRoot, ".env"),
      [
        "SAP_ASHOST=172.17.19.24",
        "SAP_SYSNR=00",
        "SAP_CLIENT=100",
        "SAP_USER=demo_user",
        "SAP_PASSWD=demo_password",
      ].join("\n"),
      "utf8",
    );

    const config = loadConfig({}, { cwd, envSearchRoots: [cwd, packageRoot] });

    assert.equal(config.sap.connectionMode, "direct");
    assert.deepEqual(config.sap.configSources, [join(packageRoot, ".env")]);
  });
});

test("loadConfig keeps the built-in allowlist even when env files provide older custom lists", () => {
  withTempDir((dir) => {
    writeFileSync(
      join(dir, ".env"),
      [
        "SAP_DEST=ENV_DEST",
        "SAP_ALLOWED_TABLES=USR41",
        "SAP_ALLOWED_FUNCTIONS=RFC_SYSTEM_INFO",
      ].join("\n"),
      "utf8",
    );

    const config = loadConfig({}, { cwd: dir, envSearchRoots: [dir] });

    assert.equal(config.sap.allowedTables.includes("USR41"), true);
    assert.equal(config.sap.allowedTables.includes("E070"), true);
    assert.equal(config.sap.allowedTables.includes("SWWWIHEAD"), true);
    assert.equal(config.sap.allowedFunctions.includes("RFC_SYSTEM_INFO"), true);
    assert.equal(config.sap.allowedFunctions.includes("TH_WPINFO"), true);
  });
});
