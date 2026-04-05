import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { readFile } from "node:fs/promises";
import * as z from "zod/v4";

import { KpiExecutor } from "../kpis/executor.js";
import type { AppConfig, KpiResult, SapClient } from "../types.js";
import { describeError } from "../utils/errors.js";
import {
  buildWrapperCallParameters,
  buildWrapperCatalog,
  buildWrapperProbeResult,
} from "../wrappers/catalog.js";
import { resolveWindow } from "../utils/dates.js";

interface ServerContext {
  config: AppConfig;
  sapClient: SapClient;
  kpiExecutor: KpiExecutor;
}

function asText(payload: unknown): string {
  return JSON.stringify(payload, null, 2);
}

function summarizeKpis(results: KpiResult[]): string {
  return results
    .map((result) => {
      if (result.status === "ok") {
        const unitSuffix = result.unit ? ` ${result.unit}` : "";
        return `${result.kpiId}: ${result.value ?? "n/a"}${unitSuffix}`;
      }

      return `${result.kpiId}: ${result.status} (${result.notes.join("; ")})`;
    })
    .join("\n");
}

async function readProjectDoc(name: string): Promise<string> {
  const fileUrl = new URL(`../../../docs/${name}`, import.meta.url);
  return readFile(fileUrl, "utf8");
}

export function createSapMcpServer(context: ServerContext): McpServer {
  const server = new McpServer({
    name: "hypercare-sap-mcp-server",
    version: "0.1.0",
  });
  const getWrapperCatalog = () =>
    buildWrapperCatalog(
      context.kpiExecutor.listDefinitions(),
      context.config.sap.allowedFunctions,
    );

  server.registerResource(
    "sap-wrapper-contracts",
    "hypercare://sap/wrapper-contracts",
    {
      title: "SAP Wrapper Contracts",
      description: "ABAP-side contract for ZHC_* SAP KPI wrapper RFCs.",
      mimeType: "text/markdown",
    },
    async (uri) => ({
      contents: [
        {
          uri: uri.toString(),
          mimeType: "text/markdown",
          text: await readProjectDoc("sap-wrapper-contracts.md"),
        },
      ],
    }),
  );

  server.registerResource(
    "sap-kpi-capture-matrix",
    "hypercare://sap/kpi-capture-matrix",
    {
      title: "SAP KPI Capture Matrix",
      description: "Feasibility matrix for SAP KPIs in the Hypercare MCP scope.",
      mimeType: "text/markdown",
    },
    async (uri) => ({
      contents: [
        {
          uri: uri.toString(),
          mimeType: "text/markdown",
          text: await readProjectDoc("sap-kpi-capture-matrix.md"),
        },
      ],
    }),
  );

  server.registerResource(
    "sap-wrapper-backlog",
    "hypercare://sap/wrapper-backlog",
    {
      title: "SAP Wrapper Backlog",
      description:
        "Implementation backlog and acceptance criteria for the first SAP ZHC_* wrappers.",
      mimeType: "text/markdown",
    },
    async (uri) => ({
      contents: [
        {
          uri: uri.toString(),
          mimeType: "text/markdown",
          text: await readProjectDoc("sap-wrapper-backlog.md"),
        },
      ],
    }),
  );

  server.registerResource(
    "sap-wrapper-catalog",
    "hypercare://sap/wrapper-catalog",
    {
      title: "SAP Wrapper Catalog",
      description:
        "Live wrapper catalog derived from the KPI registry and the configured allowlist.",
      mimeType: "application/json",
    },
    async (uri) => ({
      contents: [
        {
          uri: uri.toString(),
          mimeType: "application/json",
          text: asText({ wrappers: getWrapperCatalog() }),
        },
      ],
    }),
  );

  server.registerTool(
    "sap_connection_check",
    {
      title: "SAP Connection Check",
      description:
        "Validates whether the MCP server is configured to reach SAP via RFC.",
      inputSchema: {
        probeTable: z.string().optional(),
        probeFields: z.array(z.string()).min(1).optional(),
        readerFunction: z.string().optional(),
      },
      annotations: {
        readOnlyHint: true,
      },
    },
    async ({ probeTable, probeFields, readerFunction }) => {
      let reachable = false;
      let error: string | undefined;
      let tableReadProbe:
        | {
            ok: boolean;
            table: string;
            fields: string[];
            readerFunction?: string;
            error?: string;
          }
        | undefined;

      try {
        reachable = await context.sapClient.ping();
      } catch (cause) {
        error = describeError(cause);
      }

      if (probeTable) {
        const fields = probeFields ?? ["MANDT"];

        try {
          const probe = await context.sapClient.readTable({
            table: probeTable,
            fields,
            rowCount: 1,
            readerFunction,
          });
          tableReadProbe = {
            ok: true,
            table: probeTable,
            fields,
            readerFunction: probe.readerFunction,
          };
        } catch (cause) {
          tableReadProbe = {
            ok: false,
            table: probeTable,
            fields,
            readerFunction,
            error: describeError(cause),
          };
        }
      }

      const payload = {
        configured: Boolean(context.config.sap.connectionParameters),
        reachable,
        host: context.config.host,
        port: context.config.port,
        connectionMode: context.config.sap.connectionMode,
        configSources: context.config.sap.configSources,
        configWarnings: context.config.sap.configWarnings,
        tableReadFunctions: context.sapClient.getDiagnostics().tableReadFunctions,
        activeTableReadFunction:
          context.sapClient.getDiagnostics().activeTableReadFunction,
        allowedTables: context.config.sap.allowedTables,
        allowedFunctions: context.config.sap.allowedFunctions,
        tableReadProbe,
        error,
      };

      return {
        content: [{ type: "text", text: asText(payload) }],
        structuredContent: payload,
      };
    },
  );

  server.registerTool(
    "sap_wrapper_catalog",
    {
      title: "SAP Wrapper Catalog",
      description:
        "Lists the ZHC_* wrapper families expected by this MCP server and whether they are allowlisted.",
      inputSchema: {
        functionNames: z.array(z.string()).optional(),
        includeKpis: z.boolean().optional(),
      },
      annotations: {
        readOnlyHint: true,
      },
    },
    async ({ functionNames, includeKpis }) => {
      const requestedFunctions =
        functionNames?.map((value) => value.trim().toUpperCase()) ?? [];
      const catalog = getWrapperCatalog()
        .filter((entry) =>
          requestedFunctions.length > 0
            ? requestedFunctions.includes(entry.functionName)
            : true,
        )
        .map((entry) => ({
          functionName: entry.functionName,
          allowlisted: entry.allowlisted,
          kpiCount: entry.kpis.length,
          categories: entry.categories,
          blockers: entry.blockers,
          kpis: includeKpis === false ? undefined : entry.kpis,
        }));

      return {
        content: [{ type: "text", text: asText({ wrappers: catalog }) }],
        structuredContent: { wrappers: catalog },
      };
    },
  );

  server.registerTool(
    "sap_wrapper_probe",
    {
      title: "SAP Wrapper Probe",
      description:
        "Calls one ZHC_* wrapper function and validates the returned payload against the shared wrapper contract.",
      inputSchema: {
        functionName: z.string(),
        from: z.string().optional(),
        to: z.string().optional(),
        dimensions: z.record(z.string(), z.string()).optional(),
        expectedKpiIds: z.array(z.string()).optional(),
      },
      annotations: {
        readOnlyHint: true,
      },
    },
    async ({ functionName, from, to, dimensions, expectedKpiIds }) => {
      const normalizedFunctionName = functionName.trim().toUpperCase();
      const catalogEntry = getWrapperCatalog().find(
        (entry) => entry.functionName === normalizedFunctionName,
      );
      const input = { from, to, dimensions };
      const window = resolveWindow(input, 24);
      const effectiveExpectedKpiIds =
        expectedKpiIds && expectedKpiIds.length > 0
          ? expectedKpiIds
          : catalogEntry?.kpis.map((kpi) => kpi.wrapperKpiId) ?? [];

      try {
        const rawResult = await context.sapClient.call(
          normalizedFunctionName,
          buildWrapperCallParameters(input),
        );
        const payload = {
          ...buildWrapperProbeResult({
            functionName: normalizedFunctionName,
            result: rawResult,
            expectedKpiIds: effectiveExpectedKpiIds,
            window,
          }),
          catalogMatched: Boolean(catalogEntry),
          allowlisted: context.config.sap.allowedFunctions.includes(
            normalizedFunctionName,
          ),
          documentedKpiIds: catalogEntry?.kpis.map((kpi) => kpi.wrapperKpiId) ?? [],
        };

        return {
          content: [{ type: "text", text: asText(payload) }],
          structuredContent: payload,
        };
      } catch (error) {
        const payload = {
          ok: false,
          functionName: normalizedFunctionName,
          catalogMatched: Boolean(catalogEntry),
          allowlisted: context.config.sap.allowedFunctions.includes(
            normalizedFunctionName,
          ),
          expectedKpiIds: effectiveExpectedKpiIds,
          error: describeError(error),
        };

        return {
          content: [{ type: "text", text: asText(payload) }],
          structuredContent: payload,
        };
      }
    },
  );

  server.registerTool(
    "sap_kpi_catalog",
    {
      title: "SAP KPI Catalog",
      description:
        "Lists KPI IDs, maturity, source type, and blockers for this SAP MCP server.",
      inputSchema: {
        maturity: z
          .array(
            z.enum([
              "implemented",
              "planned",
              "custom_abap_required",
              "excluded",
            ]),
          )
          .optional(),
      },
      annotations: {
        readOnlyHint: true,
      },
    },
    async ({ maturity }) => {
      const definitions = context.kpiExecutor
        .listDefinitions()
        .filter((definition) =>
          maturity && maturity.length > 0
            ? maturity.includes(definition.maturity)
            : true,
        )
        .map((definition) => ({
          id: definition.id,
          title: definition.title,
          category: definition.category,
          tier: definition.tier,
          maturity: definition.maturity,
          summary: definition.summary,
          source: definition.source,
          notes: definition.notes ?? [],
          blocker:
            "blocker" in definition ? definition.blocker : undefined,
          wrapper:
            "wrapper" in definition ? definition.wrapper : undefined,
        }));

      return {
        content: [{ type: "text", text: asText(definitions) }],
        structuredContent: { definitions },
      };
    },
  );

  server.registerTool(
    "sap_kpi_read",
    {
      title: "SAP KPI Read",
      description:
        "Reads one or more SAP KPIs through the registry-driven execution layer.",
      inputSchema: {
        kpiIds: z.array(z.string()).min(1),
        from: z.string().optional(),
        to: z.string().optional(),
        dimensions: z.record(z.string(), z.string()).optional(),
      },
      annotations: {
        readOnlyHint: true,
      },
    },
    async ({ kpiIds, from, to, dimensions }) => {
      const results = await context.kpiExecutor.runMany(kpiIds, {
        from,
        to,
        dimensions,
      });

      return {
        content: [{ type: "text", text: summarizeKpis(results) }],
        structuredContent: { results },
      };
    },
  );

  server.registerTool(
    "sap_table_read",
    {
      title: "SAP Table Read",
      description:
        "Reads an allowlisted SAP table using the configured S/4-compatible table-reader function chain. Intended for controlled extraction and validation.",
      inputSchema: {
        table: z.string(),
        fields: z.array(z.string()).min(1),
        where: z.array(z.string()).optional(),
        rowCount: z.number().int().min(1).max(1000).optional(),
        rowSkips: z.number().int().min(0).optional(),
        readerFunction: z.string().optional(),
      },
      annotations: {
        readOnlyHint: true,
      },
    },
    async ({ table, fields, where, rowCount, rowSkips, readerFunction }) => {
      try {
        const result = await context.sapClient.readTable({
          table,
          fields,
          where,
          rowCount,
          rowSkips,
          readerFunction,
        });

        return {
          content: [{ type: "text", text: asText(result) }],
          structuredContent: {
            ok: true,
            table: result.table,
            fields: result.fields,
            rows: result.rows,
            rowCount: result.rowCount,
            truncated: result.truncated,
            readerFunction: result.readerFunction,
          },
        };
      } catch (error) {
        const payload = {
          ok: false,
          table,
          fields,
          readerFunction,
          error: describeError(error),
        };

        return {
          content: [{ type: "text", text: asText(payload) }],
          structuredContent: payload,
        };
      }
    },
  );

  server.registerTool(
    "sap_function_call",
    {
      title: "SAP Function Call",
      description:
        "Calls an allowlisted read-only RFC function module. This is the future entry point for custom ZHC_* wrappers.",
      inputSchema: {
        functionName: z.string(),
        parameters: z.record(z.string(), z.unknown()).optional(),
      },
      annotations: {
        readOnlyHint: true,
      },
    },
    async ({ functionName, parameters }) => {
      try {
        const result = await context.sapClient.call(functionName, parameters ?? {});

        return {
          content: [{ type: "text", text: asText(result) }],
          structuredContent: {
            ok: true,
            functionName: functionName.trim().toUpperCase(),
            result,
          },
        };
      } catch (error) {
        const payload = {
          ok: false,
          functionName: functionName.trim().toUpperCase(),
          error: describeError(error),
        };

        return {
          content: [{ type: "text", text: asText(payload) }],
          structuredContent: payload,
        };
      }
    },
  );

  return server;
}
