import { createMcpExpressApp } from "@modelcontextprotocol/sdk/server/express.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

import { KpiExecutor } from "./kpis/executor.js";
import { createSapMcpServer } from "./mcp/createServer.js";
import type { AppConfig, SapClient } from "./types.js";
import { describeError } from "./utils/errors.js";

interface HttpServerContext {
  config: AppConfig;
  sapClient: SapClient;
  kpiExecutor: KpiExecutor;
}

function jsonRpcError(message: string) {
  return {
    jsonrpc: "2.0",
    error: {
      code: -32000,
      message,
    },
    id: null,
  };
}

export function createHttpApp(context: HttpServerContext) {
  const app = createMcpExpressApp({ host: context.config.host });

  // ── GET /healthz — Liveness probe with full SAP diagnostics ────────────
  app.get("/healthz", async (_req, res) => {
    let reachable = false;
    let sapError: string | undefined;

    try {
      reachable = await context.sapClient.ping();
    } catch (error) {
      reachable = false;
      sapError = describeError(error);
    }

    const diagnostics = context.sapClient.getDiagnostics();

    res.json({
      ok: true,
      sapConfigured: Boolean(context.config.sap.connectionParameters),
      sapConnectionMode: context.config.sap.connectionMode,
      sapConfigSources: context.config.sap.configSources,
      sapConfigWarnings: context.config.sap.configWarnings,
      sapReachable: reachable,
      sapError,
      circuitBreakerState: diagnostics.circuitBreakerState,
      totalRfcCalls: diagnostics.totalCalls,
      totalRfcFailures: diagnostics.totalFailures,
    });
  });

  // ── GET /readyz — K8s readiness probe ────────────────────────────────
  // Returns 200 if the server is ready to accept MCP requests.
  // Returns 503 if SAP is not configured or the circuit breaker is open.
  app.get("/readyz", (_req, res) => {
    const diagnostics = context.sapClient.getDiagnostics();
    const sapConfigured = Boolean(context.config.sap.connectionParameters);
    const breakerHealthy = diagnostics.circuitBreakerState !== "open";
    const ready = sapConfigured && breakerHealthy;

    res
      .status(ready ? 200 : 503)
      .json({
        ready,
        sapConfigured,
        circuitBreakerState: diagnostics.circuitBreakerState,
      });
  });

  app.post("/mcp", async (req, res) => {
    const server = createSapMcpServer(context);
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
    });

    try {
      await server.connect(transport);
      await transport.handleRequest(req, res, req.body);
    } catch (error) {
      if (!res.headersSent) {
        res.status(500).json(jsonRpcError(String(error)));
      }
    } finally {
      res.on("close", () => {
        void transport.close();
        void server.close();
      });
    }
  });

  app.get("/mcp", (_req, res) => {
    res.status(405).json(jsonRpcError("Method not allowed."));
  });

  app.delete("/mcp", (_req, res) => {
    res.status(405).json(jsonRpcError("Method not allowed."));
  });

  return app;
}
