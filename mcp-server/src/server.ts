import { createMcpExpressApp } from "@modelcontextprotocol/sdk/server/express.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

import { extractBearerToken, verifySapJwt } from "./auth/jwtAuth.js";
import { KpiExecutor } from "./kpis/executor.js";
import { createSapMcpServer } from "./mcp/createServer.js";
import { SapClientPool } from "./sap/clientPool.js";
import type { AppConfig, SapClient } from "./types.js";
import { describeError } from "./utils/errors.js";
import type { Logger } from "./utils/logger.js";
import { createChildLogger } from "./utils/logger.js";

interface HttpServerContext {
  config: AppConfig;
  /** Default SAP client from .env (used when no JWT is provided). */
  defaultSapClient: SapClient;
  /** Multi-tenant connection pool for JWT-authenticated sessions. */
  clientPool: SapClientPool;
  logger: Logger;
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

function generateRequestId(): string {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Resolve the SapClient for this request.
 * If a JWT Bearer token is present and MCP_JWT_SECRET is configured,
 * decode the token and return a session-scoped SapClient from the pool.
 * Otherwise, return the default .env-based SapClient.
 */
async function resolveRequestSapClient(
  authHeader: string | undefined,
  context: HttpServerContext,
): Promise<{ sapClient: SapClient; fingerprint: string }> {
  const token = extractBearerToken(authHeader);

  // No JWT → use default .env client
  if (!token) {
    return { sapClient: context.defaultSapClient, fingerprint: "default" };
  }

  // JWT present but no secret configured → reject
  if (!context.config.jwtSecret) {
    throw new Error(
      "JWT token provided but MCP_JWT_SECRET is not configured on this server.",
    );
  }

  // Decode and verify JWT
  const resolved = verifySapJwt(token, context.config.jwtSecret);

  // Get or create a cached SAP connection pool for these credentials
  const sapClient = await context.clientPool.getOrCreate(
    resolved.fingerprint,
    resolved.connectionParameters,
  );

  return { sapClient, fingerprint: resolved.fingerprint };
}

export function createHttpApp(context: HttpServerContext) {
  const app = createMcpExpressApp({ host: context.config.host });
  const logger = context.logger;

  // Cached healthz state to avoid hammering SAP on every K8s probe
  let lastHealthPing: { reachable: boolean; error?: string; at: number } | undefined;
  const HEALTH_CACHE_MS = 10_000;

  // ── GET /healthz — Liveness probe with full SAP diagnostics ────────────
  app.get("/healthz", async (_req, res) => {
    let reachable = false;
    let sapError: string | undefined;

    const now = Date.now();
    if (lastHealthPing && now - lastHealthPing.at < HEALTH_CACHE_MS) {
      reachable = lastHealthPing.reachable;
      sapError = lastHealthPing.error;
    } else {
      try {
        reachable = await context.defaultSapClient.ping();
      } catch (error) {
        reachable = false;
        sapError = describeError(error);
      }
      lastHealthPing = { reachable, error: sapError, at: now };
    }

    const diagnostics = context.defaultSapClient.getDiagnostics();

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
      jwtAuthEnabled: Boolean(context.config.jwtSecret),
      activePooledConnections: context.clientPool.listActive().length,
    });
  });

  // ── GET /readyz — K8s readiness probe ────────────────────────────────
  // Returns 200 if the server is ready to accept MCP requests.
  // Returns 503 if SAP is not configured or the circuit breaker is open.
  app.get("/readyz", (_req, res) => {
    const diagnostics = context.defaultSapClient.getDiagnostics();
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
    const requestId = generateRequestId();
    const reqLogger = createChildLogger(logger, { requestId });

    // ── JWT-based SAP client resolution ──────────────────────────────
    let sapClient: SapClient;
    let fingerprint: string;

    try {
      const resolved = await resolveRequestSapClient(
        req.headers.authorization,
        context,
      );
      sapClient = resolved.sapClient;
      fingerprint = resolved.fingerprint;
    } catch (error) {
      reqLogger.warn("JWT/SAP client resolution failed", { error: describeError(error) });
      res.status(401).json(jsonRpcError(describeError(error)));
      return;
    }

    // ── Create request-scoped MCP server ─────────────────────────────
    const kpiExecutor = new KpiExecutor(sapClient, fingerprint);
    const server = createSapMcpServer({
      config: context.config,
      sapClient,
      kpiExecutor,
      logger: reqLogger,
    });
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
    });

    let closed = false;
    const cleanup = () => {
      if (closed) return;
      closed = true;
      void transport.close();
      void server.close();
    };

    res.once("close", cleanup);

    try {
      await server.connect(transport);
      await transport.handleRequest(req, res, req.body);
    } catch (error) {
      if (!res.headersSent) {
        res.status(500).json(jsonRpcError(String(error)));
      }
      reqLogger.error("MCP request handling failed", { error: String(error) });
    } finally {
      cleanup();
      // Remove listener in case close already fired
      res.removeListener("close", cleanup);
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
