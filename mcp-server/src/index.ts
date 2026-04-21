import { loadConfig } from "./config.js";
import { SapClientPool } from "./sap/clientPool.js";
import { NodeRfcSapClient } from "./sap/nodeRfcClient.js";
import { createHttpApp } from "./server.js";
import { defaultLogger } from "./utils/logger.js";

const logger = defaultLogger;

async function main() {
  const config = loadConfig();

  if (config.sap.configWarnings.length > 0) {
    for (const warning of config.sap.configWarnings) {
      logger.warn(`[config] ${warning}`);
    }
  }

  if (config.jwtSecret) {
    logger.info("[config] JWT authentication is ENABLED (MCP_JWT_SECRET is set).");
  } else {
    logger.info("[config] JWT authentication is DISABLED. Using .env SAP credentials only.");
  }

  const defaultSapClient = new NodeRfcSapClient(config.sap);
  const clientPool = new SapClientPool(config.sap);

  const app = createHttpApp({
    config,
    defaultSapClient,
    clientPool,
    logger,
  });

  const server = app.listen(config.port, config.host, () => {
    logger.info(
      `SAP MCP server listening on http://${config.host}:${config.port}/mcp`,
    );
    logger.info(
      `SAP connection mode: ${config.sap.connectionMode} (${config.sap.configSources.join(", ") || "process env only"})`,
    );
  });

  const shutdown = async (signal: string) => {
    logger.info(`[shutdown] Received ${signal}, closing connections...`);

    await new Promise<void>((resolve) => {
      server.close(() => {
        logger.info("[shutdown] HTTP server closed.");
        resolve();
      });
    });

    try {
      await defaultSapClient.close();
      logger.info("[shutdown] Default SAP client closed.");
    } catch (e) {
      logger.error("[shutdown] Error closing default SAP client", { error: String(e) });
    }

    try {
      await clientPool.closeAll();
      logger.info("[shutdown] Client pool closed.");
    } catch (e) {
      logger.error("[shutdown] Error closing client pool", { error: String(e) });
    }

    process.exit(0);
  };

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });

  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });
}

void main();
