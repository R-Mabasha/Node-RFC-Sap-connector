import { loadConfig } from "./config.js";
import { KpiExecutor } from "./kpis/executor.js";
import { NodeRfcSapClient } from "./sap/nodeRfcClient.js";
import { createHttpApp } from "./server.js";

async function main() {
  const config = loadConfig();

  if (config.sap.configWarnings.length > 0) {
    for (const warning of config.sap.configWarnings) {
      console.warn(`[config] ${warning}`);
    }
  }

  const sapClient = new NodeRfcSapClient(config.sap);
  const kpiExecutor = new KpiExecutor(sapClient);
  const app = createHttpApp({
    config,
    sapClient,
    kpiExecutor,
  });

  const server = app.listen(config.port, config.host, () => {
    console.log(
      `SAP MCP server listening on http://${config.host}:${config.port}/mcp`,
    );
    console.log(
      `SAP connection mode: ${config.sap.connectionMode} (${config.sap.configSources.join(", ") || "process env only"})`,
    );
  });

  const shutdown = async () => {
    server.close();
    await sapClient.close();
  };

  process.on("SIGINT", () => {
    void shutdown();
  });

  process.on("SIGTERM", () => {
    void shutdown();
  });
}

void main();
