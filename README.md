# Node RFC SAP Connector

SAP MCP server for Hypercare dashboards and KPI collection.

This repository contains the SAP-facing integration layer that connects to SAP S/4HANA over RFC, exposes a stable MCP interface, and serves KPI data to downstream schedulers, storage, and dashboards.

## What This Repo Contains

- an MCP server built on `node-rfc` and TypeScript
- allowlisted SAP RFC and table access
- KPI execution through a registry instead of one MCP tool per KPI
- wrapper-aware execution for future `ZHC_*` SAP custom RFCs
- validation and design docs for KPI coverage and wrapper contracts

## Current Status

- `52` KPIs are implemented directly in the Node/MCP layer
- `30` KPIs are intentionally wrapper-backed and require SAP-side `ZHC_*` ABAP RFCs
- `3` KPIs are excluded from SAP RFC MCP scope
- direct KPI backlog in the Node registry is complete

## Target Architecture

```text
SAP S/4HANA
  -> Hypercare SAP MCP Server
  -> Scheduler / Polling Layer
  -> Redis + PostgreSQL
  -> Dashboard UI
```

The MCP server is the SAP extraction boundary. Dashboards and schedulers should call stable MCP tools, not SAP tables or RFCs directly.

## Repository Layout

- [`mcp-server/`](./mcp-server) - Node/TypeScript MCP server
- [`docs/`](./docs) - KPI validation, wrapper contracts, and delivery notes
- [`implementation_plan.md`](./implementation_plan.md) - evolving implementation record
- [`kpi.md`](./kpi.md) - original KPI list
- [`kpi-datasource.md`](./kpi-datasource.md) - source notes

## Quick Start

1. Install the SAP NetWeaver RFC SDK on the machine.
2. Open [`mcp-server/`](./mcp-server).
3. Install dependencies:

```bash
npm install
```

4. Configure SAP access in `mcp-server/.env`.
5. Start the MCP server:

```bash
npm run dev
```

6. Verify health:

```bash
curl http://127.0.0.1:3001/healthz
```

Expected healthy shape:

```json
{"ok":true,"sapConfigured":true,"sapReachable":true}
```

## MCP Surface

The main tools exposed by the server are:

- `sap_connection_check`
- `sap_kpi_catalog`
- `sap_kpi_read`
- `sap_table_read`
- `sap_function_call`
- `sap_wrapper_catalog`
- `sap_wrapper_probe`

See [`mcp-server/README.md`](./mcp-server/README.md) for runtime details and KPI coverage.

## Documentation

- [`docs/README.md`](./docs/README.md)
- [`docs/sap-mcp-validation.md`](./docs/sap-mcp-validation.md)
- [`docs/sap-kpi-capture-matrix.md`](./docs/sap-kpi-capture-matrix.md)
- [`docs/sap-wrapper-contracts.md`](./docs/sap-wrapper-contracts.md)
- [`docs/sap-wrapper-backlog.md`](./docs/sap-wrapper-backlog.md)

## Verification

From [`mcp-server/`](./mcp-server):

```bash
npm run typecheck
npm test
```

## Notes

- `.env` is intentionally not committed.
- `dist/`, `node_modules/`, logs, and RFC traces are ignored.
- GitHub may show either `main` or `master`; both currently point to the same project commit.
