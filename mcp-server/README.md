# Hypercare SAP MCP Server

This package scaffolds the SAP-facing MCP layer for Hypercare.

It is built around four principles:

- a shared `node-rfc` connection pool
- a small MCP tool surface
- allowlisted table and function access
- S/4-compatible table reads through a configurable FM chain
- KPI execution through a registry instead of one tool per KPI

## Current Tool Surface

- `sap_connection_check`
- `sap_wrapper_catalog`
- `sap_wrapper_probe`
- `sap_kpi_catalog`
- `sap_kpi_read`
- `sap_table_read`
- `sap_function_call`

## MCP Resources

- `hypercare://sap/wrapper-contracts`
- `hypercare://sap/wrapper-backlog`
- `hypercare://sap/kpi-capture-matrix`
- `hypercare://sap/wrapper-catalog`

## Why This Shape

Your scheduler and dashboard should depend on stable tools, not on SAP implementation detail.

When a KPI changes from:

- raw table read
- to standard RFC
- to custom `ZHC_*` wrapper

the external MCP contract should stay the same.

## Prerequisites

1. Install SAP NetWeaver RFC SDK locally.
2. Install `node-rfc` in this package after the SDK is available.
3. Configure either:
   - `SAP_DEST` through `sapnwrfc.ini`, or
   - direct connection variables in `.env`

The upstream `node-rfc` project currently states that its public repository is not actively maintained, so pinning and runtime verification matter.

## Install

```bash
npm install
```

## Environment Loading

At startup the server loads configuration in this order:

1. `.env`
2. `.env.local`
3. `.env.example` only as a fallback when neither of the above exists
4. live process environment variables override file values

`.env.example` is treated as a convenience fallback for local setup, not as the preferred place for secrets.

## Run

```bash
npm run dev
```

The server starts in stateless Streamable HTTP mode at `http://127.0.0.1:3001/mcp` by default.

## Notes

- `sap_table_read` is allowlist-driven and uses the configured table-reader chain, defaulting to `/BUI/RFC_READ_TABLE` then `BBP_RFC_READ_TABLE`.
- For `BBP_RFC_READ_TABLE` and `/BUI/RFC_READ_TABLE`, the server compacts simple multi-clause filters into one SQL string because this SAP system rejects the same predicates when they are sent as separate `OPTIONS` rows.
- The built-in Hypercare table/function allowlists are always present; `SAP_ALLOWED_TABLES` and `SAP_ALLOWED_FUNCTIONS` add to that baseline instead of replacing it.
- `sap_function_call` is allowlist-driven and is the future entry point for custom `ZHC_*` RFC wrappers.
- Complex business KPIs should move to custom ABAP wrappers instead of repeated generic table scans.
- `sap_kpi_read` now auto-attempts wrapper-backed KPIs when the matching `ZHC_*` FM is allowlisted and available in SAP.
- `sap_kpi_read` runs KPI batches sequentially on purpose. This avoids `BBP_RFC_READ_TABLE` contention and `device or resource busy` failures on busy SAP systems.
- `sap_wrapper_catalog` gives the ABAP and dashboard teams one live view of expected wrapper families and covered KPI IDs.
- `sap_wrapper_probe` is the fastest way to validate a newly transported `ZHC_*` wrapper before debugging dashboard behavior.
- direct SAP connection settings are validated at startup; invalid `SAP_SYSNR`, invalid `SAP_CLIENT`, partial direct credentials, or swapped `SAP_ASHOST` and `SAP_SYSNR` fail fast with explicit errors

## Implemented KPIs

The current registry already exposes these KPI IDs through `sap_kpi_read`:

- `peak_concurrent_users`
- `active_user_count`
- `work_process_utilization`
- `unauthorized_login_attempts`
- `failed_login_attempts`
- `abap_dump_frequency`
- `background_job_throughput`
- `failed_job_count`
- `delayed_job_count`
- `application_server_uptime_per_instance`
- `dialog_response_time`
- `timeout_errors`
- `long_running_job_count`
- `batch_window_utilization_pct`
- `job_success_rate`
- `total_idocs_processed`
- `idocs_in_error`
- `reprocessing_success_rate`
- `idoc_backlog_volume`
- `locked_users`
- `inactive_users`
- `rfc_user_password_age`
- `posting_errors`
- `unposted_billing_documents`
- `delivery_block_rate`
- `ap_invoices`
- `ar_invoices`
- `gl_posted`
- `work_orders`
- `notifications`
- `pos_created`
- `materials_created`
- `number_range_exhaustion_pct`
- `retry_attempt_count`
- `queue_lock_failures`
- `mrp_errors`
- `goods_receipts`
- `transport_request_backlog`
- `work_item_backlog`
- `spool_queue_errors`
- `sap_application_uptime_pct`
- `average_system_restart_frequency`
- `license_utilization_pct`
- `update_task_response_time`
- `cpu_utilization_pct`
- `memory_utilization_pct`
- `system_log_errors`
- `gateway_errors`
- `lock_table_overflows`
- `failed_api_calls`
- `api_response_time`
- `replication_delays`

There is no remaining direct-KPI backlog in the Node registry. The remaining unimplemented items are the SAP-side `ZHC_*` wrapper KPIs.

## Wrapper-Backed KPI Families

The executor is now prepared to consume these SAP custom wrappers when they exist:

- `ZHC_GET_SECURITY_KPIS`
- `ZHC_GET_OTC_KPIS`
- `ZHC_GET_P2P_KPIS`
- `ZHC_GET_FINANCE_KPIS`
- `ZHC_GET_DATA_QUALITY_KPIS`
- `ZHC_GET_JOB_KPIS`
- `ZHC_GET_MANUFACTURING_KPIS`
- `ZHC_GET_SERVICE_KPIS`
- `ZHC_GET_TAX_KPIS`
- `ZHC_GET_EAM_KPIS`

The ABAP-side contract is documented in [../docs/sap-wrapper-contracts.md](../docs/sap-wrapper-contracts.md).
The implementation backlog is documented in [../docs/sap-wrapper-backlog.md](../docs/sap-wrapper-backlog.md).

## Verification

```bash
npm run typecheck
npm test
```
