# SAP Wrapper Backlog

## Purpose

This backlog is the ABAP-side handoff for the first custom SAP RFC wrappers required by the Hypercare MCP server.

These wrappers are the correct place for KPI logic that is:

- business-rule heavy
- security-sensitive
- cross-table
- too expensive or too ambiguous for repeated generic reads

The Node MCP server is already prepared to:

- call these wrappers through `sap_kpi_read`
- probe them directly through `sap_wrapper_probe`
- validate the returned contract shape

## Priority 1

### `ZHC_GET_SECURITY_KPIS`

Expected KPI IDs:

- `authorization_failures`
- `users_with_sod_conflicts`
- `emergency_access_sessions`
- `expired_password_pct`

Why first:

- hypercare always needs early visibility into access-control issues
- these KPIs should not be inferred from naive raw-table counts

Acceptance criteria:

- remote-enabled function module
- returns `ET_KPIS` rows for all four KPI IDs
- returns `EV_SCHEMA_VERSION = '1.0'`
- uses `ET_MESSAGES` for warnings instead of raising hard dumps for recoverable gaps

Suggested SAP objects:

- Security Audit Log source
- GRC or firefighter source if present
- `USR02`
- role and authorization evaluation logic

### `ZHC_GET_OTC_KPIS`

Expected KPI IDs:

- `failed_sales_orders`
- `atp_check_failures`
- `stuck_sales_documents`
- `stuck_delivery_documents`

Why first:

- OTC blockers show up immediately after go-live
- "failed" and "stuck" are business definitions, not technical field counts

Acceptance criteria:

- one agreed rule set per KPI
- dimensions at minimum support `sales_org`, `distribution_channel`, `division`, `doc_type`, `age_days`
- aging logic is implemented in SAP, not reconstructed in MCP

Suggested SAP objects:

- `VBAK`
- `VBUK`
- `VBEP`
- delivery status sources used in your landscape

### `ZHC_GET_P2P_KPIS`

Expected KPI IDs:

- `po_creation_errors`
- `invoice_match_failures`
- `gr_ir_mismatch`
- `gr_posting_failures`

Why first:

- P2P exceptions are common hypercare pain points
- these KPIs usually depend on workflow, matching, and reconciliation logic

Acceptance criteria:

- one wrapper call returns all four KPIs
- business-approved exception rules are coded in ABAP
- returned KPI IDs match the Node registry exactly

Suggested SAP objects:

- `EKKO`
- `EKBE`
- `RBKP`
- movement and invoice verification sources used in your system

### `ZHC_GET_FINANCE_KPIS`

Expected KPI IDs:

- `payment_run_errors`
- `period_end_closing_errors`
- `asset_inconsistencies`
- `reconciliation_imbalance_alerts`

Why first:

- finance wants trusted exception counts, not approximations
- close and reconciliation logic should stay close to SAP controls

Acceptance criteria:

- supports `company_code`, `ledger`, `fiscal_year`, `period`
- returns warnings in `ET_MESSAGES` when a company code or ledger is not in scope
- no direct table internals exposed to the caller

Suggested SAP objects:

- payment-run sources used in FI
- close-control logic
- asset reconciliation objects
- GL balance and reconciliation logic

## Priority 2

### `ZHC_GET_DATA_QUALITY_KPIS`

Expected KPI IDs:

- `missing_mandatory_fields`
- `duplicate_entries`
- `cvi_bp_inconsistencies`
- `data_migration_reconciliation_errors`

Reason:

- the logic is object-specific and rule-specific

### `ZHC_GET_JOB_KPIS`

Expected KPI IDs:

- `job_restart_success_rate`

Reason:

- restart correlation is better implemented near SAP job logic than through repeated table scans

### `ZHC_GET_MANUFACTURING_KPIS`

Expected KPI IDs:

- `stuck_production_orders`
- `backflush_failures`

### `ZHC_GET_SERVICE_KPIS`

Expected KPI IDs:

- `service_calls`
- `techs_dispatched`
- `parts_consumed`

### `ZHC_GET_TAX_KPIS`

Expected KPI IDs:

- `tax_reports`
- `vat_corrections`
- `audit_files`

### `ZHC_GET_EAM_KPIS`

Expected KPI IDs:

- `equip_installed`

## Delivery Checklist

For each wrapper:

1. Create the FM as remote-enabled.
2. Implement the shared contract from `sap-wrapper-contracts.md`.
3. Add the FM to `SAP_ALLOWED_FUNCTIONS` in the MCP deployment config.
4. Probe it with `sap_wrapper_probe`.
5. Validate that `sap_kpi_read` resolves the wrapper-backed KPI IDs without fallback notes.

## Probe Workflow

After transport to QA or production:

1. Run `sap_connection_check` to confirm RFC reachability.
2. Run `sap_wrapper_catalog` to confirm the wrapper is expected and allowlisted.
3. Run `sap_wrapper_probe` for the wrapper function.
4. Run `sap_kpi_read` for the covered KPI IDs.

If step 3 fails, fix SAP or allowlist issues before debugging dashboard behavior.
