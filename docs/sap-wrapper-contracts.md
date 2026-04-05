# SAP Custom Wrapper Contracts

## Purpose

These wrapper RFCs package KPI logic that should not be implemented through repeated generic table reads.

The MCP server is already prepared to call these functions through `sap_kpi_read` as soon as they are:

- created in SAP
- remote-enabled
- allowlisted in `SAP_ALLOWED_FUNCTIONS`
- authorized for the RFC user

## Standard Contract

All `ZHC_*` KPI wrappers should use the same interface shape.

### Import Parameters

| Parameter | Type | Required | Purpose |
| --- | --- | --- | --- |
| `IV_FROM_DATE` | `DATS` | No | Lower date bound in `YYYYMMDD`. |
| `IV_TO_DATE` | `DATS` | No | Upper date bound in `YYYYMMDD`. |
| `IV_FROM_TS` | `STRING` or `TIMESTAMPL` | No | ISO timestamp passed through from MCP. |
| `IV_TO_TS` | `STRING` or `TIMESTAMPL` | No | ISO timestamp passed through from MCP. |
| `IV_DIMENSIONS_JSON` | `STRING` | No | Raw JSON dimensions map from MCP. |
| `IT_DIMENSIONS` | table | No | Normalized name/value dimensions. |

Recommended line type for `IT_DIMENSIONS`:

| Field | Type | Purpose |
| --- | --- | --- |
| `NAME` | `CHAR64` | Dimension name |
| `VALUE` | `STRING` | Dimension value |

### Export / Table Parameters

| Parameter | Type | Required | Purpose |
| --- | --- | --- | --- |
| `EV_SCHEMA_VERSION` | `CHAR10` | Yes | Contract version, start with `1.0`. |
| `ET_KPIS` | table | Yes | Normalized KPI results. |
| `ET_MESSAGES` | `BAPIRET2_T` | No | Warnings and diagnostics. |

Recommended line type for `ET_KPIS`:

| Field | Type | Purpose |
| --- | --- | --- |
| `KPI_ID` | `CHAR64` | Stable KPI identifier expected by MCP. |
| `TITLE` | `STRING` | Human-readable KPI title. |
| `CATEGORY` | `CHAR64` | Category label. |
| `STATUS` | `CHAR24` | `OK`, `ERROR`, `PLANNED`, `CUSTOM_ABAP_REQUIRED`, `EXCLUDED`. |
| `UNIT` | `CHAR24` | Unit like `count`, `percent`, `days`. |
| `VALUE_NUM` | `DECFLOAT34` | Numeric KPI value. |
| `VALUE_TEXT` | `STRING` | Optional textual value. |
| `NOTES_JSON` | `STRING` | JSON array of notes. |

The MCP parser currently reads:

- `ET_KPIS`
- `KPI_ID`
- `TITLE`
- `CATEGORY`
- `STATUS`
- `UNIT`
- `VALUE_NUM` or `VALUE`
- `NOTES_JSON`

Keep those exact names if you want zero adapter code on the Node side.

## Wrapper Families

### `ZHC_GET_SECURITY_KPIS`

Expected KPI IDs:

- `authorization_failures`
- `users_with_sod_conflicts`
- `emergency_access_sessions`
- `expired_password_pct`

Suggested dimensions:

- `client`
- `company_code`
- `user_type`
- `days`

### `ZHC_GET_OTC_KPIS`

Expected KPI IDs:

- `failed_sales_orders`
- `atp_check_failures`
- `stuck_sales_documents`
- `stuck_delivery_documents`

Suggested dimensions:

- `sales_org`
- `distribution_channel`
- `division`
- `doc_type`
- `age_days`

### `ZHC_GET_P2P_KPIS`

Expected KPI IDs:

- `po_creation_errors`
- `invoice_match_failures`
- `gr_ir_mismatch`

Suggested dimensions:

- `purch_org`
- `company_code`
- `plant`
- `age_days`

### `ZHC_GET_FINANCE_KPIS`

Expected KPI IDs:

- `payment_run_errors`
- `period_end_closing_errors`
- `asset_inconsistencies`
- `reconciliation_imbalance_alerts`

Suggested dimensions:

- `company_code`
- `ledger`
- `fiscal_year`
- `period`

### `ZHC_GET_DATA_QUALITY_KPIS`

Expected KPI IDs:

- `missing_mandatory_fields`
- `duplicate_entries`
- `cvi_bp_inconsistencies`

Suggested dimensions:

- `object_type`
- `company_code`
- `country`
- `rule_set`

### `ZHC_GET_JOB_KPIS`

Expected KPI IDs:

- `job_restart_success_rate`

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

## ABAP Implementation Notes

- Mark each FM as remote-enabled.
- Do not expose raw table internals to the caller; package the business logic in ABAP.
- Use one wrapper call to return multiple KPIs in the same domain.
- Treat missing prerequisites as warnings in `ET_MESSAGES` and return the affected KPI row with `STATUS = 'ERROR'` or `STATUS = 'CUSTOM_ABAP_REQUIRED'`.
- Keep the result identifiers stable. The Node layer keys off `KPI_ID`.

## MCP Behavior

When these wrappers are present:

1. `sap_kpi_read` will call the wrapper once per family.
2. It will cache the wrapper response for the current request.
3. It will map `ET_KPIS` rows back to the requested KPI IDs.
4. If the wrapper is missing or not allowlisted, the KPI remains in `custom_abap_required` status with the failure reason in `notes`.

That gives you a clean migration path:

- start with direct RFC and safe table reads
- move complex KPIs behind wrapper RFCs
- keep the external MCP contract stable
