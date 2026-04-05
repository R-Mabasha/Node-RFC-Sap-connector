# SAP RFC MCP Validation

## Brutal Verdict

Your current KPI inventory is too broad to expose 1:1 as MCP tools.

The correct shape is:

- Keep the MCP contract small and stable.
- Put KPI logic behind a registry.
- Use standard RFCs only for safe read-only metrics.
- Use custom `ZHC_*` RFC wrappers for heavy, cross-table, or business-rule KPIs.
- Keep ServiceNow completely outside the SAP MCP server.

If you try to expose every KPI as its own tool, or if you rely on generic table-scan FMs for everything, this will become slow, fragile, and hard to secure.

## Main Findings

### 1. The KPI source mapping is internally inconsistent

`kpi-datasource.md` still mixes:

- HANA-native views
- ABAP tables
- RFC function modules
- external systems like ServiceNow

That is not an RFC-only plan. For the SAP MCP server, you need one clean rule:

- SAP MCP only exposes SAP data reachable through RFC.
- HANA-native metrics require either custom ABAP wrappers or a separate HANA connector.
- ServiceNow KPIs belong in a different connector or MCP server.

### 2. Some KPIs are fine as RFC/table-backed reads

These are reasonable Phase 1 candidates:

- Application server uptime per instance
- Average system restart frequency
- Peak concurrent users
- Dialog response time
- Background job throughput
- Work process utilization
- ABAP dump frequency
- System log errors
- Failed job count
- Delayed job count
- Long-running job count
- Job success rate
- Total IDocs processed
- IDocs in error
- Reprocessing success rate
- IDoc backlog volume
- Locked users
- Inactive users
- RFC user password age
- Unposted billing documents
- Delivery block rate

These are either direct RFC reads, direct table reads, or simple derived metrics.

### 3. Some KPIs are not safe or clean with standard RFC only

These should move to `custom_abap_required`:

- Authorization failures (system-wide SU53 is the wrong mental model)
- Users with SoD conflicts
- Emergency access sessions
- Expired password %
- Missing mandatory fields
- Duplicate entries
- CVI/BP inconsistencies
- Data migration reconciliation errors
- Stuck sales documents
- Stuck delivery documents
- GR/IR mismatch
- Failed sales orders
- PO creation errors
- GR posting failures
- Invoice match failures
- Payment run errors
- Stuck production orders
- MRP errors
- Backflush failures
- Posting errors
- Period-end closing errors
- Asset inconsistencies
- Reconciliation imbalance alerts

Reason:

- they are cross-table
- they are rule-heavy
- they often need joins, status interpretation, or aggregation
- doing them via repeated generic table scans is operationally bad

The right solution is a curated RFC wrapper per domain, for example:

- `ZHC_GET_SECURITY_KPIS`
- `ZHC_GET_JOB_KPIS`
- `ZHC_GET_IDOC_KPIS`
- `ZHC_GET_OTC_KPIS`
- `ZHC_GET_FINANCE_KPIS`

### 4. Some KPIs should be dropped from the SAP RFC MCP scope

These do not belong in Phase 1 SAP RFC MCP:

- Database uptime %
- HANA memory consumption
- Column vs row store usage
- Expensive SQL statements
- Unbalanced partition alerts
- Job prediction accuracy (AI)
- All ServiceNow ticket/SLA/automation KPIs

Reason:

- HANA internals are not cleanly represented by standard RFC
- AI prediction accuracy is not a source KPI, it is downstream analytics
- ServiceNow is not SAP

### 5. Security logging KPIs are conditional

These are only valid if the underlying logging is enabled and retained:

- Unauthorized login attempts
- Failed login attempts
- Authorization failures

Without Security Audit Log or equivalent configuration, these KPIs are fake confidence.

## Recommended KPI Status Model

Every KPI in your registry should have one of these statuses:

- `implemented`
- `planned`
- `custom_abap_required`
- `excluded`

## Recommended MCP Tool Surface

Do not build one tool per KPI.

Build these five tools:

### `sap_connection_check`

Purpose:

- verify RFC connectivity
- verify configuration presence
- verify `node-rfc` runtime readiness

### `sap_kpi_catalog`

Purpose:

- return supported KPI IDs
- return maturity status
- return source type
- return notes and blockers

### `sap_kpi_read`

Purpose:

- fetch one or many KPI IDs
- accept a time window and dimensions
- route internally through the KPI registry

This should be the main tool your scheduler uses.

### `sap_table_read`

Purpose:

- read allowlisted SAP tables, including custom WRICEF/Z tables
- support validation, debugging, and low-level extraction

### `sap_function_call`

Purpose:

- call allowlisted read-only RFCs
- later call your custom `ZHC_*` wrappers

## One Tool Or Many?

Not one tool.
Not eighty tools.

Use a small tool surface with a registry behind it.

## Future-Proof SAP Connectivity Model

### Transport

- MCP over Streamable HTTP
- stateless mode first

### SAP Connection Layer

Use `node-rfc` with a shared connection pool.

Connection model:

- `sapnwrfc.ini` destination when possible
- fallback to explicit env-based connection parameters
- one pooled SAP connector inside the MCP service

Guardrails:

- small pool sizes
- per-call timeouts
- no arbitrary FM execution
- no unrestricted table reads

## Recommended Implementation Phases

### Phase 1

Build now:

- `sap_connection_check`
- `sap_kpi_catalog`
- `sap_kpi_read`
- `sap_table_read`
- `sap_function_call`

Implement first KPI set:

- jobs
- dumps
- IDocs
- locked/inactive users
- a few simple OTC counters

### Phase 2

Add standard RFC-based system KPIs:

- application server uptime per instance
- restart frequency
- peak users
- dialog response time
- work process utilization

### Phase 3

Add custom ABAP wrappers for business and control-heavy KPIs:

- security
- finance
- OTC/P2P
- master data consistency

### Phase 4

Add downstream analytics outside the SAP extraction layer:

- KPI history
- anomaly detection
- AI prediction accuracy

## Data Flow That Fits Your Architecture

Use this flow:

1. Scheduler calls `sap_kpi_read`.
2. MCP server resolves KPI IDs through the registry.
3. Registry decides whether to use:
   - standard RFC
   - table extraction
   - custom `ZHC_*` RFC wrapper
4. MCP server normalizes the response.
5. Scheduler stores KPI history in PostgreSQL and caches recent values in Redis.
6. Dashboard reads from your app database, not directly from SAP.

## Hard Rules

- Do not expose unrestricted generic table-read FMs to every caller.
- Do not model HANA internals as if they were normal RFC KPIs.
- Do not keep ServiceNow in the SAP MCP boundary.
- Do not create one MCP tool per KPI.
- Do not let the dashboard call SAP directly.

## Final Recommendation

Build the SAP MCP server around:

- a connection pool
- an allowlisted table/function layer
- a registry-driven KPI engine
- custom ABAP wrappers for complex KPIs

That is the only direction here that scales cleanly.
