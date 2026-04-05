# SAP KPI Capture Matrix

## Scope

This document answers one question:

- which KPIs can be captured through the SAP RFC MCP server
- which cannot be captured cleanly
- why
- what the practical solution is

This matrix is for the SAP RFC MCP only.

Explicitly excluded from this document:

- all ServiceNow KPIs
- all HANA-native KPIs

HANA-native KPIs intentionally skipped:

- Database uptime %
- HANA memory consumption
- DB response time
- Table growth rate
- Column vs row store usage
- Expensive SQL statements
- Unbalanced partition alerts

## Status Meaning

- `Direct`: can be captured through the current MCP shape using standard RFCs, allowlisted table reads, or simple derived logic.
- `Wrapper`: should not be captured as raw table scans in production; use a custom SAP RFC wrapper such as `ZHC_*`.
- `Outside`: should not be computed inside the SAP extraction layer.

## Summary

- `47` KPIs are `Direct`
- `31` KPIs are `Wrapper`
- `1` KPI is `Outside`

This is capture feasibility, not current code completion. Some `Direct` KPIs are not coded yet, but they fit the current MCP architecture cleanly.

## Already Wired In This Repo

The current scaffold already has registry entries for:

- Active user count
- Unauthorized login attempts
- Failed login attempts
- Failed job count
- Delayed job count
- Long-running job count
- Job success rate
- ABAP dump frequency
- Background job throughput
- Total IDocs processed
- IDocs in error
- Reprocessing success rate
- IDoc backlog volume
- Locked users
- Inactive users
- RFC user password age
- Posting errors
- Unposted billing documents
- Delivery block rate
- AP Invoices
- AR Invoices
- GL Posted
- Work Orders
- Notifications
- POs Created
- Materials Created

## System Connectivity & Availability

| KPI | Status | Why | Solution |
| --- | --- | --- | --- |
| SAP application uptime % | Wrapper | Historical uptime % cannot be trusted from one live RFC snapshot. | Persist polling history in your scheduler or build `ZHC_GET_SYSTEM_UPTIME`. |
| Application server uptime per instance | Direct | `TH_SERVER_LIST` can expose live instance status and start time. | Add a KPI resolver over `TH_SERVER_LIST`. |
| Average system restart frequency | Direct | Restart events can be counted from SM21 via `RSLG_GET_MESSAGES`. | Filter startup and shutdown log events by window. |
| Active user count | Direct | Active sessions can be derived from live SAP session sources such as `USR41`. | Expose as a live-count KPI with small polling intervals. |
| Peak concurrent users | Direct | `SWNC_COLLECTOR_GET_AGGREGATES` already stores historical workload peaks. | Read the appropriate aggregate interval instead of sampling live sessions. |
| Unauthorized login attempts | Direct | Feasible only if Security Audit Log is enabled and retained. | Make SAL enablement a readiness prerequisite and read `RSECACTPROT`. |
| License utilization % | Direct | `SLIC_GET_INSTALLATIONS` can provide the license-side numbers. | Derive used vs entitled in KPI logic. |

## System Performance

| KPI | Status | Why | Solution |
| --- | --- | --- | --- |
| Dialog response time | Direct | Standard workload collector metric from `SWNC_COLLECTOR_GET_AGGREGATES`. | Expose directly through the KPI registry. |
| Update task response time | Direct | Can be derived from `VBHDR` plus work process state from `TH_WPINFO`. | Keep logic in the KPI layer and cap scan volume. |
| Background job throughput | Direct | `TBTCO` is the standard job status store. | Count completed jobs per requested window. |
| Work process utilization | Direct | `TH_WPINFO` gives live dispatcher work process state. | Derive busy vs idle per work process type. |
| CPU utilization % | Direct | SAP monitoring via CCMS can expose this through RFC. | Use `BAPI_SYSTEM_MON_GETSYSINFO` or the approved monitoring FM for your system. |
| Memory utilization % | Direct | Application-layer memory is available through standard monitoring data. | Read the approved monitoring source and normalize in the KPI layer. |
| ABAP dump frequency (ST22) | Direct | `SNAP` is the standard dump source. | Count dumps by date window. |
| System log errors (SM21) | Direct | `RSLG_GET_MESSAGES` reads the system log directly. | Filter by severity and window. |
| Gateway errors | Direct | Gateway and ICM monitoring can expose these counts. | Read the approved gateway monitoring FM and normalize categories. |
| Timeout errors | Direct | Workload statistics capture timeout events. | Pull timeout-related workload counters through the same KPI pipeline. |
| Lock table overflows | Direct | Enqueue statistics are available through standard RFC monitoring. | Use `ENQUEUE_STATISTICS` and normalize overflow counters. |

## Job & Batch Monitoring

| KPI | Status | Why | Solution |
| --- | --- | --- | --- |
| Failed job count | Direct | `TBTCO` status logic is straightforward. | Count aborted jobs in the time window. |
| Delayed job count | Direct | Scheduled vs actual start time is available in job data. | Derive delay threshold in KPI logic. |
| Long-running job count | Direct | Running duration can be calculated from job timestamps. | Use a configurable duration threshold or job baseline. |
| Batch window utilization % | Direct | Job start and end times can be rolled into a utilization percentage. | Compute window usage in the KPI layer or scheduler. |
| Job success rate | Direct | Finished vs failed jobs are standard status counters. | Calculate ratio from the same `TBTCO` window. |
| Job restart success rate | Wrapper | Restart correlation across reruns is not reliable as a naive raw-table count. | Build `ZHC_GET_JOB_RECOVERY_KPIS` or correlate in a wrapper. |
| Job prediction accuracy (AI) | Outside | This is downstream analytics, not source extraction. | Compute it after KPI history is stored in PostgreSQL. |

## Integration & Interfaces

| KPI | Status | Why | Solution |
| --- | --- | --- | --- |
| Total IDocs processed | Direct | `EDIDC` is the standard control record source. | Count by direction, type, and date window as needed. |
| IDocs in error | Direct | Standard error statuses are available in IDoc control data. | Normalize the chosen status list and keep it consistent. |
| Reprocessing success rate | Direct | `EDIDS` contains the status transition history required for success-after-error logic. | Derive error-to-success transitions in KPI logic. |
| IDoc backlog volume | Direct | Waiting and backlog statuses can be counted from standard IDoc state. | Normalize the status list once in the registry. |
| Failed API calls | Direct | HTTP-side failures can be read from ICM monitoring. | Count 4xx and 5xx classes as part of one API KPI family. |
| API response time | Direct | Workload and HTTP monitoring can provide the timing signal. | Pick one canonical source and keep the definition fixed. |
| Retry attempt count | Direct | Async RFC retry state is persisted in SAP queue tables. | Read retry counters from the approved async RFC source. |
| Queue lock failures | Direct | qRFC lock contention is visible in queue state tables. | Count failed or blocked queue states via allowlisted table access. |

## Security & Authorization

| KPI | Status | Why | Solution |
| --- | --- | --- | --- |
| Authorization failures (SU53) | Wrapper | SU53 is a user-session diagnostic, not a trustworthy system-wide KPI source. | Build a security wrapper over Security Audit Log or your chosen security event source. |
| Users with SoD conflicts | Wrapper | SoD is a rule-engine problem, not a simple SAP table count. | Read GRC results or implement `ZHC_GET_SOD_CONFLICTS`. |
| Locked users | Direct | Lock flags are available in user master data. | Count `USR02` lock states through allowlisted reads. |
| Inactive users | Direct | Last-logon date is standard user data. | Use a configurable inactivity threshold. |
| Emergency access sessions | Wrapper | Only reliable if GRC Firefighter or an equivalent control layer exists. | Expose through a GRC-aware wrapper such as `ZHC_GET_FIREFIGHTER_USAGE`. |
| Failed login attempts | Direct | Security Audit Log can provide this cleanly. | Treat SAL enablement and retention as a prerequisite. |
| Expired password % | Wrapper | This needs password-age policy logic plus a clean active-user denominator. | Build a security wrapper using `USR02` and system password policy. |
| RFC user password age | Direct | Technical-user password age is derivable from standard user data. | Filter system or service users and compute age in days. |

## Data Consistency & Master Data

| KPI | Status | Why | Solution |
| --- | --- | --- | --- |
| Missing mandatory fields | Wrapper | Mandatory-field logic varies by object, country, and business rule. | Create object-specific wrappers such as `ZHC_GET_BP_DATA_QUALITY`. |
| Duplicate entries | Wrapper | Duplicate detection needs business matching logic, not raw counts. | Implement a wrapper with the approved duplicate rules. |
| CVI/BP inconsistencies | Wrapper | Cross-object reconciliation should be packaged once, not rebuilt in MCP calls. | Build `ZHC_GET_CVI_INCONSISTENCIES`. |
| Data migration reconciliation errors | Wrapper | This depends on which migration tool and logs still exist post go-live. | Wrap the retained migration reconciliation source if it still exists. |
| Stuck sales documents | Wrapper | "Stuck" is a business-state rule, not one field. | Build `ZHC_GET_SD_BLOCKERS` with agreed status and aging rules. |
| Stuck delivery documents | Wrapper | Delivery blockage needs combined status and aging interpretation. | Create a dedicated SD or LE wrapper. |
| GR/IR mismatch | Wrapper | This is a reconciliation problem across PO history and invoice history. | Build `ZHC_GET_GRIR_EXCEPTIONS`. |
| Replication delays | Direct | Feasible if your replication technology persists delay state in SAP-accessible tables. | Use the actual replication source in your landscape and keep it tool-specific. |

## Business Process KPIs

### OTC, P2P, Manufacturing, Finance

| KPI | Status | Why | Solution |
| --- | --- | --- | --- |
| Failed sales orders | Wrapper | "Failed" order logic depends on multiple blocks, incompletion, and business rules. | Build `ZHC_GET_OTC_KPIS`. |
| Unposted billing documents | Direct | This is a clean status-based counter in billing data. | Count billing docs not transferred to FI. |
| Delivery block rate | Direct | Delivery block can be counted and divided by order volume. | Keep one fixed definition for numerator and denominator. |
| ATP check failures | Wrapper | ATP failure semantics vary by schedule line and business rule. | Wrap the agreed ATP-failure logic in SAP. |
| PO creation errors | Wrapper | Workflow and release errors are not one clean raw-table count. | Build a P2P wrapper that packages the actual failure criteria. |
| GR posting failures | Wrapper | Missing goods movement documents alone do not prove the right business error. | Wrap the exact movement and error logic in SAP. |
| Invoice match failures | Wrapper | Parked or blocked invoice documents are not always true 3-way match failures. | Build a wrapper with the finance-approved definition. |
| Payment run errors | Wrapper | Payment failure semantics are process-specific and often cross-table. | Package this in a finance wrapper. |
| Stuck production orders | Wrapper | "Stuck" needs status plus elapsed-time logic. | Build a manufacturing wrapper with aging thresholds. |
| MRP errors | Direct | Error messages can be counted from the planning message source. | Normalize the chosen error message types in one place. |
| Backflush failures | Wrapper | Failure semantics usually need confirmation context, not a flat table read. | Wrap the manufacturing exception logic in SAP. |
| Posting errors | Direct | A broad posting-exception counter can be derived from document status. | Start with a simple status-based KPI and tighten later if needed. |
| Period-end closing errors | Wrapper | Close errors are workflow- and sequence-driven, not a simple table count. | Build `ZHC_GET_CLOSE_EXCEPTIONS`. |
| Asset inconsistencies | Wrapper | Asset integrity checks require reconciliation logic across asset data sets. | Package the reconciliation in a finance wrapper. |
| Reconciliation imbalance alerts | Wrapper | This is financial control logic, not a raw table counter. | Build `ZHC_GET_GL_IMBALANCES`. |

### FSM, RTR, TAX, EAM, PTP

| KPI | Status | Why | Solution |
| --- | --- | --- | --- |
| Service Calls | Wrapper | The business definition depends on module usage and notification typing. | Build a service-management wrapper with the agreed object types. |
| Techs Dispatched | Wrapper | Dispatch requires assignment logic, not a plain document count. | Wrap the assignment or resource source used in your process. |
| Parts Consumed | Wrapper | Consumption must be tied to the correct service context. | Build a wrapper that links movements to service execution. |
| AP Invoices | Direct | This is a straightforward finance document volume counter. | Count AP invoice documents by posting window. |
| AR Invoices | Direct | This is a straightforward billing or FI volume counter. | Count AR invoice documents by posting window. |
| GL Posted | Direct | This is a straightforward GL document volume counter. | Count posted GL documents by window and company code if needed. |
| Tax Reports | Wrapper | Tax reporting is localization- and process-specific. | Build a country-aware tax wrapper. |
| VAT Corrections | Wrapper | VAT correction logic is not a generic raw-table count. | Package the country-specific exception logic in SAP. |
| Audit Files | Wrapper | Audit-file generation is a reporting process, not a plain transactional counter. | Expose through a tax or compliance wrapper. |
| Work Orders | Direct | Work order volume is a clean standard object count. | Count work orders by type and date window. |
| Notifications | Direct | Notification volume is a clean standard object count. | Count notifications by type and date window. |
| Equip. Installed | Wrapper | The current KPI definition is ambiguous and the table mapping is weak. | Lock the business definition first, then build a wrapper. |
| POs Created | Direct | Purchase order volume is a clean document count. | Count PO headers by creation window. |
| Materials Created | Direct | Material creation volume is a clean master-data count. | Count created materials by creation window. |
| Goods Receipts | Direct | Goods receipt volume is a clean movement counter once movement types are fixed. | Count GR documents or movements with an agreed movement-type list. |

## Hard Conclusions

- Do not model all business-process KPIs as raw generic table-read calls.
- Do not treat SU53 as a system-wide KPI source.
- Do not compute AI KPIs inside the SAP extractor.
- Do not expose every KPI as a separate MCP tool.

## Recommended Next Build Order

1. Implement all `Direct` technical KPIs first:
   - system
   - jobs
   - IDocs
   - security basics
2. Add the `Direct` business volume counters next:
   - AP Invoices
   - AR Invoices
   - GL Posted
   - Work Orders
   - Notifications
   - POs Created
   - Materials Created
   - Goods Receipts
3. Then define and build the first custom wrappers:
   - `ZHC_GET_SECURITY_KPIS`
   - `ZHC_GET_OTC_KPIS`
   - `ZHC_GET_P2P_KPIS`
   - `ZHC_GET_FINANCE_KPIS`
   - `ZHC_GET_DATA_QUALITY_KPIS`

That is the cleanest path to a reliable hypercare dashboard.
