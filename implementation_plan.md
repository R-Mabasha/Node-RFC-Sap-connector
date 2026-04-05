# Hypercare SAP MCP Server — Brutal Validation & Build Plan

## Verdict: 7.5/10 — Solid Foundation, Not Production-Complete

The architecture is correct. The registry-driven KPI engine, 7-tool MCP surface, wrapper contract, and allowlisted RFC access are all the right patterns. But you're running on 26 implemented KPIs out of 87 SAP-backed targets, the Phase 2 RFC-based system KPIs are still stubs, and there are **12+ hypercare-critical KPIs missing entirely from your list**.

---

## 1. KPI Coverage Audit

### ✅ Implemented — 26 KPIs (Live Code)

| KPI | Source | Pattern |
|-----|--------|---------|
| `failed_job_count` | TBTCO | Table count |
| `delayed_job_count` | TBTCO | Table scan + compute |
| `long_running_job_count` | TBTCO | Table scan + duration calc |
| `job_success_rate` | TBTCO | Derived ratio |
| `background_job_throughput` | TBTCO | Table count |
| `active_user_count` | USR41 | Table count |
| `unauthorized_login_attempts` | RSECACTPROT | Table count (SAL) |
| `failed_login_attempts` | RSECACTPROT | Table count (SAL) |
| `abap_dump_frequency` | SNAP | Table count |
| `total_idocs_processed` | EDIDC | Table count |
| `idocs_in_error` | EDIDC | Multi-status count |
| `reprocessing_success_rate` | EDIDS | Scan + transition tracking |
| `idoc_backlog_volume` | EDIDC | Multi-status count |
| `locked_users` | USR02 | Multi-flag count |
| `inactive_users` | USR02 | Date comparison |
| `rfc_user_password_age` | USR02 | Scan + age calc |
| `posting_errors` | BKPF | Table count |
| `unposted_billing_documents` | VBRK | Table count |
| `ap_invoices` | RBKP | Table count |
| `ar_invoices` | VBRK | Table count |
| `gl_posted` | BKPF | Table count |
| `work_orders` | AUFK | Table count |
| `notifications` | QMEL | Table count |
| `pos_created` | EKKO | Table count |
| `materials_created` | MARA | Table count |
| `delivery_block_rate` | VBAK | Derived ratio |

### 🟡 Planned — 21 KPIs (Registry Stubs, No Execute Logic)

These are the **system health KPIs your dashboard needs before wrappers**:

| KPI | Source RFC/Table | Why Critical |
|-----|------------------|--------------|
| `application_server_uptime_per_instance` | TH_SERVER_LIST | Shows which app servers are running |
| `average_system_restart_frequency` | RSLG_GET_MESSAGES | Restart = instability signal |
| `peak_concurrent_users` | SWNC_COLLECTOR_GET_AGGREGATES | Capacity planning |
| `license_utilization_pct` | SLIC_GET_INSTALLATIONS | Compliance |
| `dialog_response_time` | SWNC_COLLECTOR_GET_AGGREGATES | **#1 user experience metric** |
| `update_task_response_time` | VBHDR + TH_WPINFO | Write performance |
| `work_process_utilization` | TH_WPINFO | Resource saturation |
| `cpu_utilization_pct` | BAPI_SYSTEM_MON_GETSYSINFO | Infrastructure health |
| `memory_utilization_pct` | BAPI_SYSTEM_MON_GETSYSINFO | Infrastructure health |
| `system_log_errors` | RSLG_GET_MESSAGES | Critical error count |
| `gateway_errors` | GW_GET_STATISTIC | Integration health |
| `timeout_errors` | SWNC_COLLECTOR_GET_AGGREGATES | System overload signal |
| `lock_table_overflows` | ENQUEUE_STATISTICS | Transaction contention |
| `batch_window_utilization_pct` | TBTCO | Batch capacity |
| `failed_api_calls` | ICM_GET_MONITOR_INFO | Fiori/API health |
| `api_response_time` | SWNC/ICM | API performance |
| `retry_attempt_count` | ARFCSSTATE | Persistent integration failures |
| `queue_lock_failures` | QRFCSSTATE | Queue contention |
| `replication_delays` | IUUC_REPL_CONTENT | Data sync lag |
| `mrp_errors` | MDLG | Planning failures |
| `goods_receipts` | MKPF + MSEG | Inventory throughput |

### 🔴 Custom ABAP Required — 31 KPIs (Wrapper-Backed)

Correctly deferred to `ZHC_*` wrapper RFCs. The wrapper contract in [sap-wrapper-contracts.md](file:///e:/Hypercare/docs/sap-wrapper-contracts.md) is well-designed.

### ⬛ Excluded — 7+ HANA-native KPIs

Correctly excluded from RFC scope: DB uptime, HANA memory, DB response time, table growth, column vs row store, expensive SQL, partition alerts.

> [!TIP]
> Some HANA-excluded KPIs (table growth, expensive SQL) CAN be proxied via custom ABAP wrappers that internally query HANA views. Consider adding `ZHC_GET_DB_HEALTH_KPIS` to the wrapper backlog for Phase 3.

---

## 2. Missing Hypercare-Critical KPIs

> [!CAUTION]
> These KPIs are **not in your list at all** but are essential for any real hypercare dashboard. I've seen SAP go-lives fail because nobody was watching these.

### Priority 1 — Must Add Immediately

| KPI | Source | Type | Why |
|-----|--------|------|-----|
| **Transport Request Health** | `E070` + `E071` + `TPLOG` | Direct | Bad transports are the #1 cause of post-go-live incidents. Track stuck, failed, and recent imports. |
| **Work Item Backlog** | `SWWWIHEAD` | Direct | Workflow items stuck in user inboxes = approvals blocked = processes stalled. |
| **Spool Queue Errors** | `TSP01` | Direct | Failed print/spool requests are immediate user complaints. |
| **Number Range Exhaustion** | `NRIV` | Wrapper | When number ranges run out, document creation stops silently. Need remaining % check. |

### Priority 2 — Add for Production Readiness

| KPI | Source | Type | Why |
|-----|--------|------|-----|
| **RFC Destination Health** | `RFCDES` + probing | Wrapper | Broken RFC links = silent integration failure. Should probe SM59 destinations. |
| **Buffer Hit Ratio** | `SWNC_COLLECTOR_GET_AGGREGATES` | Direct | Poor buffer hit ratio → slow system → user complaints. |
| **SAP Note Implementation Status** | `CWBNTCUST` / `PAT*` tables | Wrapper | Are critical corrections applied? |
| **Emergency Transport Count** | `E070` filtered by time | Direct | Transports outside change window = audit risk. |

### Priority 3 — Differentiators

| KPI | Source | Type | Why |
|-----|--------|------|-----|
| **Document Processing Velocity** | `VBAK`→`LIKP`→`VBRK` chain | Wrapper | Cycle time from order to cash. Shows process health holistically. |
| **User Adoption by T-Code** | `SWNC_COLLECTOR_GET_AGGREGATES` | Direct | Which transactions are users actually using? |
| **Fiori Tile Error Rate** | ICM monitoring | Direct | Fiori apps failing silently on the UI layer. |
| **Table Space Growth Rate** | `DBSTATTABC` | Direct | Already in your capture matrix as feasible. |

---

## 3. Architecture Review

### ✅ What's Done Right

| Aspect | Status | Notes |
|--------|--------|-------|
| Registry-driven KPI engine | ✅ Excellent | KPIs don't leak into MCP tools |
| 7-tool MCP surface | ✅ Correct | Lean, stable API — add KPIs, not tools |
| Wrapper contract | ✅ Well-designed | ET_KPIS/ET_MESSAGES/EV_SCHEMA_VERSION is clean |
| Allowlist security | ✅ Correct | Both tables and functions are gated |
| node-rfc pool | ✅ Production-ready | Configurable low/high, stateless mode |
| HTTP Streamable transport | ✅ Correct | Fits scheduler → MCP → SAP flow |
| MCP SDK 1.29.0 | ✅ Current | Latest stable |
| structuredContent | ✅ Future-proof | Returns both text and structured data |
| Resources for docs | ✅ Good | Wrapper contracts/backlog accessible via MCP |
| Config validation | ✅ Thorough | Detects swapped params, warns on .env.example fallback |

### ⚠️ Issues to Fix

| Issue | Severity | Impact |
|-------|----------|--------|
| `idocsInError` fires **7 parallel RFC calls** (one per status) | Medium | 7x the RFC connections needed. Use `IN` clause: `STATUS IN ('51','52','56','63','65','66','69')` |
| `unauthorized_login_attempts` and `failed_login_attempts` are **identical code** | Low | Same WHERE clause, same table, same logic. They're duplicates measuring the same thing. |
| No **tiered polling metadata** on KPI definitions | High | Your scheduler needs to know: realtime (1m) vs batch (30m) vs daily. Currently missing from the registry. |
| No **circuit breaker** for SAP connection | Medium | If SAP is down, the server hammers it with every scheduler call. |
| No **/readyz** endpoint for K8s | Low | Only `/healthz` exists. Add readiness probe. |
| No **RFC call metrics** | Medium | No latency/error rate telemetry. Blind to performance degradation. |
| Server creates **new McpServer per POST** | Low | Correct for stateless, but initialization overhead per request. |

### ❌ Missing for Production

| Gap | Impact | Recommendation |
|-----|--------|----------------|
| No multi-system support | Can't monitor DEV/QA/PRD or multi-region PRD | Add system alias parameter to KPI reads |
| No SNC authentication | Won't pass enterprise security review | Add SNC config options alongside user/pass |
| No message server routing | No HA in production | Add mshost/msserv/R3NAME connection mode |
| No response caching with TTL | Daily-tier KPIs re-hit SAP on every call | Add optional TTL cache per tier |

---

## 4. How the Data Flows

```
┌─────────────────────────────────────────────────────────────┐
│  SAP S/4HANA (RFC port 33XX)                                │
│  ┌───────────────┐  ┌──────────────────┐  ┌─────────────┐  │
│  │ Standard RFCs  │  │ Table Read FMs   │  │ ZHC_* RFCs  │  │
│  │ TH_SERVER_LIST │  │ /BUI/RFC_READ_   │  │ (Custom     │  │
│  │ TH_WPINFO      │  │ TABLE            │  │  Wrappers)  │  │
│  │ SWNC_COLLECTOR │  │ BBP_RFC_READ_    │  │             │  │
│  │ RSLG_GET_MSGS  │  │ TABLE            │  │             │  │
│  └───────┬───────┘  └────────┬─────────┘  └──────┬──────┘  │
└──────────┼──────────────────┼──────────────────┼──────────┘
           └──────────────────┼──────────────────┘
                              │ node-rfc (Pool)
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  MCP Server (hypercare-sap-mcp-server)                      │
│                                                             │
│  ┌─────────────────────┐  ┌────────────────────────────┐   │
│  │  7 MCP Tools         │  │  KPI Registry (definitions) │   │
│  │  ├ sap_connection_   │  │  ├ 26 implemented           │   │
│  │  │   check           │  │  ├ 21 planned               │   │
│  │  ├ sap_kpi_catalog   │  │  ├ 31 wrapper-backed        │   │
│  │  ├ sap_kpi_read  ◀───┼──┤  └ 7 excluded              │   │
│  │  ├ sap_table_read    │  │                              │   │
│  │  ├ sap_function_call │  └────────────────────────────┘   │
│  │  ├ sap_wrapper_      │                                   │
│  │  │   catalog         │  ┌────────────────────────────┐   │
│  │  └ sap_wrapper_probe │  │  Wrapper Contract Parser    │   │
│  └─────────────────────┘  │  ET_KPIS → KpiResult[]      │   │
│                            └────────────────────────────┘   │
│  POST /mcp (Streamable HTTP, stateless)                     │
│  GET  /healthz                                              │
└─────────────────────┬───────────────────────────────────────┘
                      │ HTTP
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  AgentSpace Codebase                                        │
│  ┌──────────────┐  ┌──────────┐  ┌──────────────────────┐  │
│  │  Scheduler    │  │  Redis   │  │  PostgreSQL          │  │
│  │  1m/5m/30m/   │──│  (cache) │──│  (KPI history)       │  │
│  │  daily tiers  │  │          │  │                      │  │
│  └──────────────┘  └──────────┘  └──────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Dashboard UI (React/Next.js)                         │   │
│  │  Reads from PostgreSQL, NOT from SAP directly         │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. Proposed Changes

### Phase 1 — Fix Issues in Current Code

#### [MODIFY] [definitions.ts](file:///e:/Hypercare/mcp-server/src/kpis/definitions.ts)

1. **Fix `idocsInError`**: Replace 7 parallel `countRows` calls with a single call using `IN` clause
2. **Deduplicate** `unauthorized_login_attempts` vs `failed_login_attempts` — document that they're the same measurement
3. **Add `tier` field** to `BaseKpiDefinition`: `'realtime' | 'frequent' | 'batch' | 'daily'`
4. **Add new KPI definitions** for transport health, work item backlog, spool queue, number ranges (stubs initially)

#### [MODIFY] [types.ts](file:///e:/Hypercare/mcp-server/src/types.ts)

- Add `KpiTier` type to the type system
- Add `tier` to `KpiResult` so the scheduler knows the polling frequency

#### [MODIFY] [nodeRfcClient.ts](file:///e:/Hypercare/mcp-server/src/sap/nodeRfcClient.ts)

- Add circuit breaker logic with configurable failure threshold and backoff duration
- Add basic RFC call timing metrics (duration per call)

#### [MODIFY] [server.ts](file:///e:/Hypercare/mcp-server/src/server.ts)

- Add `/readyz` endpoint for K8s readiness probes

#### [MODIFY] [config.ts](file:///e:/Hypercare/mcp-server/src/config.ts)

- Add `E070`, `E071`, `SWWWIHEAD`, `TSP01`, `NRIV`, `DBSTATTABC` to default allowed tables
- Add circuit breaker config (failure threshold, backoff duration)

---

### Phase 2 — Implement Planned RFC-Based KPIs

Implement the `execute` logic for all 21 planned KPIs. Grouped by RFC dependency:

**Group A: SWNC_COLLECTOR_GET_AGGREGATES** (dialog time, peak users, memory, timeouts, API response time, buffer hit ratio)
- One RFC call, multiple KPI extractions

**Group B: TH_WPINFO / TH_SERVER_LIST** (work process util, app server uptime)
- Live dispatcher queries

**Group C: RSLG_GET_MESSAGES** (system log errors, restart frequency)
- System log filtering

**Group D: BAPI_SYSTEM_MON_GETSYSINFO** (CPU, memory)
- CCMS monitoring

**Group E: ENQUEUE_STATISTICS, ICM/GW monitoring, table reads** (locks, gateway, API, queue)

---

### Phase 3 — Custom Wrapper Handoff

Hand the [sap-wrapper-backlog.md](file:///e:/Hypercare/docs/sap-wrapper-backlog.md) to the ABAP team. Add `ZHC_GET_DB_HEALTH_KPIS` to the backlog for HANA-proxied metrics.

---

## 6. Open Questions

> [!IMPORTANT]
> **Multi-system**: Do you need to monitor multiple SAP systems (DEV/QA/PRD or multi-region PRD) from one MCP server, or is this single-system for now?

> [!IMPORTANT]
> **Transport monitoring**: Which transport route are we monitoring? (e.g., only production imports, or the whole landscape?)

> [!WARNING]
> **SNC authentication**: Is your target SAP system requiring SNC for RFC connections, or is basic user/password acceptable for now?

> [!IMPORTANT]
> **Priority confirmation**: Should I start implementing the Phase 1 fixes and the new KPI definitions (transport health, work items, spool, number ranges), or do you want to adjust the build order first?

---

## Verification Plan

### Automated Tests
- Run `npm run typecheck` after every code change
- Run `npm run test` for existing unit tests
- Add unit tests for new KPI definitions using the existing test patterns in `executor.test.ts`

### Integration Verification
- `sap_connection_check` with `probeTable: "TBTCO"` to verify RFC connectivity
- `sap_kpi_catalog` to confirm all new definitions appear with correct maturity
- `sap_kpi_read` with KPI IDs to verify execute logic on connected system
- `sap_wrapper_probe` to validate any deployed ZHC_* wrappers

### Manual Verification
- Dashboard team confirms KPI output format matches their ingestion expectations
- ABAP team reviews wrapper backlog for feasibility
## 2026-04-05 Validation Override

Use this section as the source of truth. The rest of this file contains useful direction, but several counts and SAP capability assumptions below are stale.

- Actual KPI registry: `85`
- Actual maturity split: `52 implemented`, `0 planned`, `30 custom_abap_required`, `3 excluded`
- `/BUI/RFC_READ_TABLE` is not available in this SAP system
- `BBP_RFC_READ_TABLE` is the working reader
- `BBP_RFC_READ_TABLE` requires compound filters to be sent as one SQL string, not as multiple `OPTIONS` rows
- `RSECACTPROT` and `SNAP` are not table-readable in this SAP system
- `TH_SERVER_LIST`, `TH_WPINFO`, and `SWNC_COLLECTOR_GET_AGGREGATES` exist here
- `SLIC_GET_INSTALLATIONS`, `ENQUEUE_STATISTICS`, `ICM_GET_MONITOR_INFO`, `GW_GET_STATISTIC`, `BAPI_SYSTEM_MON_GETSYSINFO`, and `RSLG_GET_MESSAGES` are missing here
- Newly implemented in the MCP since this plan was written:
  - `work_process_utilization`
  - `sap_application_uptime_pct`
  - `application_server_uptime_per_instance`
  - `average_system_restart_frequency`
  - `batch_window_utilization_pct`
  - `peak_concurrent_users`
  - `dialog_response_time`
  - `update_task_response_time`
  - `timeout_errors`
  - `license_utilization_pct`
  - `cpu_utilization_pct`
  - `memory_utilization_pct`
  - `system_log_errors`
  - `gateway_errors`
  - `lock_table_overflows`
  - `failed_api_calls`
  - `api_response_time`
  - `replication_delays`
  - `number_range_exhaustion_pct`
  - `retry_attempt_count`
  - `queue_lock_failures`
  - `mrp_errors`
  - `goods_receipts`
  - `transport_request_backlog`
  - `work_item_backlog`
  - `spool_queue_errors`
- Node-side direct KPI backlog is now empty. The remaining backlog is SAP-side `ZHC_*` wrapper development plus live validation on the target landscape.
