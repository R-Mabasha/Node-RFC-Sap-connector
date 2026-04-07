# ABAP Node-RFC MCP - Quick Reference & Cheat Sheet

## 🚀 Quick Start

### 1️⃣ Check Connection
```bash
# Verifies MCP server connectivity
Tool: sap_connection_check
Params: probeTable="T000", probeFields=["MANDT"]
Expected: "reachable": true, "accessMode": "unrestricted"
```

### 2️⃣ Get System Info
```bash
# Retrieves SAP system configuration
Tool: sap_function_call
Params: functionName="RFC_SYSTEM_INFO"
Returns: System ID, Release, Host, DB Type, etc.
```

### 3️⃣ Read KPIs
```bash
# Fetch business metrics
Tool: sap_kpi_read
Params: kpiIds=["active_user_count", "failed_job_count"]
Returns: Real-time and historical metrics
```

---

## 📋 KPI Quick Reference

### Top 10 Most Important KPIs

| # | KPI ID | Title | Tier | Value (Live) | Unit |
|---|--------|-------|------|--------------|------|
| 1 | active_user_count | Active Users | Realtime | 8 | count |
| 2 | work_process_utilization | WP Utilization | Realtime | 13.89% | percent |
| 3 | failed_job_count | Failed Jobs (24h) | Batch | 427 | count |
| 4 | job_success_rate | Job Success Rate | Batch | 97.66% | percent |
| 5 | background_job_throughput | Jobs Completed (24h) | Batch | 17,776 | count |
| 6 | delayed_job_count | Delayed Jobs | Frequent | 0 | count |
| 7 | long_running_job_count | Long-Running Jobs | Frequent | 0 | count |
| 8 | inactive_users | Inactive Users (90d) | Daily | 193 | count |
| 9 | locked_users | Locked Users | Batch | 15 | count |
| 10 | sap_application_uptime_pct | System Uptime | Daily | TBD | percent |

---

## 🔧 Tool Commands Cheat Sheet

### Connection & Diagnostics
```typescript
// Test connectivity
mcp_abap-node-rfc_sap_connection_check({
  probeTable: "T000",
  probeFields: ["MANDT", "DBSYS"]
})

// Test with custom reader
mcp_abap-node-rfc_sap_connection_check({
  probeTable: "USR02",
  probeFields: ["BNAME"],
  readerFunction: "BBP_RFC_READ_TABLE"
})
```

### Table Operations
```typescript
// Simple read
mcp_abap-node-rfc_sap_table_read({
  table: "TBTCO",
  fields: ["JOBNAME", "JOBCOUNT", "STATUS"],
  rowCount: 10
})

// Read with filter
mcp_abap-node-rfc_sap_table_read({
  table: "USR02",
  fields: ["BNAME", "UFLAG"],
  where: ["UFLAG != 0"],
  rowCount: 5
})

// Read with pagination
mcp_abap-node-rfc_sap_table_read({
  table: "TBTCO",
  fields: ["JOBNAME", "STATUS"],
  rowCount: 100,
  rowSkips: 1000
})
```

### Function Calls
```typescript
// System info
mcp_abap-node-rfc_sap_function_call({
  functionName: "RFC_SYSTEM_INFO"
})

// Work process info
mcp_abap-node-rfc_sap_function_call({
  functionName: "TH_WPINFO"
})

// Server list
mcp_abap-node-rfc_sap_function_call({
  functionName: "TH_SERVER_LIST"
})

// With parameters
mcp_abap-node-rfc_sap_function_call({
  functionName: "SWNC_COLLECTOR_GET_AGGREGATES",
  parameters: {
    PERIOD_START: "2026040100",
    PERIOD_TYPE: "DAY"
  }
})
```

### KPI Operations
```typescript
// Single KPI
mcp_abap-node-rfc_sap_kpi_read({
  kpiIds: ["active_user_count"]
})

// Multiple KPIs
mcp_abap-node-rfc_sap_kpi_read({
  kpiIds: [
    "active_user_count",
    "work_process_utilization",
    "failed_job_count",
    "job_success_rate"
  ]
})

// With time window
mcp_abap-node-rfc_sap_kpi_read({
  kpiIds: ["failed_job_count"],
  from: "2026-04-01",
  to: "2026-04-06"
})

// With dimensions
mcp_abap-node-rfc_sap_kpi_read({
  kpiIds: ["number_range_exhaustion_pct"],
  dimensions: {
    nriv_objects: "SD_DOC,MM_DOC"
  }
})
```

### Catalog Operations
```typescript
// All KPIs
mcp_abap-node-rfc_sap_kpi_catalog()

// Implemented only
mcp_abap-node-rfc_sap_kpi_catalog({
  maturity: ["implemented"]
})

// Custom ABAP required
mcp_abap-node-rfc_sap_kpi_catalog({
  maturity: ["custom_abap_required"]
})

// All wrappers
mcp_abap-node-rfc_sap_wrapper_catalog({
  includeKpis: true
})

// Specific wrappers
mcp_abap-node-rfc_sap_wrapper_catalog({
  functionNames: ["ZHC_GET_SECURITY_KPIS", "ZHC_GET_OTC_KPIS"]
})
```

### Wrapper Testing
```typescript
// Test wrapper
mcp_abap-node-rfc_sap_wrapper_probe({
  functionName: "ZHC_GET_SECURITY_KPIS"
})

// Test with expected KPIs
mcp_abap-node-rfc_sap_wrapper_probe({
  functionName: "ZHC_GET_SECURITY_KPIS",
  expectedKpiIds: ["authorization_failures", "users_with_sod_conflicts"]
})

// Test with time window
mcp_abap-node-rfc_sap_wrapper_probe({
  functionName: "ZHC_GET_DATA_QUALITY_KPIS",
  from: "2026-04-01",
  to: "2026-04-06"
})
```

---

## 🎯 Use Case Templates

### Template 1: System Health Dashboard
```json
{
  "name": "Daily System Health",
  "tools": [
    {
      "tool": "sap_kpi_read",
      "kpiIds": [
        "sap_application_uptime_pct",
        "active_user_count",
        "work_process_utilization",
        "job_success_rate",
        "failed_job_count",
        "inactive_users",
        "locked_users"
      ]
    }
  ]
}
```

### Template 2: Job Monitoring
```json
{
  "name": "Job Health Monitor",
  "tools": [
    {
      "tool": "sap_kpi_read",
      "kpiIds": [
        "failed_job_count",
        "delayed_job_count",
        "long_running_job_count",
        "job_success_rate",
        "background_job_throughput",
        "batch_window_utilization_pct"
      ]
    }
  ]
}
```

### Template 3: Integration Monitoring
```json
{
  "name": "Integration Health",
  "tools": [
    {
      "tool": "sap_kpi_read",
      "kpiIds": [
        "total_idocs_processed",
        "idocs_in_error",
        "reprocessing_success_rate",
        "failed_api_calls",
        "api_response_time",
        "queue_lock_failures"
      ]
    }
  ]
}
```

### Template 4: Security Audit
```json
{
  "name": "Security Audit",
  "tools": [
    {
      "tool": "sap_kpi_read",
      "kpiIds": [
        "inactive_users",
        "locked_users",
        "rfc_user_password_age",
        "unauthorized_login_attempts",
        "failed_login_attempts"
      ]
    },
    {
      "tool": "sap_wrapper_probe",
      "functionName": "ZHC_GET_SECURITY_KPIS"
    }
  ]
}
```

### Template 5: Process Monitoring (OTC)
```json
{
  "name": "Order-to-Cash Monitoring",
  "tools": [
    {
      "tool": "sap_kpi_read",
      "kpiIds": [
        "pos_created",
        "delivery_block_rate",
        "unposted_billing_documents"
      ]
    },
    {
      "tool": "sap_wrapper_probe",
      "functionName": "ZHC_GET_OTC_KPIS"
    }
  ]
}
```

---

## 📊 KPI Lookup Table

### System Performance
```
cpu_utilization_pct          → BAPI_SYSTEM_MON_GETSYSINFO
memory_utilization_pct       → BAPI_SYSTEM_MON_GETSYSINFO
dialog_response_time         → SWNC workload aggregates
update_task_response_time    → SWNC workload aggregates
system_log_errors            → RSLG system log
gateway_errors               → GW statistics
timeout_errors               → SWNC workload aggregates
lock_table_overflows         → ENQUEUE_STATISTICS
work_process_utilization     → TH_WPINFO
```

### Job & Batch
```
failed_job_count             → TBTCO table
delayed_job_count            → TBTCO (start time vs schedule)
long_running_job_count       → TBTCO (running > 120 min)
job_success_rate             → TBTCO (finished vs aborted)
background_job_throughput    → TBTCO finished count
batch_window_utilization_pct → TBTCO runtime aggregation
```

### Integration
```
total_idocs_processed        → EDIDC created
idocs_in_error               → EDIDC error statuses
reprocessing_success_rate    → EDIDS transitions
idoc_backlog_volume          → EDIDC backlog statuses
failed_api_calls             → ICM HTTP monitoring
api_response_time            → ICM or SWNC metrics
retry_attempt_count          → ARFCSSTATE
queue_lock_failures          → QRFCSSTATE
```

### Security
```
inactive_users               → USR02 (GLTGB > 90 days)
locked_users                 → USR02 (UFLAG = 32, 64, 128)
rfc_user_password_age        → USR02 (GLTGF)
failed_login_attempts        → RSECACTPROT
unauthorized_login_attempts  → RSECACTPROT (failed audit)
```

### User Activity
```
active_user_count            → USR41 (live sessions)
peak_concurrent_users        → SWNC aggregates
license_utilization_pct      → SLIC_GET_INSTALLATIONS
```

### Data & Master
```
number_range_exhaustion_pct  → NRIV depletion ratio
replication_delays           → IUUC_REPL_CONTENT
missing_mandatory_fields     → Custom wrapper (ZHC_GET_DATA_QUALITY_KPIS)
duplicate_entries            → Custom wrapper (ZHC_GET_DATA_QUALITY_KPIS)
```

---

## 🔍 Debugging Common Issues

### Issue: Table Not Found
```
Error: "TABLE_NOT_AVAILABLE"
Solution: 
1. Check table authorization (S_TABU_DIS)
2. Verify table name spelling
3. Try alternative table-reader function
4. Check BBP_RFC_READ_TABLE vs /BUI/RFC_READ_TABLE compatibility
```

### Issue: Field Not Valid
```
Error: "FIELD_NOT_VALID"
Solution:
1. Verify field name (case-sensitive in some contexts)
2. Use fields specific to the table-reader function
3. Check table-reader function documentation
4. Reduce field list and test individually
```

### Issue: Function Not Found
```
Error: "FU_NOT_FOUND"
Solution:
1. Verify function module name (RFC name)
2. Check if function is available on this S/4HANA release
3. Try RFC_SYSTEM_INFO first to confirm RFC access
4. Check RFC authorization (S_RFC)
```

### Issue: KPI Error Status
```
Status: "error"
Solution:
1. Check underlying table/RFC availability
2. Review notes field for specific error
3. Test with sap_table_read or sap_function_call
4. Verify dimension parameters if provided
```

### Issue: Wrapper Not Available
```
Error: Wrapper function not found
Solution:
1. Implement custom ABAP wrapper function in SAP
2. Or use alternative implemented KPIs
3. Check ZHC_* function naming convention
4. Verify wrapper catalog before calling
```

---

## ✅ Health Check Procedure

```bash
# Step 1: Check connectivity
sap_connection_check(probeTable="T000", probeFields=["MANDT"])
Expected: {"reachable": true, "accessMode": "unrestricted"}

# Step 2: Verify RFC capability
sap_function_call(functionName="RFC_SYSTEM_INFO")
Expected: System info returned, "S4_HANA": "X"

# Step 3: Test realtime KPIs
sap_kpi_read(kpiIds=["active_user_count", "work_process_utilization"])
Expected: All status "ok", numeric values

# Step 4: Verify catalog access
sap_kpi_catalog(maturity=["implemented"])
Expected: 69 implemented KPIs returned

# Step 5: Check wrapper availability
sap_wrapper_catalog(includeKpis=true)
Expected: 10 wrappers, blockers documented

# Result: ✅ All systems GO
```

---

## 📈 Performance Tips

1. **Batch KPI Requests**: Call multiple KPIs in one `sap_kpi_read` instead of individual calls
2. **Use Time Windows**: Narrow `from`/`to` dates for faster table scans
3. **Limit Row Count**: Set `rowCount` appropriately for large tables
4. **Cache Results**: Store KPI snapshots for trending analysis
5. **Implement Alerting**: Create thresholds based on historical baselines
6. **Use Wrappers**: Let custom ABAP optimize complex calculations

---

## 🔐 Security Best Practices

1. ✅ **Use Restricted Credentials**: SAP user with minimal required authorizations
2. ✅ **Monitor Access**: Enable security audit logging
3. ✅ **Rotate Passwords**: RFC user password regularly
4. ✅ **Network Security**: MCP runs on 127.0.0.1 only (localhost)
5. ✅ **Secure Config**: Never commit `.env` with credentials
6. ✅ **Audit Trail**: Log all MCP tool invocations

---

## 📞 Support Resources

| Resource | Purpose |
|----------|---------|
| MCP_TOOLS_GUIDE.md | Complete tool documentation |
| DETAILED_TOOL_REFERENCE.md | In-depth API reference |
| docs/sap-mcp-validation.md | Validation procedures |
| docs/sap-wrapper-backlog.md | Wrapper implementation plans |
| docs/sap-wrapper-contracts.md | Wrapper API contracts |

---

*Last Updated: April 6, 2026*
*SAP System: AK4 (S/4HANA Release 757)*
*MCP Server Status: ✅ Active & Ready*
