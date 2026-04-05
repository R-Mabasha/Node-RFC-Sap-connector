# KPI Deep Dive — What It Is, Where It Comes From, Why We Trust It

---

## CATEGORY 1 — System Connectivity & Availability

### Availability

**1. SAP Application Uptime %**
- **What it means:** What percentage of time was the SAP system available and responsive during a given period. During hypercare this is your most watched number — any downtime = business stopped.
- **Data Source:** `SMLG` table + System Start Time delta calculation
- **Why correct:** `SMLG` stores logon group statistics and server registration timestamps. By comparing the system start time against current time and subtracting any recorded downtime windows, you get a true uptime percentage. `TH_SERVER_LIST` was rejected because it only gives you a live snapshot — it cannot tell you what happened 3 hours ago.

---

**2. Database Uptime %**
- **What it means:** How long the HANA database itself was running versus total time. SAP app can be up but DB can be down — they are separate layers.
- **Data Source:** `SYS.M_DATABASE` HANA view — `START_TIME` field
- **Why correct:** This is a HANA system view that SAP itself uses internally. `START_TIME` tells you exactly when the DB instance started. Delta between that and now gives you uptime. No other reliable way to get this.

---

**3. Application Server Uptime Per Instance**
- **What it means:** In SAP you can have multiple app server instances (CI, Dialog, etc.). This tracks uptime per individual instance, not just the whole system.
- **Data Source:** `TH_SERVER_LIST` RFC Function Module
- **Why correct:** This FM returns the list of all registered app server instances with their current status. For per-instance tracking this is correct — the uptime % correction applies only to the system-level KPI, not per-instance status check.

---

**4. Average System Restart Frequency**
- **What it means:** How many times has the system been restarted in a given period. During hypercare, frequent restarts = serious instability signal.
- **Data Source:** `RSLG_GET_MESSAGES` RFC FM filtering SM21 system log for restart events
- **Why correct:** Every SAP system restart writes a specific entry in the system log (SM21). This RFC FM is the programmatic way to read that log. You filter by severity and event type to isolate restart events.

---

### User Activity

**5. Active User Count**
- **What it means:** How many users are currently logged into the SAP system right now across all app servers.
- **Data Source:** `USR41` table
- **Why correct:** `USR41` is the session management table. It records every active terminal session with user ID, instance, and login time. `USR02` is static user master — it does not reflect who is logged in NOW. `TH_USER_LIST` only shows one app server. `USR41` is the only table that aggregates across all instances.

---

**6. Peak Concurrent Users**
- **What it means:** The maximum number of users logged in at the same time within a time window. Used for capacity planning and identifying system stress peaks.
- **Data Source:** `SWNC_COLLECTOR_GET_AGGREGATES` RFC FM
- **Why correct:** This is the FM behind transaction ST03N (Workload Monitor). SAP's own workload collector aggregates peak user counts by time interval. This is the only source that gives you historical peak data, not just a live count.

---

**7. Unauthorized Login Attempts**
- **What it means:** How many times someone tried to log in with wrong credentials or from an unauthorized terminal. Critical security signal during hypercare.
- **Data Source:** `RSECACTPROT` table (Security Audit Log)
- **Why correct:** When Security Audit Log (SM19/SM20) is active — which it must be in any S/4HANA system — every failed login is written here with event code `AU1` and sub-event `F`. This is the gold standard because it is tamper-resistant and captures attempts at the kernel level.

---

**8. License Utilization %**
- **What it means:** What percentage of your licensed SAP users are actively being used. Relevant during hypercare for cost control and compliance.
- **Data Source:** `SLIC_GET_INSTALLATIONS` RFC FM
- **Why correct:** This FM is what SAP's own USMM (User Measurement) transaction uses. It reads the license contract data and active user classification. No other source gives you both the license entitlement and actual usage in one call.

---

## CATEGORY 2 — System Performance

### Workload

**9. Dialog Response Time**
- **What it means:** How long it takes for the system to respond to a user's screen action (pressing Enter, clicking a button). The standard threshold is under 1000ms. Above 2000ms = users complaining.
- **Data Source:** `SWNC_COLLECTOR_GET_AGGREGATES` RFC FM
- **Why correct:** SAP's workload collector captures response time at the work process level — it measures from when the request hits the dispatcher to when the response is sent back. This is the same source as ST03N and is the industry-accepted measurement method.

---

**10. Update Task Response Time**
- **What it means:** SAP splits screen processing (dialog) from database writes (update tasks). This measures how fast the update work processes are completing database commits.
- **Data Source:** `VBHDR` table + `TH_WPINFO` RFC FM
- **Why correct:** `VBHDR` holds update request headers with timestamps. `TH_WPINFO` gives you live work process status. Together they tell you if update tasks are queuing up or completing on time.

---

**11. Background Job Throughput**
- **What it means:** How many background jobs are completing per hour/day. Low throughput = jobs are queuing, running long, or failing.
- **Data Source:** `TBTCO` table
- **Why correct:** `TBTCO` is the definitive job scheduling and status table. Every background job — scheduled, running, completed, failed — is recorded here. No RFC needed for this one. Direct table read with date/status filter gives you exact throughput numbers.

---

**12. Work Process Utilization**
- **What it means:** SAP has a fixed pool of work processes (Dialog, Update, Background, Spool, Enqueue). If all Dialog WPs are busy, new users get queued. This measures how full that pool is.
- **Data Source:** `TH_WPINFO` RFC FM
- **Why correct:** This FM queries the dispatcher directly and returns the real-time status of every work process on every instance. It is a kernel-level call — the most accurate source possible.

---

**13. CPU Utilization %**
- **What it means:** How much of the server's CPU is being consumed by SAP processes. Sustained high CPU = performance degradation incoming.
- **Data Source:** `BAPI_SYSTEM_MON_GETSYSINFO` RFC FM via CCMS
- **Why correct:** CCMS (Computing Center Management System) is SAP's own monitoring framework. This BAPI reads the CCMS monitoring tree which collects OS-level metrics including CPU from each app server. It is how SAP Solution Manager itself reads these values.

---

**14. Memory Utilization %**
- **What it means:** How much RAM is being used by the SAP application layer (ABAP heap, roll area, buffer pools).
- **Data Source:** `SWNC_COLLECTOR_GET_AGGREGATES` RFC FM
- **Why correct:** The workload collector captures memory consumption snapshots over time. For application-layer memory this is the correct source. HANA memory is separate (next KPI).

---

**15. HANA Memory Consumption**
- **What it means:** How much RAM the HANA in-memory database is consuming. HANA loads data into memory — if it runs out, it crashes. This is the most critical infrastructure metric in an S/4HANA system.
- **Data Source:** `SYS.M_SERVICE_MEMORY` HANA view
- **Why correct:** This is a HANA internal system view that shows memory allocation per service (nameserver, indexserver, etc.). It is queried via HANA SQL through ADT or a JDBC/ODBC connection. There is no RFC equivalent for this — you must go directly to HANA layer.

---

### Database Health

**16. DB Response Time**
- **What it means:** How long HANA takes to execute SQL queries on average. High DB response time is usually the root cause of slow dialog response time.
- **Data Source:** `SYS.M_LOAD_HISTORY_SERVICE` HANA view
- **Why correct:** This view records historical load including SQL response times sampled over time. It is HANA's own performance history and is the only source that gives you trending DB response time, not just a live snapshot.

---

**17. Table Growth Rate**
- **What it means:** How fast are the largest SAP tables growing in size. Important during hypercare when live business data starts accumulating on top of migration data.
- **Data Source:** `SYS.M_TABLE_SIZES` HANA view
- **Why correct:** This HANA view gives you the current size of every table in memory and on disk. By polling it over time you calculate the delta — that delta is your growth rate. No ABAP table gives you this HANA-layer sizing data.

---

**18. Column vs Row Store Usage**
- **What it means:** HANA stores tables as either column store (optimized for analytics) or row store (optimized for transactions). Wrong store type for a table = massive performance hit.
- **Data Source:** `SYS.M_TABLES` HANA view
- **Why correct:** This view exposes the `TABLE_TYPE` field which is either COLUMN or ROW for every table. This is metadata about the HANA storage layer — only accessible via HANA system views.

---

**19. Expensive SQL Statements**
- **What it means:** SQL queries that consume excessive CPU or memory — the top suspects when the system slows down. During hypercare these usually come from missing indexes post-migration.
- **Data Source:** `SYS.M_EXPENSIVE_STATEMENTS` HANA view
- **Why correct:** HANA automatically logs statements that exceed a configurable threshold into this view. It captures the full SQL text, execution count, total duration, and CPU time. This is the equivalent of ST05 but at the HANA layer with historical data.

---

**20. Unbalanced Partition Alerts**
- **What it means:** Large HANA tables are partitioned across nodes. If one partition has 80% of the data and others have 20%, queries slow down dramatically. This is a post-migration common issue.
- **Data Source:** `SYS.M_TABLE_PARTITIONS` HANA view
- **Why correct:** This view shows record count and memory size per partition per table. By comparing partition sizes you can algorithmically flag tables where one partition is disproportionately larger than others.

---

### Technical Errors

**21. ABAP Dump Frequency (ST22)**
- **What it means:** Every time an ABAP program crashes hard (runtime error), it creates a dump. During hypercare, new dumps = broken custom code or missing config.
- **Data Source:** `SNAP` table
- **Why correct:** ST22 transaction reads directly from `SNAP`. Every dump is stored here with program name, user, time, and error class. Direct table query with date filter gives you exact dump count and trending.

---

**22. System Log Errors (SM21)**
- **What it means:** The SAP system log records serious system-level events — not business errors but infrastructure errors like work process crashes, memory alerts, and kernel issues.
- **Data Source:** `RSLG_GET_MESSAGES` RFC FM
- **Why correct:** SM21 itself calls this FM. It reads the system log files directly. You filter by severity `E` (Error) and `A` (Abnormal termination) to get only the critical entries.

---

**23. Gateway Errors**
- **What it means:** SAP Gateway handles RFC and BAPI connections from external systems. Gateway errors mean external apps (OData, third-party) cannot talk to SAP.
- **Data Source:** `ICM_GET_MONITOR_INFO` RFC FM
- **Why correct:** The ICM (Internet Communication Manager) and Gateway share monitoring infrastructure. This FM returns connection statistics including error counts. It is how SMGW transaction gets its data.

---

**24. Timeout Errors**
- **What it means:** Requests that took too long and were killed by the system. High timeout count = system overloaded or specific programs hanging.
- **Data Source:** `SWNC_COLLECTOR_GET_AGGREGATES` RFC FM
- **Why correct:** The workload collector tracks timeout events as part of its statistics collection. Same source as response time — it is all captured in the same workload snapshot.

---

**25. Lock Table Overflows**
- **What it means:** SAP has an enqueue server that manages database locks. If the lock table overflows, transactions fail with lock errors. During hypercare this happens when too many users hit the same records.
- **Data Source:** `ENQUEUE_STATISTICS` RFC FM
- **Why correct:** This FM reads directly from the enqueue server statistics — the same source as transaction SM12. It gives you current lock count, max locks, and overflow events.

---

## CATEGORY 3 — Job & Batch Monitoring

**26. Failed Job Count**
- **What it means:** Background jobs that terminated with error. Every failed job means a business process did not complete — payroll didn't run, invoices didn't post, etc.
- **Data Source:** `TBTCO` table, `STATUS = 'A'` (Aborted)
- **Why correct:** `TBTCO` is the single source of truth for all background job execution. Status `A` means aborted. Simple count of this status in the time window gives exact failed job count.

---

**27. Delayed Job Count**
- **What it means:** Jobs that started later than their scheduled time. Delays ripple — if Job A is late, Job B that depends on it waits.
- **Data Source:** `TBTCO` table, compare `SDLSTRTDT/SDLSTRTTM` (scheduled) vs `STRTDATE/STRTTIME` (actual)
- **Why correct:** Both scheduled and actual start times are in `TBTCO`. The delta between them is the delay. If delta exceeds a threshold (e.g., 15 mins), it counts as delayed.

---

**28. Long-Running Job Count**
- **What it means:** Jobs that are taking longer than expected — even if not failed yet. A job that normally runs 10 mins but is now at 2 hours is a problem.
- **Data Source:** `TBTCO` + `TBTCS` tables
- **Why correct:** `TBTCO` has start time of running jobs. `TBTCS` has the step-level detail. For running jobs (STATUS = 'R'), calculate duration from start to now. Compare against historical baseline.

---

**29. Batch Window Utilization %**
- **What it means:** Companies have a defined batch window (e.g., 10pm to 6am). This measures what % of that window is being consumed by jobs. If you hit 100%, jobs spill into business hours.
- **Data Source:** `TBTCO` table
- **Why correct:** All job start and end times are in `TBTCO`. Calculate total job runtime within the defined window versus total window duration. Simple but powerful metric.

---

**30. Job Success Rate**
- **What it means:** Percentage of all jobs that completed successfully. Core health metric — during hypercare you want this above 95%.
- **Data Source:** `TBTCO` table, `STATUS = 'F'` (Finished) divided by total
- **Why correct:** All terminal statuses are in `TBTCO`. F = Finished successfully, A = Aborted, P = Scheduled. Success rate = F / (F + A) × 100.

---

**31. Job Restart Success Rate**
- **What it means:** Of jobs that failed and were manually or automatically restarted, what % succeeded on restart. Low rate = systemic issue, not a one-time glitch.
- **Data Source:** `TBTCO` table, track jobs that went from A → F on retry
- **Why correct:** Each restart creates a new `TBTCO` entry linked to the same job definition. By tracking job name + class across entries you can identify restart attempts and their outcomes.

---

**32. Job Prediction Accuracy (AI)**
- **What it means:** AI-predicted completion time vs actual completion time accuracy. Deferred during hypercare as noted in validation.
- **Data Source:** `TBTCO` historical data as training input
- **Why correct:** Historical job runtimes in `TBTCO` are the training data for any ML model predicting job durations.

---

## CATEGORY 4 — Integration & Interfaces

### IDoc

**33. Total IDocs Processed**
- **What it means:** How many IDocs (Intermediate Documents — SAP's integration messages) were processed in a time period. Volume indicator for integration health.
- **Data Source:** `EDIDC` table
- **Why correct:** `EDIDC` is the IDoc control record table. Every IDoc ever created is here with its status, direction, message type, and timestamps. Count of records by date range gives you total processed.

---

**34. IDocs in Error**
- **What it means:** IDocs that failed to process. Each one is a failed business transaction — a failed goods receipt, failed invoice, failed order from an external system.
- **Data Source:** `EDIDC` table, `STATUS IN (51, 52, 56, 63, 65, 66, 69)`
- **Why correct:** SAP IDoc status codes are standardized. 51 = Application document not posted, 52 = Application document not fully posted, 56 = IDoc with errors added to inbound queue. Adding 66 (wait for predecessor) and 69 (edit) per the validator catches the silent failures.

---

**35. Reprocessing Success Rate**
- **What it means:** Of IDocs that were in error and reprocessed, what % succeeded. Tells you if errors are being resolved or just repeatedly failing.
- **Data Source:** `EDIDC` + `EDIDS` tables
- **Why correct:** `EDIDS` is the IDoc status record — every status change for every IDoc is logged here. Track IDocs that had an error status then moved to status 53 (Application document posted). That transition = successful reprocessing.

---

**36. IDoc Backlog Volume**
- **What it means:** IDocs that are queued and waiting to be processed. Large backlog = integration bottleneck.
- **Data Source:** `EDIDC` table, `STATUS IN (30, 64, 66, 69)`
- **Why correct:** 30 = IDoc ready for dispatch, 64 = IDoc ready to be transferred to application. Plus the validator-added 66 and 69 for S/4HANA-specific waiting states.

---

### API & WebService

**37. Failed API Calls**
- **What it means:** OData or REST API calls from external apps (Fiori, third party) that returned errors. During hypercare, Fiori apps commonly break due to missing config.
- **Data Source:** `ICM_GET_MONITOR_INFO` RFC FM via ICM monitoring
- **Why correct:** All HTTP/HTTPS traffic to SAP goes through the ICM. This FM exposes ICM statistics including error counts by status code. It is the kernel-level HTTP monitoring layer.

---

**38. API Response Time**
- **What it means:** How fast SAP is responding to API calls from external consumers. Critical for Fiori user experience.
- **Data Source:** `SWNC_COLLECTOR_GET_AGGREGATES` RFC FM
- **Why correct:** The workload collector captures HTTP response times alongside dialog response times. Same trusted source, different filter.

---

**39. Retry Attempt Count**
- **What it means:** How many times external systems retried failed RFC or API calls. High retry count = persistent failures, not transient ones.
- **Data Source:** `ARFCSSTATE` table (asynchronous RFC state)
- **Why correct:** Async RFC calls that fail are queued in `ARFCSSTATE` with retry counters. The `COUNTER` field tracks how many times a failed call has been retried.

---

**40. Queue Lock Failures**
- **What it means:** qRFC (queued RFC) calls that could not process because the queue was locked by another process. Indicates contention in integration pipelines.
- **Data Source:** `QRFCSSTATE` table
- **Why correct:** This is the qRFC outbound queue state table. Lock failures are recorded as specific status entries. Direct table query gives you the count.

---

## CATEGORY 5 — Security & Authorization

**41. Authorization Failures (SU53)**
- **What it means:** How many times users got an authorization error — tried to do something they don't have permission for. High count post go-live = roles are incomplete.
- **Data Source:** `RSECACTPROT` table, `EVENT = 'AU5'`
- **Why correct:** Security Audit Log records every authorization check failure with event code AU5. This is the kernel-level capture — it happens before the ABAP application layer so it cannot be bypassed.

---

**42. Users with SoD Conflicts**
- **What it means:** Users who have conflicting roles — e.g., can both create and approve purchase orders. Segregation of Duties violations are an audit finding.
- **Data Source:** GRC tables `GRACSOBJECT` / `GRFNMWRULESTAT`
- **Why correct:** If GRC (Governance Risk Compliance) is in use, these tables store the SoD analysis results. If no GRC, this requires a custom check against role assignments in `AGR_USERS` vs SoD ruleset.

---

**43. Locked Users**
- **What it means:** User accounts that are locked — either by too many failed logins, by admin, or by the system.
- **Data Source:** `USR02` table, `UFLAG IN (32, 64, 128)`
- **Why correct:** `UFLAG` is a bitmask field in the user master. Each value represents a lock reason. 32 = locked by admin, 64 = locked by failed logins, 128 = locked globally. This is the definitive source.

---

**44. Inactive Users**
- **What it means:** User accounts that have not logged in for a defined period (typically 90 days). Security risk — unused accounts should be locked.
- **Data Source:** `USR02` table, `TRDAT < (today minus threshold)`
- **Why correct:** `TRDAT` is the last login date field in `USR02`. Simple date comparison gives you all users who haven't logged in since the threshold. Clean and reliable.

---

**45. Emergency Access Sessions**
- **What it means:** Privileged "firefighter" access sessions where someone used emergency elevated permissions. Every session must be logged and reviewed. High count = control weakness.
- **Data Source:** GRC table `GRACFFLOG`
- **Why correct:** SAP GRC Firefighter logs every emergency access session in `GRACFFLOG` with user, FFID used, timestamps, and transactions executed. This is the only source — there is no SAP standard table outside GRC for this.

---

**46. Failed Login Attempts**
- **What it means:** Simple count of all failed login attempts — wrong password, wrong user, locked account tries.
- **Data Source:** `RSECACTPROT` table, `EVENT = 'AU1'`, `SUBEVENT = 'F'`
- **Why correct:** AU1 is the login event code, subevent F means failed. Security Audit Log is the tamper-resistant, kernel-level source for this.

---

**47. Expired Password %**
- **What it means:** What percentage of active users have passwords that have passed their maximum age. These users will be blocked on next login.
- **Data Source:** `USR02` table, `PWDLGDATE < today`
- **Why correct:** `PWDLGDATE` is the password last change date. Combined with the system password expiry policy you can calculate which users are expired or about to expire.

---

**48. RFC User Password Age**
- **What it means:** System/technical users used for RFC connections between systems. If their password expires, all RFC connections break — taking down integrations silently.
- **Data Source:** `USR02` + `USREFUS` tables, filter `USTYP = 'S'` (system user)
- **Why correct:** `USTYP = 'S'` identifies system/service users. `PWDLGDATE` gives password age. These accounts need special attention because password expiry here means integration failure, not just one user locked out.

---

## CATEGORY 6 — Data Consistency & Master Data

**49. Missing Mandatory Fields**
- **What it means:** Master data records (customers, vendors, materials) where required fields are empty. Migrated data often has gaps that only surface when transactions are processed.
- **Data Source:** `KNA1` (customers), `LFA1` (vendors), `MARA` (materials)
- **Why correct:** These are the core master data tables in SAP. Check for null/blank values in fields that are business-critical (e.g., reconciliation account, payment terms, base unit of measure).

---

**50. Duplicate Entries**
- **What it means:** Same customer, vendor, or material created twice — common during data migration. Causes split transactional history and reporting errors.
- **Data Source:** `BUT000` (Business Partner) + `KNA1/LFA1`
- **Why correct:** In S/4HANA the Business Partner (`BUT000`) is the central object. Duplicates are found by comparing NAME1, address, tax number fields. Cross-referencing with `KNA1`/`LFA1` covers both BP and classic customer/vendor duplicates.

---

**51. CVI/BP Inconsistencies**
- **What it means:** In S/4HANA, Customer/Vendor Integration (CVI) requires every customer and vendor to have a linked Business Partner. Gaps here break financial postings.
- **Data Source:** `CVI_CUST_LINK` + `CVI_VEND_LINK` tables
- **Why correct:** These are the CVI mapping tables that store the link between `KUNNR/LIFNR` and `PARTNER`. Any customer or vendor record not present in these tables is a CVI gap and will cause errors at posting time.

---

**52. Data Migration Reconciliation Errors**
- **What it means:** Discrepancy between what was migrated and what the source system had. Common for open items, balances, and stock quantities.
- **Data Source:** LTRC migration cockpit tables / `SMIGRATION`
- **Why correct:** SAP's migration cockpit logs every object migrated with status. Reconciliation errors are flagged here during the migration phase and should be tracked until resolved post go-live.

---

**53. Stuck Sales Documents**
- **What it means:** Sales orders that are not progressing — blocked, incomplete, or in an intermediate status. Money not flowing.
- **Data Source:** `VBUK` table (SD document status)
- **Why correct:** `VBUK` holds the overall status of every SD document. Fields like `GBSTK` (overall status), `LFSTK` (delivery status), `FKSTK` (billing status) tell you exactly where a document is stuck.

---

**54. Stuck Delivery Documents**
- **What it means:** Outbound deliveries that are not progressing to goods issue or confirmation. Warehouse operations blocked.
- **Data Source:** `LIKP` (delivery header) + `VBUK`
- **Why correct:** `LIKP` stores delivery data, `VBUK` stores its status. `WBSTK` = goods movement status. Not equal to `C` (completed) means the delivery is not moving forward.

---

**55. GR/IR Mismatch**
- **What it means:** Goods Receipts have been posted but no Invoice received, or Invoice received but no Goods Receipt. Classic month-end reconciliation problem.
- **Data Source:** `EKBE` (PO history) + `RBKP` (invoice header)
- **Why correct:** `EKBE` records all movements against a PO line including GRs (movement type `E`) and invoices (`Q`). Comparing quantities and values between these two movement types per PO line gives you the mismatch.

---

**56. Replication Delays**
- **What it means:** In S/4HANA landscape with SLT or other replication tools, delays in data replication to BW or other connected systems.
- **Data Source:** `IUUC_REPL_CONTENT` table
- **Why correct:** SLT (SAP Landscape Transformation) uses this table to track replication status and timestamps. Delta between source change time and replication time gives you delay.

---

## CATEGORY 7 — Business Process KPIs

### OTC (Order to Cash)

**57. Failed Sales Orders** — `VBAK + VBUK` — Credit block flag `CMGST = 'B'` or delivery block `LIFSK ≠ ' '`. These fields directly reflect orders that cannot proceed.

**58. Unposted Billing Documents** — `VBRK` table, `RFBSK ≠ 'C'`. Billing docs not transferred to FI. Revenue not recognized until this is cleared.

**59. Delivery Block Rate** — `VBAK`, `LIFSK ≠ ' '`. Any non-blank delivery block means the order cannot be delivered. Count vs total orders gives the rate.

**60. ATP Check Failures** — `VBEP` (schedule lines), `MTVFP = 'CN'`. ATP (Available to Promise) failure means confirmed quantity is zero — customer order cannot be fulfilled from stock.

---

### P2P (Procure to Pay)

**61. PO Creation Errors** — `EKKO` + workflow table `SWI2_DIAG`. POs stuck in workflow or with error status indicate procurement process blockage.

**62. GR Posting Failures** — `MSEG + MKPF`. Material document not created despite goods movement attempt. Inventory not updated.

**63. Invoice Match Failures** — `RBKP`, `RBSTAT IN ('B', 'S')`. B = blocked for payment, S = parked. 3-way match failed between PO, GR, and Invoice.

**64. Payment Run Errors** — `REGUH + REGUP`. These are the payment run header and item tables. Error flags indicate payments that could not be executed.

---

### Manufacturing

**65. Stuck Production Orders** — `AUFK + AFKO`. System status `CRTD` (created but not released) or `REL` for extended period indicates production orders not progressing.

**66. MRP Errors** — `MDLG` table, `MSGTY = 'E'`. MRP exception messages logged here when planning run encounters errors.

**67. Backflush Failures** — `AFRU` table. Backflushing auto-posts goods issue at confirmation. Failures here mean material consumption not recorded.

---

### Finance

**68. Posting Errors** — `BKPF`, `BSTAT ≠ ' '`. Blank status = posted. Any other value means the document is parked, held, or in error.

**69. Period-End Closing Errors** — `FAGLPERI` + Closing Cockpit. Period status table shows which periods are open/closed and any errors in the closing sequence.

**70. Asset Inconsistencies** — `ANLA + ANLC`. Asset master versus asset value reconciliation. ANLC holds cumulative values — mismatch with ANLA indicates data integrity issues.

**71. Reconciliation Imbalance** — `FAGLFLEXT`. New GL totals table. Debit and credit totals per account and period should always balance. Any imbalance is a critical financial error.

---

### FSM, RTR, TAX, EAM, PTP

These are volume counters — counts of service calls, invoices posted, work orders created etc. Sources are straightforward standard SAP tables:
- FSM: `QMEL` (notifications), `AFVC` (operations)
- RTR: `RBKP` (AP), `VBRK` (AR), `BKPF` (GL)
- TAX: `BSET` (tax items), `BKPF + BSEG` (audit)
- EAM: `AUFK` (PM orders), `QMEL` (notifications), `IFLOT` (functional locations)
- PTP: `EKKO` (POs), `MARA` (materials), `MKPF + MSEG` (GRs)

---

## CATEGORY 8 — ServiceNow (Incident & Support)

**All 10 KPIs come from ServiceNow REST API — not SAP.**

The key point is the validator's correction — for aggregated metrics like MTTR and SLA compliance, use **`/api/now/pa/scorecards`** (Performance Analytics) instead of querying the raw `incident` table. PA pre-calculates these numbers on ServiceNow's side — your dashboard just reads the result instead of computing it, which is dramatically faster and more accurate.

---

**Want me to now move to the architecture and data pipeline design, or do you want to go deeper on any specific KPI first?**
=================================================
This is the **final, locked-in Master KPI List** for your AgentSpace MCP Server. Every HANA-native source has been replaced with a validated **RFC Function Module** or **ABAP Table** to ensure 100% connectivity via `node-rfc` without needing direct database access.

---

## 1. System Connectivity & Availability
| KPI | Validated RFC / Table Source | Key Fields / Logic |
| :--- | :--- | :--- |
| **SAP App Uptime %** | `TH_SERVER_LIST` | Delta: `START_DATE` to System Time. |
| **Database Uptime %** | `TH_SERVER_LIST` | Proxy: Uses DB-dependent start signals from the App Server. |
| **App Server Uptime** | `TH_SERVER_LIST` | `SRVNAME`, `HOST`, `START_DATE`. |
| **System Restarts** | `RSLG_GET_MESSAGES` | Filter for System Start/Shutdown logs in SM21. |
| **Active User Count** | Table `USR41` | Count of active terminal sessions (Live). |
| **Peak Concurrent Users** | `SWNC_COLLECTOR_GET_AGGREGATES` | Field: `MAX_USERS` in workload history. |
| **Unauthorized Logins** | Table `RSECACTPROT` | Security Audit Log: Event `AU1`, Sub-event `F`. |
| **License Utilization %** | `SLIC_GET_INSTALLATIONS` | `USER_COUNT` vs. active license keys. |

---

## 2. System Performance (Workload & DB Health)
| KPI | Validated RFC / Table Source | Key Fields / Logic |
| :--- | :--- | :--- |
| **Dialog Response Time** | `SWNC_COLLECTOR_GET_AGGREGATES` | `AVG_RESP_TIME` (Standard: <1000ms). |
| **Update Task Time** | `TH_WPINFO` | Monitoring of `Update` work process status. |
| **Background Throughput** | Table `TBTCO` | Count of `Finished` status in the last hour. |
| **Work Process Util %** | `TH_WPINFO` | % of Busy vs. Idle Work Processes. |
| **CPU Utilization %** | `BAPI_SYSTEM_MON_GETSYSINFO` | OS-level CPU stats from CCMS. |
| **Memory Utilization %** | `BAPI_SYSTEM_MON_GETSYSINFO` | Physical vs. Virtual Memory consumption. |
| **DB Response Time** | `SWNC_COLLECTOR_GET_AGGREGATES` | Field: `DB_TIME` component of task. |
| **Table Growth Rate** | Table `DBSTATTABC` | History of table statistics (Size in KB/MB). |
| **Column vs Row Store** | Table `DD02L` | Field `TABCLASS` (Cluster/Pool) & S/4 Meta-tags. |
| **Expensive SQL** | `RSDB_SQL_STATEMENT_LOAD` | Top 10 heavy statements (Tier 3 Fetch). |

---

## 3. Technical Errors & Job Monitoring
| KPI | Validated RFC / Table Source | Key Fields / Logic |
| :--- | :--- | :--- |
| **ABAP Dumps (ST22)** | Table `SNAP` | Count by `DATUM` and `PGMNA` (Program). |
| **Sys Log Errors (SM21)** | `RSLG_GET_MESSAGES` | Severity `E` (Error) or `A` (Aborted). |
| **Gateway Errors** | `GW_GET_STATISTIC` | Gateway connection failures and overflows. |
| **Failed Job Count** | Table `TBTCO` | Status = `A` (Aborted/Cancelled). |
| **Delayed Job Count** | Table `TBTCO` | `SDLSTRTDT` vs. `STRTDATE` (Start Delay). |
| **Long-Running Jobs** | Table `TBTCO` | Duration > Baseline for specific Job Name. |
| **Job Success Rate** | Table `TBTCO` | Ratio of `F` (Finished) to `A` (Aborted). |

---

## 4. Integration & Interfaces
| KPI | Validated RFC / Table Source | Key Fields / Logic |
| :--- | :--- | :--- |
| **Total IDocs** | Table `EDIDC` | Total count for current `CREDAT`. |
| **IDocs in Error** | Table `EDIDC` | Statuses: `51, 52, 56, 63, 65`. |
| **IDoc Backlog** | Table `EDIDC` | Statuses: `30, 64, 66` (Ready/Waiting). |
| **Failed API Calls** | `ICM_GET_MONITOR_INFO` | Count of HTTP 4xx/5xx status codes. |
| **API Response Time** | `ICM_GET_MONITOR_INFO` | Average latency per internal Web Service. |
| **Queue Lock Failures** | Table `TRFCQOUT` | Outbound queue entries with status `SYSFAIL`. |

---

## 5. Security & Business Process (S/4HANA Tables)
| Category | KPI | Validated Table Source |
| :--- | :--- | :--- |
| **Security** | **Auth Failures** | Table `RSECACTPROT` (Event `AU5`). |
| **Security** | **Locked Users** | Table `USR02` (UFLAG = `64` or `128`). |
| **OTC** | **Failed Orders** | Tables `VBAK` + `VBUK` (Block Status). |
| **OTC** | **Unposted Billing** | Table `VBRK` (Status != `C`). |
| **P2P** | **PO Errors** | Table `EKKO` (Release Indicator != `Final`). |
| **P2P** | **Invoice Mismatch** | Table `RBKP` (Status `B` - Blocked). |
| **Finance** | **Posting Errors** | Table `BKPF` (Status `E` or Parked). |
| **Finance** | **Reconciliation** | Table `FAGLFLEXT` (Debit vs. Credit balance). |
| **Master Data** | **Duplicate BPs** | Table `BUT000` (Name/Address Match). |
| **Master Data** | **CVI Inconsistency** | Table `CVI_CUST_LINK` (Missing mappings). |

---

## 6. Incident & Support (ServiceNow REST)
*Note: These are called via HTTPS REST, not RFC.*
* **Total Ticket Volume:** `/api/now/pa/scorecards` (Indicator: `Number of open incidents`)
* **Ticket Severity:** `/api/now/table/incident` (Field: `severity`)
* **MTTR (Resolution):** `/api/now/pa/scorecards` (Indicator: `Average resolution time`)
* **SLA Compliance:** `/api/now/pa/scorecards` (Indicator: `% of incidents resolved within SLA`)

---

### Brutal Validator's "AgentSpace" Checklist:
1.  **Authorization:** Ensure your RFC user has `S_RFC` for the FMs listed and `S_TABU_DIS` for the tables.
2.  **Performance:** Always use `ROWCOUNT` (e.g., 500) when calling `RFC_READ_TABLE` for `VBAK` or `BKPF` to prevent memory overflows.
3.  **Tiered Pulls:**
    * **Real-time (1m):** `TH_WPINFO`, `USR41`.
    * **Batch (30m):** `TBTCO`, `EDIDC`, `VBAK`.
    * **Deep (Daily):** `BUT000`, `DD02L`, `DBSTATTABC`.

**Is this list complete for your customer's dashboard, or do you need a specific deep-dive into the FSM/Manufacturing tables?**