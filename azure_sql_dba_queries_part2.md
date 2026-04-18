# Azure SQL DBA — Advanced Queries (Part 2)

> Continuation of the core DBA query reference.  
> Covers In-Memory OLTP, Columnstore, HA/Geo-Replication, Extended Events, SQL Agent (MI), Change Tracking, CDC, Hyperscale, and Proactive Alerting.

---

## 📋 Table of Contents

21. [In-Memory OLTP (Hekaton)](#21-in-memory-oltp-hekaton)
22. [Columnstore Index Analysis](#22-columnstore-index-analysis)
23. [High Availability & Geo-Replication](#23-high-availability--geo-replication)
24. [Extended Events (Azure SQL)](#24-extended-events-azure-sql)
25. [SQL Agent Jobs (Managed Instance)](#25-sql-agent-jobs-managed-instance)
26. [Change Tracking & CDC](#26-change-tracking--cdc)
27. [Plan Guides & Hints](#27-plan-guides--hints)
28. [Resource Governor (Managed Instance)](#28-resource-governor-managed-instance)
29. [Hyperscale-Specific DMVs](#29-hyperscale-specific-dmvs)
30. [Performance Baseline Capture](#30-performance-baseline-capture)
31. [Proactive Health Alerting Queries](#31-proactive-health-alerting-queries)
32. [Azure SQL DBA Interview One-Liners](#32-azure-sql-dba-interview-one-liners)

---

## 21. In-Memory OLTP (Hekaton)

### 21.1 Memory-Optimized Tables
```sql
SELECT
    t.name                         AS table_name,
    t.is_memory_optimized,
    t.durability_desc,             -- SCHEMA_AND_DATA or SCHEMA_ONLY
    tp.memory_allocated_for_table_kb / 1024.0  AS allocated_mb,
    tp.memory_used_by_table_kb / 1024.0        AS used_mb
FROM sys.tables t
JOIN sys.dm_db_xtp_table_memory_stats tp ON t.object_id = tp.object_id
WHERE t.is_memory_optimized = 1
ORDER BY used_mb DESC;
```

### 21.2 In-Memory Index Usage
```sql
SELECT
    OBJECT_NAME(i.object_id)         AS table_name,
    i.name                           AS index_name,
    i.type_desc,
    xii.scans_started,
    xii.rows_returned,
    xii.rows_touched,
    xii.rows_expiring,
    xii.rows_expired,
    xii.rows_expired_removed
FROM sys.indexes i
JOIN sys.dm_db_xtp_index_stats xii
    ON i.object_id = xii.object_id AND i.index_id = xii.index_id
JOIN sys.tables t ON i.object_id = t.object_id
WHERE t.is_memory_optimized = 1
ORDER BY xii.rows_touched DESC;
```

### 21.3 In-Memory OLTP Garbage Collection
```sql
SELECT
    xtp_gc_entry_count,
    xtp_gc_delete_ts_count,
    xtp_gc_mark_expired_count,
    xtp_gc_threads_waiting,
    xtp_gc_reclaimable_versions,
    dust_elapsed_ms
FROM sys.dm_xtp_gc_stats;
```

### 21.4 Memory-Optimized Checkpoint Files
```sql
SELECT
    container_id,
    checkpoint_file_id,
    file_type_desc,
    state_desc,
    lower_bound_tsn,
    upper_bound_tsn,
    file_size_in_bytes / 1048576.0  AS size_mb
FROM sys.dm_db_xtp_checkpoint_files
ORDER BY checkpoint_file_id DESC;
```

### 21.5 XTP (In-Memory) Memory Usage
```sql
SELECT
    type,
    name,
    memory_consumer_desc,
    allocated_bytes / 1048576.0   AS allocated_mb,
    used_bytes / 1048576.0        AS used_mb
FROM sys.dm_xtp_system_memory_consumers
ORDER BY allocated_mb DESC;
```

### 21.6 Natively Compiled Procedure Stats
```sql
SELECT
    OBJECT_NAME(ps.object_id)                         AS proc_name,
    ps.execution_count,
    ps.total_worker_time / ps.execution_count         AS avg_cpu_us,
    ps.total_elapsed_time / ps.execution_count        AS avg_elapsed_us,
    ps.total_logical_reads / ps.execution_count       AS avg_logical_reads,
    ps.last_execution_time
FROM sys.dm_exec_procedure_stats ps
JOIN sys.procedures p ON ps.object_id = p.object_id
WHERE p.is_ms_shipped = 0
ORDER BY avg_cpu_us DESC;
```

---

## 22. Columnstore Index Analysis

### 22.1 Columnstore Segment Quality (Delta Store & Rowgroup Health)
```sql
SELECT
    OBJECT_NAME(i.object_id)        AS table_name,
    i.name                          AS index_name,
    css.partition_number,
    css.row_group_id,
    css.state_desc,
    css.total_rows,
    css.deleted_rows,
    css.size_in_bytes / 1048576.0   AS size_mb,
    -- < 102400 rows = small rowgroup (suboptimal, consider rebuild)
    CASE WHEN css.total_rows < 102400 THEN 'SUBOPTIMAL' ELSE 'OK' END AS quality
FROM sys.dm_db_column_store_row_group_physical_stats css
JOIN sys.indexes i ON css.object_id = i.object_id AND css.index_id = i.index_id
ORDER BY css.total_rows ASC;
```

### 22.2 Delta Store Inspection
```sql
-- Delta rowgroups (open/closed/tombstone = not yet compressed)
SELECT
    OBJECT_NAME(object_id)   AS table_name,
    index_id,
    row_group_id,
    state_desc,              -- OPEN, CLOSED, COMPRESSED, TOMBSTONE
    total_rows,
    deleted_rows,
    size_in_bytes / 1024.0   AS size_kb
FROM sys.dm_db_column_store_row_group_physical_stats
WHERE state_desc <> 'COMPRESSED'
ORDER BY total_rows DESC;
```

### 22.3 Columnstore Usage Stats
```sql
SELECT
    OBJECT_NAME(i.object_id)     AS table_name,
    i.name                       AS index_name,
    ius.user_scans,
    ius.user_seeks,
    ius.user_lookups,
    ius.user_updates,
    ius.last_user_scan
FROM sys.dm_db_index_usage_stats ius
JOIN sys.indexes i ON ius.object_id = i.object_id AND ius.index_id = i.index_id
WHERE i.type IN (5, 6)   -- 5 = Clustered CCI, 6 = NonClustered CCI
  AND ius.database_id = DB_ID()
ORDER BY ius.user_scans DESC;
```

### 22.4 Force Columnstore Compression (Tuple Mover Help)
```sql
-- Close open delta stores manually
ALTER INDEX [CCI_IndexName] ON [schema].[TableName] REORGANIZE
WITH (COMPRESS_ALL_ROW_GROUPS = ON);
```

---

## 23. High Availability & Geo-Replication

### 23.1 Active Geo-Replication Status (Azure SQL DB)
```sql
-- Run on primary or secondary
SELECT
    link_guid,
    partner_server,
    partner_database,
    replication_state_desc,
    partner_role_desc,
    last_replication,
    replication_lag_sec,
    allow_connections_desc,
    is_termination_allowed,
    start_time
FROM sys.dm_geo_replication_link_status;
```

### 23.2 Replication Lag Monitoring
```sql
SELECT
    partner_server,
    partner_database,
    replication_state_desc,
    replication_lag_sec,
    last_replication,
    DATEDIFF(SECOND, last_replication, GETUTCDATE()) AS seconds_since_sync
FROM sys.dm_geo_replication_link_status
WHERE replication_state_desc <> 'CATCH_UP';
```

### 23.3 Always On AG Status (Managed Instance)
```sql
SELECT
    ag.name                        AS ag_name,
    ar.replica_server_name,
    ar.availability_mode_desc,
    ar.failover_mode_desc,
    ar.session_timeout,
    ars.role_desc,
    ars.operational_state_desc,
    ars.connected_state_desc,
    ars.synchronization_health_desc,
    ars.last_connect_error_description,
    drs.synchronization_state_desc,
    drs.synchronization_health_desc AS db_sync_health,
    drs.log_send_queue_size  / 1024.0 AS log_send_queue_mb,
    drs.redo_queue_size / 1024.0      AS redo_queue_mb,
    drs.log_send_rate / 1024.0        AS log_send_rate_mb_s,
    drs.redo_rate / 1024.0            AS redo_rate_mb_s,
    drs.last_hardened_lsn,
    drs.last_redone_time
FROM sys.availability_groups ag
JOIN sys.availability_replicas ar        ON ag.group_id = ar.group_id
JOIN sys.dm_hadr_availability_replica_states ars ON ar.replica_id = ars.replica_id
JOIN sys.dm_hadr_database_replica_states drs     ON ar.replica_id = drs.replica_id
ORDER BY ag.name, ar.replica_server_name;
```

### 23.4 AG Listener Info (Managed Instance)
```sql
SELECT
    agl.dns_name,
    agl.port,
    agl.ip_configuration_string_from_cluster,
    ag.name AS ag_name
FROM sys.availability_group_listeners agl
JOIN sys.availability_groups ag ON agl.group_id = ag.group_id;
```

### 23.5 Failover History
```sql
SELECT
    hdr.group_id,
    ag.name                AS ag_name,
    hdr.replica_id,
    ar.replica_server_name,
    hdr.failure_condition_level,
    hdr.health_check_timeout
FROM sys.dm_hadr_availability_replica_states hdr
JOIN sys.availability_replicas ar ON hdr.replica_id = ar.replica_id
JOIN sys.availability_groups ag   ON hdr.group_id = ag.group_id;
```

---

## 24. Extended Events (Azure SQL)

> Azure SQL supports `sys.dm_xe_database_*` (database-scoped) — NOT server-scoped (`sys.dm_xe_sessions`).

### 24.1 List Active XE Sessions
```sql
SELECT
    name,
    pending_buffers,
    total_regular_buffers,
    total_large_buffers,
    total_buffer_size / 1024.0   AS total_buffer_kb,
    total_bytes_written / 1024.0 AS bytes_written_kb,
    dropped_event_count,
    dropped_buffer_count,
    create_time,
    last_activity
FROM sys.dm_xe_database_sessions
ORDER BY create_time DESC;
```

### 24.2 Create a Custom XE Session (Slow Queries)
```sql
CREATE EVENT SESSION [SlowQueries] ON DATABASE
ADD EVENT sqlserver.sql_statement_completed (
    WHERE duration > 1000000   -- > 1 second (microseconds)
    ACTION (
        sqlserver.sql_text,
        sqlserver.query_plan_xml,
        sqlserver.username,
        sqlserver.database_name,
        sqlserver.client_hostname
    )
)
ADD TARGET package0.ring_buffer (SET max_memory = 51200)  -- 50 MB
WITH (
    MAX_DISPATCH_LATENCY = 5 SECONDS,
    TRACK_CAUSALITY = ON
);

ALTER EVENT SESSION [SlowQueries] ON DATABASE STATE = START;
```

### 24.3 Read XE Ring Buffer Data
```sql
SELECT
    event_data.value('(@timestamp)[1]', 'datetime2')         AS event_time,
    event_data.value('(data[@name="duration"]/value)[1]','bigint') / 1000 AS duration_ms,
    event_data.value('(action[@name="sql_text"]/value)[1]','nvarchar(max)') AS sql_text,
    event_data.value('(action[@name="username"]/value)[1]','nvarchar(256)')  AS login_name,
    event_data.value('(action[@name="client_hostname"]/value)[1]','nvarchar(256)') AS host_name
FROM (
    SELECT CAST(target_data AS XML) AS session_data
    FROM sys.dm_xe_database_session_targets t
    JOIN sys.dm_xe_database_sessions s ON t.event_session_address = s.address
    WHERE s.name = 'SlowQueries'
      AND t.target_name = 'ring_buffer'
) AS data
CROSS APPLY session_data.nodes('//RingBufferTarget/event') AS xevents(event_data)
ORDER BY event_time DESC;
```

### 24.4 Stop and Drop XE Session
```sql
ALTER  EVENT SESSION [SlowQueries] ON DATABASE STATE = STOP;
DROP   EVENT SESSION [SlowQueries] ON DATABASE;
```

### 24.5 Capture Deadlocks via XE
```sql
CREATE EVENT SESSION [DeadlockMonitor] ON DATABASE
ADD EVENT sqlserver.xml_deadlock_report
ADD TARGET package0.ring_buffer (SET max_memory = 10240)
WITH (MAX_DISPATCH_LATENCY = 5 SECONDS);

ALTER EVENT SESSION [DeadlockMonitor] ON DATABASE STATE = START;
```

---

## 25. SQL Agent Jobs (Managed Instance)

> SQL Agent is available on **Azure SQL Managed Instance** only. Fully managed service, no configuration needed.

### 25.1 List All Jobs and Status
```sql
SELECT
    j.job_id,
    j.name                  AS job_name,
    j.enabled,
    j.description,
    j.date_created,
    j.date_modified,
    ja.last_run_date,
    ja.last_run_time,
    CASE ja.last_run_outcome
        WHEN 0 THEN 'Failed'
        WHEN 1 THEN 'Succeeded'
        WHEN 2 THEN 'Retry'
        WHEN 3 THEN 'Cancelled'
        ELSE 'Unknown'
    END                     AS last_run_outcome,
    ja.next_run_date,
    ja.next_run_time
FROM msdb.dbo.sysjobs j
JOIN msdb.dbo.sysjobactivity ja ON j.job_id = ja.job_id
ORDER BY j.name;
```

### 25.2 Job Execution History (Last 7 Days)
```sql
SELECT TOP 100
    j.name                     AS job_name,
    jh.step_id,
    jh.step_name,
    jh.run_date,
    jh.run_time,
    CASE jh.run_status
        WHEN 0 THEN 'Failed'
        WHEN 1 THEN 'Succeeded'
        WHEN 2 THEN 'Retry'
        WHEN 3 THEN 'Cancelled'
    END                        AS status,
    jh.run_duration,           -- HHMMSS format
    jh.message
FROM msdb.dbo.sysjobhistory jh
JOIN msdb.dbo.sysjobs j ON jh.job_id = j.job_id
WHERE jh.run_date >= CONVERT(INT, CONVERT(VARCHAR(8), DATEADD(DAY,-7,GETDATE()),112))
ORDER BY jh.run_date DESC, jh.run_time DESC;
```

### 25.3 Currently Running Jobs
```sql
SELECT
    j.name           AS job_name,
    ja.start_execution_date,
    DATEDIFF(MINUTE, ja.start_execution_date, GETDATE()) AS running_minutes,
    ja.last_executed_step_id,
    js.step_name     AS current_step
FROM msdb.dbo.sysjobactivity ja
JOIN msdb.dbo.sysjobs j ON ja.job_id = j.job_id
LEFT JOIN msdb.dbo.sysjobsteps js
    ON ja.job_id = js.job_id AND ja.last_executed_step_id = js.step_id
WHERE ja.start_execution_date IS NOT NULL
  AND ja.stop_execution_date IS NULL
ORDER BY running_minutes DESC;
```

### 25.4 Failed Jobs in Last 24 Hours
```sql
SELECT
    j.name           AS job_name,
    jh.step_name,
    jh.run_date,
    jh.run_time,
    jh.message
FROM msdb.dbo.sysjobhistory jh
JOIN msdb.dbo.sysjobs j ON jh.job_id = j.job_id
WHERE jh.run_status = 0  -- Failed
  AND jh.run_date >= CONVERT(INT, CONVERT(VARCHAR(8), DATEADD(DAY,-1,GETDATE()),112))
ORDER BY jh.run_date DESC;
```

---

## 26. Change Tracking & CDC

### 26.1 Check Change Tracking Status
```sql
-- Database level
SELECT
    d.name,
    ct.is_auto_cleanup_on,
    ct.retention_period,
    ct.retention_period_units_desc
FROM sys.change_tracking_databases ct
JOIN sys.databases d ON ct.database_id = d.database_id;

-- Table level
SELECT
    OBJECT_NAME(object_id) AS table_name,
    is_track_columns_updated_on,
    min_valid_version,
    begin_version,
    cleanup_version
FROM sys.change_tracking_tables;
```

### 26.2 Query Changed Rows (Change Tracking)
```sql
DECLARE @last_sync_version BIGINT = CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('dbo.Orders'));

SELECT
    ct.OrderID,
    ct.SYS_CHANGE_OPERATION,   -- I=Insert, U=Update, D=Delete
    ct.SYS_CHANGE_VERSION,
    ct.SYS_CHANGE_CREATION_VERSION,
    o.*
FROM CHANGETABLE(CHANGES dbo.Orders, @last_sync_version) AS ct
LEFT JOIN dbo.Orders o ON ct.OrderID = o.OrderID;

-- Get current version for tracking
SELECT CHANGE_TRACKING_CURRENT_VERSION();
```

### 26.3 CDC Status Check
```sql
-- Database CDC enabled?
SELECT is_cdc_enabled, name FROM sys.databases WHERE name = DB_NAME();

-- Tables with CDC enabled
SELECT
    s.name       AS schema_name,
    t.name       AS table_name,
    ct.capture_instance,
    ct.start_lsn,
    ct.index_name,
    ct.captured_column_list,
    ct.filegroup_name,
    ct.create_date,
    ct.index_column_list
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN cdc.change_tables ct ON t.object_id = ct.source_object_id
ORDER BY t.name;
```

### 26.4 Query CDC Changes
```sql
DECLARE @from_lsn BINARY(10) = sys.fn_cdc_get_min_lsn('dbo_Orders');
DECLARE @to_lsn   BINARY(10) = sys.fn_cdc_get_max_lsn();

SELECT *
FROM cdc.fn_cdc_get_all_changes_dbo_Orders(@from_lsn, @to_lsn, 'all with merge')
ORDER BY __$start_lsn;
-- __$operation: 1=Delete, 2=Insert, 3=Before Update, 4=After Update
```

### 26.5 CDC Cleanup & Latency
```sql
-- Check CDC latency
SELECT
    ct.capture_instance,
    MAX(CAST(tran_commit_time AS DATETIME)) AS latest_commit_time,
    DATEDIFF(SECOND, MAX(tran_commit_time), GETUTCDATE()) AS lag_seconds
FROM cdc.change_tables ct
CROSS APPLY cdc.fn_cdc_get_all_changes_dbo_Orders(
    sys.fn_cdc_get_min_lsn(ct.capture_instance),
    sys.fn_cdc_get_max_lsn(), 'all') ch
GROUP BY ct.capture_instance;
```

---

## 27. Plan Guides & Hints

### 27.1 List Existing Plan Guides
```sql
SELECT
    name,
    scope_type_desc,
    scope_object_id,
    OBJECT_NAME(scope_object_id)   AS scope_object,
    is_disabled,
    query_text,
    hints
FROM sys.plan_guides
ORDER BY name;
```

### 27.2 Create a Plan Guide (Force Index Hint)
```sql
EXEC sys.sp_create_plan_guide
    @name = N'PG_ForceIndex_Orders',
    @stmt = N'SELECT * FROM dbo.Orders WHERE CustomerID = @CustomerID',
    @type = N'SQL',
    @module_or_batch = NULL,
    @params = N'@CustomerID INT',
    @hints = N'OPTION (TABLE HINT (dbo.Orders, INDEX(IX_Orders_CustomerID)))';
```

### 27.3 Validate Plan Guides
```sql
SELECT * FROM sys.plan_guides WHERE is_disabled = 0;
EXEC sys.sp_control_plan_guide N'VALIDATE', N'PG_ForceIndex_Orders';
```

### 27.4 Drop a Plan Guide
```sql
EXEC sys.sp_control_plan_guide N'DROP', N'PG_ForceIndex_Orders';
-- Or drop all:
EXEC sys.sp_control_plan_guide N'DROP ALL';
```

### 27.5 USE HINT Query-Level Hints (SQL Server 2016+)
```sql
-- Force legacy CE (Cardinality Estimator)
SELECT * FROM dbo.Orders WHERE CustomerID = 42
OPTION (USE HINT ('FORCE_LEGACY_CARDINALITY_ESTIMATION'));

-- Disable parameter sniffing per-query
SELECT * FROM dbo.Orders WHERE CustomerID = @id
OPTION (OPTIMIZE FOR UNKNOWN);

-- Max DOP override
SELECT * FROM dbo.LargeTable
OPTION (MAXDOP 1);

-- Recompile to avoid plan caching
EXEC usp_GetOrders @CustomerID = 42
    WITH RECOMPILE;
```

---

## 28. Resource Governor (Managed Instance)

### 28.1 Resource Pool Summary
```sql
SELECT
    pool_id,
    name,
    min_cpu_percent,
    max_cpu_percent,
    min_memory_percent,
    max_memory_percent,
    cap_cpu_percent,
    min_iops_per_volume,
    max_iops_per_volume
FROM sys.resource_governor_resource_pools
ORDER BY pool_id;
```

### 28.2 Workload Group Summary
```sql
SELECT
    wg.name                  AS workload_group,
    rp.name                  AS resource_pool,
    wg.importance,
    wg.request_max_memory_grant_percent,
    wg.request_max_cpu_time_sec,
    wg.request_memory_grant_timeout_sec,
    wg.max_dop,
    wg.group_max_requests
FROM sys.resource_governor_workload_groups wg
JOIN sys.resource_governor_resource_pools rp ON wg.pool_id = rp.pool_id;
```

### 28.3 Live Resource Pool Stats
```sql
SELECT
    rp.name,
    rprs.total_cpu_usage_ms,
    rprs.total_cpu_wait_ms,
    rprs.active_worker_count,
    rprs.active_request_count,
    rprs.blocked_task_count,
    rprs.used_memgrant_kb / 1024.0  AS used_memory_grant_mb,
    rprs.total_memgrant_count,
    rprs.total_memgrant_timeout_count  AS memory_grant_timeouts
FROM sys.dm_resource_governor_resource_pools rprs
JOIN sys.resource_governor_resource_pools rp ON rprs.pool_id = rp.pool_id;
```

---

## 29. Hyperscale-Specific DMVs

> Azure SQL **Hyperscale** tier has additional page servers and log service architecture.

### 29.1 Hyperscale Replica Stats
```sql
-- Available on Hyperscale secondary replicas
SELECT
    replica_address,
    secondary_replica_type_desc,
    is_local,
    synchronization_health_desc,
    last_log_block_lsn,
    last_log_block_time,
    last_applied_log_block_lsn,
    last_applied_log_block_time
FROM sys.dm_database_replica_states;
```

### 29.2 Page Server I/O Stats (Hyperscale)
```sql
SELECT
    page_server_reads,
    page_server_reads_in_progress,
    page_server_reads_successful,
    page_server_reads_failed,
    prefetched_pages_from_page_server,
    prefetch_initiated_pages,
    forwarded_valid_page_server_reads
FROM sys.dm_user_db_resource_governance;
```

### 29.3 Hyperscale Resource Governance
```sql
SELECT
    incoming_requests_per_second,
    incoming_dtu_percent,
    incoming_cpu_percent,
    incoming_log_write_percent,
    outgoing_cpu_percent,
    outgoing_dtu_percent
FROM sys.dm_user_db_resource_governance;
```

---

## 30. Performance Baseline Capture

### 30.1 Snapshot Current Wait Stats Into a Table
```sql
-- Create baseline table (run once)
CREATE TABLE dbo.WaitStatsBaseline (
    snapshot_time  DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    wait_type      NVARCHAR(60)  NOT NULL,
    waiting_tasks  BIGINT,
    wait_time_ms   BIGINT,
    signal_wait_ms BIGINT,
    max_wait_ms    BIGINT
);

-- Capture snapshot
INSERT INTO dbo.WaitStatsBaseline (wait_type, waiting_tasks, wait_time_ms, signal_wait_ms, max_wait_ms)
SELECT wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, max_wait_time_ms
FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN (
    'SLEEP_TASK','WAITFOR','DISPATCHER_QUEUE_SEMAPHORE',
    'REQUEST_FOR_DEADLOCK_SEARCH','XE_TIMER_EVENT','LAZYWRITER_SLEEP'
);
```

### 30.2 Delta Between Two Snapshots
```sql
WITH Current AS (
    SELECT wait_type, wait_time_ms, signal_wait_time_ms
    FROM sys.dm_os_wait_stats
),
Baseline AS (
    SELECT wait_type,
           wait_time_ms,
           signal_wait_ms
    FROM dbo.WaitStatsBaseline
    WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM dbo.WaitStatsBaseline)
)
SELECT
    c.wait_type,
    c.wait_time_ms - b.wait_time_ms             AS delta_wait_ms,
    c.signal_wait_time_ms - b.signal_wait_ms    AS delta_signal_ms
FROM Current c
JOIN Baseline b ON c.wait_type = b.wait_type
WHERE c.wait_time_ms - b.wait_time_ms > 0
ORDER BY delta_wait_ms DESC;
```

### 30.3 Capture Query Store Baseline by Time Window
```sql
-- Create a stored snapshot of QS stats
SELECT
    GETUTCDATE()                             AS captured_at,
    q.query_id,
    qt.query_sql_text,
    p.plan_id,
    rs.count_executions,
    rs.avg_cpu_time,
    rs.avg_duration,
    rs.avg_logical_io_reads,
    rs.avg_memory_usage_kb,
    rs.avg_rowcount
INTO dbo.QueryStoreBaseline
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan p        ON q.query_id      = p.query_id
JOIN sys.query_store_runtime_stats rs ON p.plan_id    = rs.plan_id
JOIN sys.query_store_runtime_stats_interval rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
WHERE rsi.start_time >= DATEADD(HOUR, -1, GETUTCDATE());
```

### 30.4 DTU/vCore Utilization Trend (Hourly)
```sql
-- Run from master database on Azure SQL DB
SELECT
    DATEPART(HOUR, start_time)          AS hour_of_day,
    CAST(AVG(avg_cpu_percent) AS DECIMAL(5,2))      AS avg_cpu_pct,
    CAST(MAX(avg_cpu_percent) AS DECIMAL(5,2))      AS peak_cpu_pct,
    CAST(AVG(avg_data_io_percent) AS DECIMAL(5,2))  AS avg_io_pct,
    CAST(MAX(avg_data_io_percent) AS DECIMAL(5,2))  AS peak_io_pct,
    CAST(AVG(avg_log_write_percent) AS DECIMAL(5,2)) AS avg_log_pct,
    COUNT(*)                            AS data_points
FROM sys.resource_stats
WHERE database_name = 'YourDatabase'
  AND start_time >= DATEADD(DAY, -7, GETUTCDATE())
GROUP BY DATEPART(HOUR, start_time)
ORDER BY avg_cpu_pct DESC;
```

---

## 31. Proactive Health Alerting Queries

> Schedule these via **SQL Agent** (MI) or **Azure Automation / Logic Apps** (Azure SQL DB). Alert threshold when result is non-empty.

### 31.1 🔴 Alert: CPU Sustained > 90%
```sql
SELECT 'HIGH_CPU' AS alert_type, AVG(avg_cpu_percent) AS avg_cpu_5min
FROM sys.dm_db_resource_stats
WHERE end_time >= DATEADD(MINUTE, -5, GETUTCDATE())
HAVING AVG(avg_cpu_percent) > 90;
```

### 31.2 🔴 Alert: Storage > 90% Full
```sql
SELECT
    'STORAGE_FULL' AS alert_type,
    name,
    CAST(FILEPROPERTY(name,'SpaceUsed') * 8.0 / size * 100 AS DECIMAL(5,2)) AS used_pct
FROM sys.database_files
WHERE FILEPROPERTY(name,'SpaceUsed') * 8.0 / size * 100 > 90;
```

### 31.3 🔴 Alert: Long-Running Blocking (> 60 seconds)
```sql
SELECT
    'BLOCKING' AS alert_type,
    r.session_id,
    r.blocking_session_id,
    r.wait_time / 1000 AS wait_sec,
    r.wait_type
FROM sys.dm_exec_requests r
WHERE r.blocking_session_id <> 0
  AND r.wait_time > 60000;   -- 60 seconds
```

### 31.4 🔴 Alert: Log Reuse Waiting on Active Transaction
```sql
SELECT
    'LOG_REUSE_WAIT' AS alert_type,
    name,
    log_reuse_wait_desc
FROM sys.databases
WHERE log_reuse_wait_desc NOT IN ('NOTHING', 'CHECKPOINT', 'LOG_BACKUP')
  AND database_id = DB_ID();
```

### 31.5 🔴 Alert: Failed Jobs (MI)
```sql
SELECT
    'FAILED_JOB' AS alert_type,
    j.name AS job_name,
    jh.message,
    jh.run_date,
    jh.run_time
FROM msdb.dbo.sysjobhistory jh
JOIN msdb.dbo.sysjobs j ON jh.job_id = j.job_id
WHERE jh.run_status = 0
  AND jh.run_date = CONVERT(INT, CONVERT(VARCHAR(8), GETDATE(), 112));
```

### 31.6 🔴 Alert: Geo-Replication Lag > 30 seconds
```sql
SELECT
    'GEO_REPLICATION_LAG' AS alert_type,
    partner_server,
    partner_database,
    replication_lag_sec
FROM sys.dm_geo_replication_link_status
WHERE replication_lag_sec > 30;
```

### 31.7 🔴 Alert: Index Fragmentation > 40% on Large Tables
```sql
SELECT
    'HIGH_FRAGMENTATION' AS alert_type,
    OBJECT_NAME(ips.object_id)              AS table_name,
    i.name                                  AS index_name,
    ips.avg_fragmentation_in_percent,
    ips.page_count
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
WHERE ips.avg_fragmentation_in_percent > 40
  AND ips.page_count > 5000;
```

### 31.8 🔴 Alert: Memory Grant Timeouts
```sql
SELECT
    'MEMORY_PRESSURE' AS alert_type,
    pool_id,
    name,
    total_memgrant_timeout_count AS timeouts
FROM sys.dm_resource_governor_resource_pools
WHERE total_memgrant_timeout_count > 0;
```

### 31.9 🔴 Alert: TempDB Version Store > 5 GB
```sql
SELECT
    'TEMPDB_VERSION_STORE' AS alert_type,
    version_store_reserved_page_count * 8 / 1024.0 AS version_store_mb
FROM sys.dm_db_file_space_usage   -- Run in TempDB
HAVING version_store_reserved_page_count * 8 / 1024.0 > 5120;
```

### 31.10 🔴 Alert: Stale Statistics (> 7 days, > 100K modifications)
```sql
SELECT
    'STALE_STATS' AS alert_type,
    OBJECT_NAME(s.object_id)   AS table_name,
    s.name                     AS stat_name,
    sp.last_updated,
    sp.modification_counter
FROM sys.stats s
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
WHERE sp.modification_counter > 100000
  AND sp.last_updated < DATEADD(DAY, -7, GETDATE());
```

---

## 32. Azure SQL DBA Interview One-Liners

> Critical single queries every Senior DBA must know by heart.

```sql
-- Who is blocking whom RIGHT NOW?
SELECT blocking_session_id, session_id, wait_type, wait_time/1000 wait_sec FROM sys.dm_exec_requests WHERE blocking_session_id > 0;

-- What is SQL Server waiting on (top 5)?
SELECT TOP 5 wait_type, wait_time_ms FROM sys.dm_os_wait_stats WHERE wait_type NOT LIKE 'SLEEP%' ORDER BY wait_time_ms DESC;

-- What query is a session currently running?
SELECT session_id, text FROM sys.dm_exec_requests r CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) WHERE session_id = <SPID>;

-- How much log space is used?
DBCC SQLPERF(LOGSPACE);

-- What is the log waiting on?
SELECT name, log_reuse_wait_desc FROM sys.databases WHERE name = DB_NAME();

-- Find long-running transactions
SELECT transaction_id, transaction_begin_time, DATEDIFF(s, transaction_begin_time, GETDATE()) age_sec FROM sys.dm_tran_active_transactions ORDER BY age_sec DESC;

-- Check index fragmentation quickly
SELECT OBJECT_NAME(object_id) tbl, index_id, avg_fragmentation_in_percent FROM sys.dm_db_index_physical_stats(DB_ID(),NULL,NULL,NULL,'LIMITED') WHERE avg_fragmentation_in_percent > 30;

-- Is Query Store enabled?
SELECT actual_state_desc, current_storage_size_mb FROM sys.database_query_store_options;

-- Azure CPU % last 5 min
SELECT AVG(avg_cpu_percent) avg_cpu FROM sys.dm_db_resource_stats WHERE end_time > DATEADD(mi,-5,GETUTCDATE());

-- Find top 5 queries by CPU from Query Store today
SELECT TOP 5 qt.query_sql_text, AVG(rs.avg_cpu_time) avg_cpu FROM sys.query_store_query q JOIN sys.query_store_query_text qt ON q.query_text_id=qt.query_text_id JOIN sys.query_store_plan p ON q.query_id=p.query_id JOIN sys.query_store_runtime_stats rs ON p.plan_id=rs.plan_id JOIN sys.query_store_runtime_stats_interval rsi ON rs.runtime_stats_interval_id=rsi.runtime_stats_interval_id WHERE rsi.start_time >= CAST(CAST(GETUTCDATE() AS DATE) AS DATETIME2) GROUP BY qt.query_sql_text ORDER BY avg_cpu DESC;
```

---

## 🏁 Complete DMV Reference Map

| Category | Key DMV |
|---|---|
| **In-Memory OLTP** | `sys.dm_db_xtp_table_memory_stats`, `sys.dm_xtp_gc_stats` |
| **Columnstore** | `sys.dm_db_column_store_row_group_physical_stats` |
| **Geo-Replication** | `sys.dm_geo_replication_link_status` |
| **Always On (MI)** | `sys.dm_hadr_availability_replica_states`, `sys.dm_hadr_database_replica_states` |
| **Extended Events** | `sys.dm_xe_database_sessions`, `sys.dm_xe_database_session_targets` |
| **SQL Agent (MI)** | `msdb.dbo.sysjobs`, `msdb.dbo.sysjobhistory`, `msdb.dbo.sysjobactivity` |
| **Change Tracking** | `sys.change_tracking_tables`, `CHANGETABLE()` |
| **CDC** | `cdc.change_tables`, `cdc.fn_cdc_get_all_changes_*` |
| **Plan Guides** | `sys.plan_guides`, `sys.sp_create_plan_guide` |
| **Resource Governor** | `sys.dm_resource_governor_resource_pools`, `sys.resource_governor_workload_groups` |
| **Hyperscale** | `sys.dm_user_db_resource_governance`, `sys.dm_database_replica_states` |
| **Elastic Pool** | `sys.dm_elastic_pool_resource_stats`, `sys.elastic_pool_resource_stats` |
| **Automated Tuning** | `sys.dm_db_tuning_recommendations`, `sys.database_automatic_tuning_options` |

---

*Part 2 — Advanced Azure SQL DBA Queries. Use alongside Part 1 (azure_sql_dba_queries.md).*
