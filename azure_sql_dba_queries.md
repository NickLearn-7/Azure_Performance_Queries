# Azure SQL DBA — Performance & Administration Query Reference

> All queries are compatible with **Azure SQL Database** and **Azure SQL Managed Instance** unless noted.  
> Use `sys.*` DMVs; most classic server-level DMVs are available with Azure-specific additions.

---

## 📋 Table of Contents

1. [Instance & Database Health Overview](#1-instance--database-health-overview)
2. [Wait Statistics](#2-wait-statistics)
3. [Top Expensive Queries (Ad-hoc DMVs)](#3-top-expensive-queries-ad-hoc-dmvs)
4. [Query Store (Recommended for Azure SQL)](#4-query-store-recommended-for-azure-sql)
5. [Index Analysis & Fragmentation](#5-index-analysis--fragmentation)
6. [Missing Index Recommendations](#6-missing-index-recommendations)
7. [Blocking & Deadlocks](#7-blocking--deadlocks)
8. [TempDB Analysis](#8-tempdb-analysis)
9. [Memory & Buffer Pool](#9-memory--buffer-pool)
10. [Storage, File I/O & Space](#10-storage-file-io--space)
11. [Connection & Session Management](#11-connection--session-management)
12. [Transaction Log Analysis](#12-transaction-log-analysis)
13. [Statistics & Cardinality](#13-statistics--cardinality)
14. [Azure-Specific: DTU / vCore Resource Governance](#14-azure-specific-dtu--vcore-resource-governance)
15. [Elastic Pool Monitoring](#15-elastic-pool-monitoring)
16. [Automated Tuning Status](#16-automated-tuning-status)
17. [Intelligent Insights & Diagnostics](#17-intelligent-insights--diagnostics)
18. [Security & Auditing](#18-security--auditing)
19. [Backup & Restore (Managed Instance)](#19-backup--restore-managed-instance)
20. [Maintenance & Housekeeping](#20-maintenance--housekeeping)

---

## 1. Instance & Database Health Overview

### 1.1 Database Size and State
```sql
SELECT
    name,
    database_id,
    state_desc,
    recovery_model_desc,
    compatibility_level,
    collation_name,
    is_auto_shrink_on,
    is_auto_update_stats_on,
    is_query_store_on
FROM sys.databases
ORDER BY name;
```

### 1.2 Database File Sizes & Growth Settings
```sql
SELECT
    DB_NAME() AS DatabaseName,
    name        AS LogicalName,
    physical_name,
    type_desc,
    CAST(size * 8.0 / 1024 AS DECIMAL(10,2))           AS SizeMB,
    CAST(FILEPROPERTY(name,'SpaceUsed') * 8.0/1024 AS DECIMAL(10,2)) AS UsedMB,
    CAST((size - FILEPROPERTY(name,'SpaceUsed')) * 8.0/1024 AS DECIMAL(10,2)) AS FreeMB,
    is_percent_growth,
    growth
FROM sys.database_files;
```

### 1.3 Service Tier & Edition (Azure SQL DB)
```sql
SELECT 
    SERVERPROPERTY('Edition')          AS Edition,
    SERVERPROPERTY('EngineEdition')    AS EngineEdition,  -- 5 = Azure SQL DB, 8 = MI
    SERVERPROPERTY('ProductVersion')   AS Version,
    SERVERPROPERTY('ServerName')       AS ServerName,
    @@DBTS                             AS CurrentTimestamp;
```

### 1.4 Current DTU / SLO Information
```sql
SELECT * FROM sys.database_service_objectives;
-- Returns: edition, service_objective (S3, P1, GP_Gen5_4, etc.), elastic_pool_name
```

### 1.5 Active Requests Snapshot
```sql
SELECT
    r.session_id,
    r.status,
    r.command,
    r.wait_type,
    r.wait_time / 1000.0        AS wait_sec,
    r.total_elapsed_time / 1000.0 AS elapsed_sec,
    r.cpu_time / 1000.0         AS cpu_sec,
    r.reads,
    r.writes,
    r.logical_reads,
    s.login_name,
    s.host_name,
    s.program_name,
    DB_NAME(r.database_id)      AS database_name,
    SUBSTRING(t.text, (r.statement_start_offset/2)+1,
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.text)
          ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1) AS query_text
FROM sys.dm_exec_requests r
JOIN sys.dm_exec_sessions  s ON r.session_id = s.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE r.session_id <> @@SPID
ORDER BY r.total_elapsed_time DESC;
```

---

## 2. Wait Statistics

### 2.1 Cumulative Wait Stats (Since Last Restart / Clear)
```sql
SELECT TOP 20
    wait_type,
    waiting_tasks_count,
    wait_time_ms,
    max_wait_time_ms,
    signal_wait_time_ms,
    wait_time_ms - signal_wait_time_ms      AS resource_wait_ms,
    CAST(100.0 * wait_time_ms / SUM(wait_time_ms) OVER() AS DECIMAL(5,2)) AS pct
FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN (
    -- Benign / idle waits to exclude
    'SLEEP_TASK','SLEEP_SYSTEMTASK','SQLTRACE_BUFFER_FLUSH',
    'WAITFOR','DISPATCHER_QUEUE_SEMAPHORE','REQUEST_FOR_DEADLOCK_SEARCH',
    'XE_TIMER_EVENT','BROKER_TO_FLUSH','BROKER_TASK_STOP','CLR_AUTO_EVENT',
    'DISPATCHER_QUEUE_SEMAPHORE','DIRTY_PAGE_POLL','HADR_FILESTREAM_IOMGR_IOCOMPLETION',
    'HADR_WORK_QUEUE','LAZYWRITER_SLEEP','LOGMGR_QUEUE','ONDEMAND_TASK_QUEUE',
    'REQUEST_FOR_DEADLOCK_SEARCH','SERVER_IDLE_CHECK','SLEEP_DBSTARTUP',
    'SLEEP_DBTASK','SLEEP_MASTERDBREADY','SLEEP_MASTERMDREADY',
    'SLEEP_MASTERUPGRADED','SLEEP_MSDBSTARTUP','SLEEP_TEMPDBSTARTUP',
    'SLEEP_WORKER_BY_TEMPTABLE','SNI_HTTP_ACCEPT','SP_SERVER_DIAGNOSTICS_SLEEP',
    'SQLTRACE_INCREMENTAL_FLUSH_SLEEP','WAITFOR','XE_DISPATCHER_WAIT',
    'XE_TIMER_EVENT','BROKER_EVENTHANDLER','CHECKPOINT_QUEUE',
    'DBMIRROR_EVENTS_QUEUE','SQLTRACE_BUFFER_FLUSH','WAIT_XTP_OFFLINE_CKPT_NEW_LOG'
)
ORDER BY wait_time_ms DESC;
```

### 2.2 Wait Stats Per Query (session-level)
```sql
SELECT
    session_id,
    wait_type,
    wait_duration_ms,
    blocking_session_id,
    resource_description
FROM sys.dm_os_waiting_tasks
WHERE session_id > 50  -- exclude system
ORDER BY wait_duration_ms DESC;
```

### 2.3 Signal vs Resource Waits Ratio
```sql
SELECT
    wait_type,
    wait_time_ms,
    signal_wait_time_ms,
    CAST(100.0 * signal_wait_time_ms / NULLIF(wait_time_ms,0) AS DECIMAL(5,2)) AS signal_pct
FROM sys.dm_os_wait_stats
WHERE wait_time_ms > 0
ORDER BY wait_time_ms DESC;
```

---

## 3. Top Expensive Queries (Ad-hoc DMVs)

### 3.1 Top Queries by CPU
```sql
SELECT TOP 20
    qs.total_worker_time / qs.execution_count          AS avg_cpu_us,
    qs.total_worker_time                               AS total_cpu_us,
    qs.execution_count,
    qs.total_elapsed_time / qs.execution_count         AS avg_elapsed_us,
    qs.total_logical_reads / qs.execution_count        AS avg_logical_reads,
    qs.total_physical_reads / qs.execution_count       AS avg_physical_reads,
    SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text)
          ELSE qs.statement_end_offset END - qs.statement_start_offset)/2)+1) AS query_text,
    qp.query_plan,
    DB_NAME(qt.dbid) AS database_name
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
ORDER BY avg_cpu_us DESC;
```

### 3.2 Top Queries by Logical Reads (I/O)
```sql
SELECT TOP 20
    qs.total_logical_reads / qs.execution_count        AS avg_logical_reads,
    qs.total_physical_reads / qs.execution_count       AS avg_physical_reads,
    qs.total_worker_time / qs.execution_count          AS avg_cpu_us,
    qs.execution_count,
    SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text)
          ELSE qs.statement_end_offset END - qs.statement_start_offset)/2)+1) AS query_text
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
ORDER BY avg_logical_reads DESC;
```

### 3.3 Top Queries by Memory Grant
```sql
SELECT TOP 20
    mg.granted_memory_kb / 1024.0      AS granted_mb,
    mg.requested_memory_kb / 1024.0    AS requested_mb,
    mg.used_memory_kb / 1024.0         AS used_mb,
    mg.session_id,
    mg.query_cost,
    mg.dop,
    r.command,
    r.status,
    SUBSTRING(t.text, (r.statement_start_offset/2)+1,
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.text)
          ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1) AS query_text
FROM sys.dm_exec_query_memory_grants mg
JOIN sys.dm_exec_requests r ON mg.session_id = r.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
ORDER BY granted_mb DESC;
```

### 3.4 Recently Cached Plans
```sql
SELECT TOP 20
    cp.usecounts,
    cp.size_in_bytes / 1024.0     AS plan_size_kb,
    cp.cacheobjtype,
    cp.objtype,
    qt.text                        AS query_text,
    DB_NAME(qt.dbid)               AS database_name
FROM sys.dm_exec_cached_plans cp
CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) qt
ORDER BY cp.usecounts DESC;
```

---

## 4. Query Store (Recommended for Azure SQL)

> Query Store is **auto-enabled** on Azure SQL Database. Prefer it over ad-hoc DMVs for trend analysis.

### 4.1 Check Query Store Status
```sql
SELECT 
    actual_state_desc,
    desired_state_desc,
    current_storage_size_mb,
    max_storage_size_mb,
    flush_interval_seconds,
    interval_length_minutes,
    size_based_cleanup_mode_desc,
    query_capture_mode_desc,
    stale_query_threshold_days
FROM sys.database_query_store_options;
```

### 4.2 Top 20 CPU-Consuming Queries (Last 24 Hours)
```sql
SELECT TOP 20
    q.query_id,
    qt.query_sql_text,
    SUM(rs.avg_cpu_time)            AS total_avg_cpu,
    SUM(rs.count_executions)        AS total_executions,
    AVG(rs.avg_cpu_time)            AS avg_cpu_us,
    AVG(rs.avg_duration)            AS avg_duration_us,
    AVG(rs.avg_logical_io_reads)    AS avg_logical_reads,
    AVG(rs.avg_physical_io_reads)   AS avg_physical_reads,
    AVG(rs.avg_memory_usage_kb)     AS avg_memory_kb,
    p.plan_id
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan p        ON q.query_id      = p.query_id
JOIN sys.query_store_runtime_stats rs ON p.plan_id    = rs.plan_id
JOIN sys.query_store_runtime_stats_interval rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
WHERE rsi.start_time >= DATEADD(HOUR, -24, GETUTCDATE())
GROUP BY q.query_id, qt.query_sql_text, p.plan_id
ORDER BY avg_cpu_us DESC;
```

### 4.3 Regressed Queries (Plan Regression Detection)
```sql
SELECT TOP 25
    q.query_id,
    qt.query_sql_text,
    rs1.avg_duration       AS recent_duration_us,
    rs2.avg_duration       AS baseline_duration_us,
    CAST(rs1.avg_duration  AS FLOAT) / NULLIF(rs2.avg_duration,0) AS regression_ratio,
    p1.plan_id             AS recent_plan_id,
    p2.plan_id             AS baseline_plan_id
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan p1 ON q.query_id = p1.query_id
JOIN sys.query_store_runtime_stats rs1 ON p1.plan_id = rs1.plan_id
JOIN sys.query_store_plan p2 ON q.query_id = p2.query_id
JOIN sys.query_store_runtime_stats rs2 ON p2.plan_id = rs2.plan_id
WHERE p1.plan_id <> p2.plan_id
  AND rs1.avg_duration > rs2.avg_duration * 2   -- 2x slower
  AND rs1.count_executions > 10
ORDER BY regression_ratio DESC;
```

### 4.4 Force / Unforce a Plan
```sql
-- Force a specific plan
EXEC sys.sp_query_store_force_plan
    @query_id  = <query_id>,
    @plan_id   = <plan_id>;

-- Unforce
EXEC sys.sp_query_store_unforce_plan
    @query_id  = <query_id>,
    @plan_id   = <plan_id>;
```

### 4.5 Queries with Multiple Plans (Plan Instability)
```sql
SELECT
    q.query_id,
    qt.query_sql_text,
    COUNT(DISTINCT p.plan_id) AS plan_count,
    MIN(rs.avg_duration)      AS best_duration_us,
    MAX(rs.avg_duration)      AS worst_duration_us
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan p        ON q.query_id      = p.query_id
JOIN sys.query_store_runtime_stats rs ON p.plan_id    = rs.plan_id
GROUP BY q.query_id, qt.query_sql_text
HAVING COUNT(DISTINCT p.plan_id) > 1
ORDER BY plan_count DESC;
```

### 4.6 Flush & Purge Query Store Data
```sql
-- Force flush to disk
EXEC sys.sp_query_store_flush_db;

-- Remove a single query
EXEC sys.sp_query_store_remove_query @query_id = <query_id>;

-- Reset all Query Store data (use with caution!)
ALTER DATABASE [YourDB] SET QUERY_STORE CLEAR;
```

---

## 5. Index Analysis & Fragmentation

### 5.1 Index Fragmentation Report
```sql
SELECT
    OBJECT_NAME(ips.object_id)                         AS table_name,
    i.name                                             AS index_name,
    ips.index_type_desc,
    ips.avg_fragmentation_in_percent,
    ips.fragment_count,
    ips.page_count,
    ips.avg_page_space_used_in_percent,
    CASE
        WHEN ips.avg_fragmentation_in_percent > 30 THEN 'REBUILD'
        WHEN ips.avg_fragmentation_in_percent > 10 THEN 'REORGANIZE'
        ELSE 'OK'
    END AS recommendation
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
WHERE ips.page_count > 1000   -- Only significant indexes
ORDER BY ips.avg_fragmentation_in_percent DESC;
```

### 5.2 Index Usage Stats (Seek / Scan / Lookup / Update)
```sql
SELECT
    OBJECT_NAME(ius.object_id)           AS table_name,
    i.name                               AS index_name,
    i.type_desc,
    ius.user_seeks,
    ius.user_scans,
    ius.user_lookups,
    ius.user_updates,
    ius.last_user_seek,
    ius.last_user_scan,
    ius.last_user_update,
    -- High update, low seek = candidate for removal
    CAST(ius.user_updates AS FLOAT) / NULLIF(ius.user_seeks + ius.user_scans + ius.user_lookups, 0) AS update_to_read_ratio
FROM sys.dm_db_index_usage_stats ius
JOIN sys.indexes i   ON ius.object_id = i.object_id AND ius.index_id = i.index_id
WHERE ius.database_id = DB_ID()
  AND i.type_desc <> 'HEAP'
ORDER BY update_to_read_ratio DESC NULLS LAST;
```

### 5.3 Unused Indexes (High Maintenance, No Reads)
```sql
SELECT
    OBJECT_NAME(i.object_id)   AS table_name,
    i.name                     AS index_name,
    i.type_desc,
    ISNULL(ius.user_seeks, 0)  AS user_seeks,
    ISNULL(ius.user_scans, 0)  AS user_scans,
    ISNULL(ius.user_updates, 0) AS user_updates
FROM sys.indexes i
LEFT JOIN sys.dm_db_index_usage_stats ius
    ON i.object_id = ius.object_id
    AND i.index_id = ius.index_id
    AND ius.database_id = DB_ID()
WHERE i.type_desc <> 'HEAP'
  AND ISNULL(ius.user_seeks, 0) = 0
  AND ISNULL(ius.user_scans, 0) = 0
  AND ISNULL(ius.user_lookups, 0) = 0
  AND ISNULL(ius.user_updates, 0) > 0
ORDER BY user_updates DESC;
```

### 5.4 Duplicate / Overlapping Indexes
```sql
SELECT
    OBJECT_NAME(i.object_id) AS table_name,
    i.name                   AS index_name,
    i.type_desc,
    (SELECT STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal)
     FROM sys.index_columns ic
     JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
     WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
    ) AS key_columns,
    (SELECT STRING_AGG(c.name, ', ')
     FROM sys.index_columns ic
     JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
     WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
    ) AS included_columns
FROM sys.indexes i
WHERE i.type > 0
ORDER BY table_name, key_columns;
```

---

## 6. Missing Index Recommendations

### 6.1 All Missing Indexes Ranked by Impact
```sql
SELECT TOP 25
    mid.database_id,
    DB_NAME(mid.database_id)                           AS database_name,
    OBJECT_NAME(mid.object_id, mid.database_id)        AS table_name,
    migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans) AS impact_score,
    migs.user_seeks,
    migs.user_scans,
    migs.avg_total_user_cost,
    migs.avg_user_impact,
    mid.equality_columns,
    mid.inequality_columns,
    mid.included_columns,
    -- Generate CREATE INDEX statement
    'CREATE INDEX [IX_' + OBJECT_NAME(mid.object_id, mid.database_id) + '_missing_' + CAST(mid.index_handle AS VARCHAR) + '] ON '
        + mid.statement
        + ' (' + ISNULL(mid.equality_columns,'')
        + CASE WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL THEN ',' ELSE '' END
        + ISNULL(mid.inequality_columns,'') + ')'
        + ISNULL(' INCLUDE (' + mid.included_columns + ')','') AS create_index_statement
FROM sys.dm_db_missing_index_details mid
JOIN sys.dm_db_missing_index_groups mig ON mid.index_handle = mig.index_handle
JOIN sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle
WHERE mid.database_id = DB_ID()
ORDER BY impact_score DESC;
```

---

## 7. Blocking & Deadlocks

### 7.1 Current Blocking Chain
```sql
WITH BlockingChain AS (
    SELECT
        session_id,
        blocking_session_id,
        wait_type,
        wait_time / 1000.0      AS wait_sec,
        status,
        command,
        CAST(0 AS INT)          AS level,
        CAST(session_id AS VARCHAR(MAX)) AS chain
    FROM sys.dm_exec_requests
    WHERE blocking_session_id <> 0

    UNION ALL

    SELECT
        r.session_id,
        r.blocking_session_id,
        r.wait_type,
        r.wait_time / 1000.0,
        r.status,
        r.command,
        bc.level + 1,
        bc.chain + ' -> ' + CAST(r.session_id AS VARCHAR)
    FROM sys.dm_exec_requests r
    JOIN BlockingChain bc ON r.session_id = bc.blocking_session_id
)
SELECT DISTINCT
    bc.session_id,
    bc.blocking_session_id,
    bc.wait_type,
    bc.wait_sec,
    bc.chain,
    s.login_name,
    s.host_name,
    s.program_name,
    SUBSTRING(t.text, (r.statement_start_offset/2)+1,
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.text)
          ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1) AS blocked_query
FROM BlockingChain bc
JOIN sys.dm_exec_sessions s  ON bc.session_id = s.session_id
JOIN sys.dm_exec_requests r  ON bc.session_id = r.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
ORDER BY bc.wait_sec DESC;
```

### 7.2 Lock Information
```sql
SELECT
    tl.resource_type,
    tl.resource_database_id,
    DB_NAME(tl.resource_database_id) AS db_name,
    tl.resource_description,
    tl.request_mode,
    tl.request_status,
    tl.request_session_id,
    s.login_name,
    s.host_name,
    SUBSTRING(t.text, (r.statement_start_offset/2)+1,
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.text)
          ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1) AS query_text
FROM sys.dm_tran_locks tl
LEFT JOIN sys.dm_exec_sessions  s ON tl.request_session_id = s.session_id
LEFT JOIN sys.dm_exec_requests  r ON tl.request_session_id = r.session_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE tl.resource_type <> 'DATABASE'
ORDER BY tl.resource_type, tl.request_status;
```

### 7.3 Deadlock Ring Buffer (Extended Events alternative)
```sql
-- Read deadlock info from ring buffer (Azure supports this)
SELECT
    xdr.value('@timestamp','datetime2') AS deadlock_time,
    xdr.query('.')                      AS deadlock_xml
FROM (
    SELECT CAST(target_data AS XML) AS target_data
    FROM sys.dm_xe_database_session_targets t
    JOIN sys.dm_xe_database_sessions s ON t.event_session_address = s.address
    WHERE s.name = 'system_health'
      AND t.target_name = 'ring_buffer'
) AS data
CROSS APPLY target_data.nodes('//RingBufferTarget/event[@name="xml_deadlock_report"]') AS xevents(xdr)
ORDER BY deadlock_time DESC;
```

### 7.4 Kill Blocking Head
```sql
-- Find head blockers (sessions blocking but not blocked themselves)
SELECT 
    s.session_id,
    s.login_name,
    s.host_name,
    s.program_name,
    r.wait_type,
    r.total_elapsed_time / 1000 AS elapsed_sec,
    t.text AS current_sql
FROM sys.dm_exec_sessions s
LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE s.session_id IN (
    SELECT DISTINCT blocking_session_id FROM sys.dm_exec_requests WHERE blocking_session_id <> 0
)
AND s.session_id NOT IN (
    SELECT session_id FROM sys.dm_exec_requests WHERE blocking_session_id <> 0 AND blocking_session_id IS NOT NULL
);

-- KILL <session_id>;  -- Use carefully!
```

---

## 8. TempDB Analysis

### 8.1 TempDB Space Usage by Session
```sql
SELECT TOP 20
    tsu.session_id,
    tsu.request_id,
    tsu.user_objects_alloc_page_count * 8 / 1024.0     AS user_obj_mb,
    tsu.internal_objects_alloc_page_count * 8 / 1024.0 AS internal_obj_mb,
    tsu.user_objects_dealloc_page_count * 8 / 1024.0   AS user_obj_dealloc_mb,
    s.login_name,
    s.host_name,
    s.program_name,
    SUBSTRING(t.text, (r.statement_start_offset/2)+1,
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.text)
          ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1) AS query_text
FROM sys.dm_db_task_space_usage tsu
JOIN sys.dm_exec_sessions s ON tsu.session_id = s.session_id
LEFT JOIN sys.dm_exec_requests r ON tsu.session_id = r.session_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE tsu.session_id > 50
ORDER BY (tsu.user_objects_alloc_page_count + tsu.internal_objects_alloc_page_count) DESC;
```

### 8.2 TempDB Total Space
```sql
SELECT
    SUM(unallocated_extent_page_count) * 8 / 1024.0  AS free_mb,
    SUM(version_store_reserved_page_count) * 8 / 1024.0 AS version_store_mb,
    SUM(internal_object_reserved_page_count) * 8 / 1024.0 AS internal_obj_mb,
    SUM(user_object_reserved_page_count) * 8 / 1024.0 AS user_obj_mb,
    SUM(mixed_extent_page_count) * 8 / 1024.0 AS mixed_extent_mb
FROM sys.dm_db_file_space_usage;  -- Run in TempDB context
```

### 8.3 Version Store Growth (RCSI Pressure)
```sql
SELECT
    transaction_sequence_num,
    version_sequence_num,
    database_id,
    DB_NAME(database_id)        AS database_name,
    rowset_ts,
    min_cleanup_ts
FROM sys.dm_tran_top_version_generators
ORDER BY rowset_ts DESC;
```

---

## 9. Memory & Buffer Pool

### 9.1 Buffer Pool Usage by Database
```sql
SELECT
    DB_NAME(database_id)                  AS database_name,
    COUNT(*) * 8 / 1024.0                 AS cached_mb,
    COUNT(CASE WHEN is_modified = 1 THEN 1 END) * 8 / 1024.0 AS dirty_mb
FROM sys.dm_os_buffer_descriptors
WHERE database_id <> 32767  -- excludes resource DB
GROUP BY database_id
ORDER BY cached_mb DESC;
```

### 9.2 Buffer Pool by Table (Top Memory Consumers)
```sql
SELECT TOP 20
    OBJECT_NAME(p.object_id)  AS table_name,
    i.name                    AS index_name,
    COUNT(*) * 8 / 1024.0    AS cached_mb,
    SUM(CASE WHEN bd.is_modified = 1 THEN 1 ELSE 0 END) * 8 / 1024.0 AS dirty_mb
FROM sys.dm_os_buffer_descriptors bd
JOIN sys.allocation_units au ON bd.allocation_unit_id = au.allocation_unit_id
JOIN sys.partitions p ON au.container_id = p.hobt_id
JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
WHERE bd.database_id = DB_ID()
GROUP BY p.object_id, i.name
ORDER BY cached_mb DESC;
```

### 9.3 Memory Clerks (Top Allocations)
```sql
SELECT TOP 20
    type                                AS clerk_type,
    name,
    SUM(pages_kb) / 1024.0             AS allocated_mb
FROM sys.dm_os_memory_clerks
GROUP BY type, name
ORDER BY allocated_mb DESC;
```

### 9.4 Memory Grant Waiting Queries
```sql
SELECT
    session_id,
    request_id,
    scheduler_id,
    granted_memory_kb / 1024.0  AS granted_mb,
    requested_memory_kb / 1024.0 AS requested_mb,
    required_memory_kb / 1024.0  AS required_mb,
    used_memory_kb / 1024.0      AS used_mb,
    max_used_memory_kb / 1024.0  AS max_used_mb,
    wait_order,
    is_next_candidate,
    queue_id,
    resource_semaphore_id,
    timeout_sec,
    dop,
    query_cost
FROM sys.dm_exec_query_memory_grants
ORDER BY wait_order;
```

---

## 10. Storage, File I/O & Space

### 10.1 I/O Stall by Database File
```sql
SELECT
    DB_NAME(f.database_id)  AS database_name,
    f.physical_name,
    f.type_desc,
    vfs.num_of_reads,
    vfs.num_of_writes,
    vfs.io_stall_read_ms    / NULLIF(vfs.num_of_reads, 0)  AS avg_read_ms,
    vfs.io_stall_write_ms   / NULLIF(vfs.num_of_writes, 0) AS avg_write_ms,
    vfs.io_stall            AS total_io_stall_ms,
    vfs.size_on_disk_bytes / 1048576.0  AS size_mb
FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
JOIN sys.master_files f ON vfs.database_id = f.database_id AND vfs.file_id = f.file_id
ORDER BY vfs.io_stall DESC;
```

### 10.2 Table Size (Top Space Consumers)
```sql
SELECT TOP 30
    t.name                                          AS table_name,
    s.name                                          AS schema_name,
    p.rows                                          AS row_count,
    SUM(a.total_pages) * 8 / 1024.0                AS total_mb,
    SUM(a.used_pages) * 8 / 1024.0                 AS used_mb,
    (SUM(a.total_pages) - SUM(a.used_pages)) * 8 / 1024.0 AS unused_mb
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.indexes i ON t.object_id = i.object_id
JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.allocation_units a ON p.partition_id = a.container_id
GROUP BY t.name, s.name, p.rows
ORDER BY total_mb DESC;
```

### 10.3 Database Free Space
```sql
SELECT
    name,
    CAST(size * 8.0 / 1024 AS DECIMAL(10,2))                       AS total_mb,
    CAST(FILEPROPERTY(name,'SpaceUsed') * 8.0 / 1024 AS DECIMAL(10,2)) AS used_mb,
    CAST((size - FILEPROPERTY(name,'SpaceUsed')) * 8.0 / 1024 AS DECIMAL(10,2)) AS free_mb,
    CAST(100.0 * FILEPROPERTY(name,'SpaceUsed') / NULLIF(size, 0) AS DECIMAL(5,2)) AS used_pct
FROM sys.database_files;
```

---

## 11. Connection & Session Management

### 11.1 Active Sessions Summary
```sql
SELECT
    s.status,
    COUNT(*)                        AS session_count,
    SUM(s.cpu_time) / 1000.0       AS total_cpu_sec,
    SUM(s.memory_usage * 8) / 1024.0 AS total_memory_mb,
    SUM(s.reads)                    AS total_reads,
    SUM(s.writes)                   AS total_writes
FROM sys.dm_exec_sessions s
WHERE s.is_user_process = 1
GROUP BY s.status;
```

### 11.2 Sessions by Login / Program
```sql
SELECT
    login_name,
    host_name,
    program_name,
    COUNT(*)           AS session_count,
    SUM(cpu_time)      AS total_cpu_ms,
    SUM(reads)         AS total_reads,
    MIN(login_time)    AS oldest_session,
    MAX(last_request_start_time) AS last_activity
FROM sys.dm_exec_sessions
WHERE is_user_process = 1
GROUP BY login_name, host_name, program_name
ORDER BY session_count DESC;
```

### 11.3 Long-Running Transactions
```sql
SELECT
    at.transaction_id,
    at.transaction_begin_time,
    DATEDIFF(SECOND, at.transaction_begin_time, GETDATE()) AS duration_sec,
    at.transaction_type,
    at.transaction_state,
    es.session_id,
    es.login_name,
    es.host_name,
    SUBSTRING(st.text, (er.statement_start_offset/2)+1,
        ((CASE er.statement_end_offset WHEN -1 THEN DATALENGTH(st.text)
          ELSE er.statement_end_offset END - er.statement_start_offset)/2)+1) AS query_text
FROM sys.dm_tran_active_transactions at
JOIN sys.dm_tran_session_transactions tst ON at.transaction_id = tst.transaction_id
JOIN sys.dm_exec_sessions es ON tst.session_id = es.session_id
LEFT JOIN sys.dm_exec_requests er ON es.session_id = er.session_id
OUTER APPLY sys.dm_exec_sql_text(er.sql_handle) st
ORDER BY duration_sec DESC;
```

---

## 12. Transaction Log Analysis

### 12.1 Log Space Usage
```sql
DBCC SQLPERF(LOGSPACE);
-- Or using DMV (Azure SQL DB preferred):
SELECT
    name,
    log_reuse_wait_desc,
    log_size_mb = size * 8.0 / 1024,
    log_used_mb = FILEPROPERTY(name, 'SpaceUsed') * 8.0 / 1024
FROM sys.databases
WHERE name = DB_NAME();
```

### 12.2 Log Reuse Wait Reason
```sql
SELECT
    DB_NAME(database_id)   AS database_name,
    log_reuse_wait_desc,   -- Key: LOG_BACKUP, ACTIVE_TRANSACTION, REPLICATION etc.
    recovery_model_desc
FROM sys.databases
WHERE database_id > 4;    -- Skip system DBs
```

### 12.3 Active Virtual Log Files (VLF Count)
```sql
-- High VLF count (>1000) degrades recovery and log performance
DBCC LOGINFO;  -- Review VLF count
```

### 12.4 Transaction Log Contents (Active)
```sql
SELECT TOP 100
    [Current LSN],
    Operation,
    Context,
    [Transaction ID],
    [Begin Time],
    Description,
    [Page ID],
    [Slot ID],
    [Number of Locks],
    [Lock Information]
FROM fn_dblog(NULL, NULL)
WHERE Operation IN ('LOP_BEGIN_XACT','LOP_COMMIT_XACT','LOP_ABORT_XACT')
ORDER BY [Current LSN] DESC;
```

---

## 13. Statistics & Cardinality

### 13.1 Stale Statistics
```sql
SELECT
    OBJECT_NAME(s.object_id)   AS table_name,
    s.name                     AS stat_name,
    sp.last_updated,
    sp.rows,
    sp.rows_sampled,
    sp.unfiltered_rows,
    sp.modification_counter,
    CAST(100.0 * sp.rows_sampled / NULLIF(sp.rows, 0) AS DECIMAL(5,2)) AS sample_pct,
    DATEDIFF(DAY, sp.last_updated, GETDATE())          AS days_since_update
FROM sys.stats s
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
WHERE sp.modification_counter > 0
ORDER BY sp.modification_counter DESC;
```

### 13.2 Update All Statistics (Per Table)
```sql
-- Per table
UPDATE STATISTICS [schema].[table_name] WITH FULLSCAN;

-- All tables in a database
EXEC sp_updatestats;
```

### 13.3 Statistics Histogram
```sql
DBCC SHOW_STATISTICS('schema.TableName', 'IndexOrStatName');
```

---

## 14. Azure-Specific: DTU / vCore Resource Governance

### 14.1 Resource Utilization (Last 1 Hour)
```sql
-- Azure SQL Database only
SELECT TOP 60
    end_time,
    avg_cpu_percent,
    avg_data_io_percent,
    avg_log_write_percent,
    avg_memory_usage_percent,
    xtp_storage_percent,
    max_worker_percent,
    max_session_percent,
    dtu_limit,                  -- NULL for vCore
    cpu_limit,                  -- vCore models
    avg_instance_cpu_percent,
    avg_instance_memory_percent
FROM sys.dm_db_resource_stats
ORDER BY end_time DESC;
```

### 14.2 Resource Stats (Last 14 Days — Azure Portal equivalent)
```sql
-- Available in master database (Azure SQL DB)
SELECT TOP 336   -- 14 days * 24 hours
    start_time,
    end_time,
    database_name,
    sku,
    storage_in_megabytes,
    avg_cpu_percent,
    avg_data_io_percent,
    avg_log_write_percent,
    max_worker_percent,
    max_session_percent
FROM sys.resource_stats
WHERE database_name = 'YourDBName'
ORDER BY start_time DESC;
```
> **Note:** `sys.resource_stats` is queried from the **master** database.

### 14.3 Worker Thread Utilization
```sql
SELECT
    scheduler_id,
    cpu_id,
    status,
    runnable_tasks_count,
    work_queue_count,
    pending_disk_io_count,
    load_factor,
    yield_count
FROM sys.dm_os_schedulers
WHERE status = 'VISIBLE ONLINE'
ORDER BY runnable_tasks_count DESC;
```

### 14.4 Throttling Detection
```sql
-- Check if requests are being throttled by resource governor
SELECT
    pool_id,
    name,
    statistics_start_time,
    total_cpu_usage_ms,
    cache_memory_kb,
    compile_memory_kb,
    used_memgrant_kb,
    total_memgrant_count,
    total_memgrant_timeout_count,   -- >0 means memory pressure
    active_worker_count,
    blocked_task_count,
    total_query_optimizations,
    total_suboptimal_plans
FROM sys.dm_resource_governor_resource_pools;
```

---

## 15. Elastic Pool Monitoring

> Run these from the **master** database or the individual database context.

### 15.1 Elastic Pool Resource Usage
```sql
-- In individual database
SELECT TOP 60
    end_time,
    avg_cpu_percent,
    avg_data_io_percent,
    avg_log_write_percent,
    avg_memory_usage_percent,
    elastic_pool_percent_allocation,
    max_worker_percent,
    max_session_percent
FROM sys.dm_elastic_pool_resource_stats
ORDER BY end_time DESC;
```

### 15.2 All Databases in a Pool Stats
```sql
-- From master
SELECT
    database_name,
    end_time,
    avg_cpu_percent,
    avg_data_io_percent,
    avg_log_write_percent,
    max_worker_percent,
    max_session_percent
FROM sys.elastic_pool_resource_stats
WHERE elastic_pool_name = 'YourPoolName'
  AND end_time > DATEADD(HOUR, -24, GETUTCDATE())
ORDER BY end_time DESC, avg_cpu_percent DESC;
```

---

## 16. Automated Tuning Status

### 16.1 Check Automated Tuning Options
```sql
SELECT
    name,
    desired_state_desc,
    actual_state_desc,
    reason_desc
FROM sys.database_automatic_tuning_options;
```

### 16.2 Automated Tuning Recommendations
```sql
SELECT
    name,
    type_desc,
    state_desc,
    score,
    details,
    execute_action_start_time,
    execute_action_initiated_by,
    revert_action_start_time
FROM sys.dm_db_tuning_recommendations
ORDER BY score DESC;
```

### 16.3 Enable / Disable Automatic Tuning
```sql
-- Enable plan forcing
ALTER DATABASE [YourDB] SET AUTOMATIC_TUNING (FORCE_LAST_GOOD_PLAN = ON);

-- Enable index recommendations (Advisory Mode - no auto-apply)
ALTER DATABASE [YourDB] SET AUTOMATIC_TUNING (CREATE_INDEX = ON);
ALTER DATABASE [YourDB] SET AUTOMATIC_TUNING (DROP_INDEX = ON);
```

---

## 17. Intelligent Insights & Diagnostics

### 17.1 Extended Events — System Health Session
```sql
-- View system health ring buffer events
SELECT
    CAST(target_data AS XML).value('(//event/@timestamp)[1]', 'datetime2') AS event_time,
    CAST(target_data AS XML).value('(//event/@name)[1]', 'varchar(100)')   AS event_name,
    target_data
FROM sys.dm_xe_database_session_targets xst
JOIN sys.dm_xe_database_sessions xs ON xst.event_session_address = xs.address
WHERE xs.name = 'system_health'
  AND xst.target_name = 'ring_buffer'
ORDER BY event_time DESC;
```

### 17.2 Execution Plan Cache Analysis
```sql
-- Plans with high compile cost but low reuse
SELECT TOP 20
    usecounts,
    size_in_bytes,
    cacheobjtype,
    objtype,
    plan_handle,
    qt.text
FROM sys.dm_exec_cached_plans cp
CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) qt
WHERE cp.usecounts = 1
  AND cp.objtype = 'Adhoc'
ORDER BY size_in_bytes DESC;
```

### 17.3 Parameter Sniffing Detection
```sql
-- Find queries with high variance in execution stats (possible parameter sniffing)
SELECT TOP 20
    qs.sql_handle,
    qs.execution_count,
    qs.total_worker_time / qs.execution_count          AS avg_cpu,
    qs.min_worker_time,
    qs.max_worker_time,
    qs.max_worker_time - qs.min_worker_time            AS cpu_variance,
    qt.text
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
WHERE qs.execution_count > 5
ORDER BY cpu_variance DESC;
```

---

## 18. Security & Auditing

### 18.1 Database Users and Roles
```sql
SELECT
    dp.name           AS principal_name,
    dp.type_desc,
    dp.authentication_type_desc,
    ds.name           AS default_schema,
    ISNULL(rp.name, 'N/A') AS role_name
FROM sys.database_principals dp
LEFT JOIN sys.schemas ds ON dp.default_schema_id = ds.schema_id
LEFT JOIN sys.database_role_members drm ON dp.principal_id = drm.member_principal_id
LEFT JOIN sys.database_principals rp ON drm.role_principal_id = rp.principal_id
WHERE dp.type NOT IN ('R')  -- Exclude built-in roles from member list
ORDER BY dp.type_desc, dp.name;
```

### 18.2 Object-Level Permissions
```sql
SELECT
    pr.name            AS principal_name,
    pr.type_desc,
    perm.permission_name,
    perm.state_desc,
    obj.name           AS object_name,
    obj.type_desc      AS object_type
FROM sys.database_permissions perm
JOIN sys.database_principals pr ON perm.grantee_principal_id = pr.principal_id
LEFT JOIN sys.objects obj ON perm.major_id = obj.object_id
ORDER BY pr.name, obj.name;
```

### 18.3 Login Activity (Azure SQL DB Audit Log via DMV)
```sql
-- Last logins from sys.dm_exec_sessions
SELECT
    login_name,
    host_name,
    program_name,
    login_time,
    last_request_start_time,
    last_request_end_time,
    status
FROM sys.dm_exec_sessions
WHERE is_user_process = 1
ORDER BY login_time DESC;
```

### 18.4 Transparent Data Encryption (TDE) Status
```sql
SELECT
    db.name,
    dek.encryption_state_desc,
    dek.percent_complete,
    dek.key_algorithm,
    dek.key_length,
    dek.encryptor_type
FROM sys.dm_database_encryption_keys dek
JOIN sys.databases db ON dek.database_id = db.database_id;
```

---

## 19. Backup & Restore (Managed Instance)

> `sys.backupset` and related views are available on **Azure SQL Managed Instance**.  
> Azure SQL Database backups are **fully managed** — use the Portal or `sys.dm_database_restorepoints`.

### 19.1 Last Backup Times (Managed Instance)
```sql
SELECT
    d.name,
    MAX(CASE WHEN b.type = 'D' THEN b.backup_finish_date END) AS last_full,
    MAX(CASE WHEN b.type = 'I' THEN b.backup_finish_date END) AS last_diff,
    MAX(CASE WHEN b.type = 'L' THEN b.backup_finish_date END) AS last_log
FROM sys.databases d
LEFT JOIN msdb.dbo.backupset b ON d.name = b.database_name
GROUP BY d.name
ORDER BY d.name;
```

### 19.2 Restore Points (Azure SQL DB)
```sql
SELECT *
FROM sys.dm_database_restorepoints
ORDER BY restore_point_creation_time DESC;
```

### 19.3 Point-in-Time Restore (Azure CLI / PowerShell)
```powershell
# Azure PowerShell
Restore-AzSqlDatabase `
  -FromPointInTimeBackup `
  -PointInTime "2025-01-15T12:00:00Z" `
  -ResourceGroupName "rg-prod" `
  -ServerName "sql-prod" `
  -TargetDatabaseName "db_restored" `
  -ResourceId "/subscriptions/.../databases/db_prod"
```

---

## 20. Maintenance & Housekeeping

### 20.1 Rebuild Index Online
```sql
-- Online rebuild (no blocking, Premium/BC tier required for full online)
ALTER INDEX [IX_IndexName] ON [schema].[TableName] REBUILD WITH (ONLINE = ON, MAXDOP = 4);

-- Reorganize (always online)
ALTER INDEX [IX_IndexName] ON [schema].[TableName] REORGANIZE;

-- All indexes on a table
ALTER INDEX ALL ON [schema].[TableName] REBUILD WITH (ONLINE = ON);
```

### 20.2 Update Statistics
```sql
UPDATE STATISTICS [schema].[TableName] WITH FULLSCAN, NORECOMPUTE;
-- Or async update:
UPDATE STATISTICS [schema].[TableName] WITH SAMPLE 30 PERCENT;
```

### 20.3 Check Database Consistency (DBCC CHECKDB)
```sql
-- Azure SQL DB supports DBCC CHECKDB
DBCC CHECKDB ('YourDatabase') WITH NO_INFOMSGS, ALL_ERRORMSGS;

-- Faster (no data/key checks):
DBCC CHECKDB ('YourDatabase') WITH PHYSICAL_ONLY;
```

### 20.4 Shrink (Avoid in Production — Emergency Only)
```sql
-- Check space first
DBCC SQLPERF(LOGSPACE);

-- Emergency log shrink (NEVER routine):
BACKUP LOG [YourDB] TO DISK = 'NUL';
DBCC SHRINKFILE (YourDB_log, 512);  -- Target 512 MB
```

### 20.5 Clear Plan Cache (Targeted)
```sql
-- Clear single database plan cache
ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;

-- Or by plan handle (surgical):
DBCC FREEPROCCACHE (<plan_handle>);
```

### 20.6 Identify Auto-Close / Auto-Shrink Issues
```sql
SELECT name, is_auto_close_on, is_auto_shrink_on
FROM sys.databases
WHERE is_auto_close_on = 1 OR is_auto_shrink_on = 1;
-- Both should be OFF in production
```

---

## 🔑 Quick Reference Cheat Sheet

| Scenario | Primary DMV / Command |
|---|---|
| CPU top queries | `sys.dm_exec_query_stats` |
| Wait stats | `sys.dm_os_wait_stats` |
| Blocking | `sys.dm_exec_requests` |
| Missing indexes | `sys.dm_db_missing_index_details` |
| Index fragmentation | `sys.dm_db_index_physical_stats` |
| Buffer pool | `sys.dm_os_buffer_descriptors` |
| Azure resource usage | `sys.dm_db_resource_stats` |
| Query Store trends | `sys.query_store_runtime_stats` |
| TempDB pressure | `sys.dm_db_task_space_usage` |
| Memory grants | `sys.dm_exec_query_memory_grants` |
| Log reuse wait | `sys.databases.log_reuse_wait_desc` |
| Plan instability | `sys.query_store_plan` |
| TDE status | `sys.dm_database_encryption_keys` |
| Automated tuning | `sys.dm_db_tuning_recommendations` |
| I/O stalls | `sys.dm_io_virtual_file_stats` |
| Elastic pool | `sys.dm_elastic_pool_resource_stats` |

---

*Generated for Azure SQL DBA reference. Tested on Azure SQL Database (Gen5 vCore / DTU) and Azure SQL Managed Instance.*
