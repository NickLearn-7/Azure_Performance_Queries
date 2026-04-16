# Chat Conversation

Note: _This is purely the output of the chat conversation and does not contain any raw data, codebase snippets, etc. used to generate the output._

### User Input

Give me Azure SQL DBA all Performance, DBA level queries 

*Edited relevant file*

### User Input

Continue

*Edited relevant file*

### Planner Response

Here's what's covered in **Part 2** — [azure_sql_dba_queries_part2.md](file:///C:/Users/n.a.sharad.deshmukh/.gemini/antigravity/brain/4250617b-3213-4939-81ad-2a68be2b279b/azure_sql_dba_queries_part2.md):

| # | Section | Key Highlights |
|---|---|---|
| 21 | **In-Memory OLTP** | Table memory stats, XTP GC, checkpoint files, natively compiled proc stats |
| 22 | **Columnstore** | Rowgroup quality, delta store inspection, force compression |
| 23 | **HA & Geo-Replication** | Lag monitoring, AG replica health, failover state, listener info |
| 24 | **Extended Events** | Create XE sessions, read ring buffer, deadlock capture |
| 25 | **SQL Agent (MI)** | Job status, execution history, running jobs, failed job alerts |
| 26 | **Change Tracking & CDC** | CT query, CDC change functions, CDC latency |
| 27 | **Plan Guides & Hints** | Create/validate/drop plan guides, USE HINT, OPTIMIZE FOR UNKNOWN |
| 28 | **Resource Governor** | Pool/workload stats, live metrics |
| 29 | **Hyperscale DMVs** | Page server I/O, replica states, resource governance |
| 30 | **Performance Baselines** | Wait stats snapshots, delta analysis, DTU trend by hour |
| 31 | **Proactive Alerts** | 10 ready-to-schedule alert queries (CPU, blocking, geo-lag, jobs, etc.) |
| 32 | **Interview One-Liners** | Quick DBA diagnostics from memory |

---

**Together, both parts cover 32 sections across ~120 production-grade T-SQL queries.** You can use Part 1 for daily ops and Part 2 for advanced diagnostics, alerting, and MI-specific features. Want me to export these as a combined HTML/PDF cheat sheet or add any specific topic?