# ADF Pipeline Runs Table — Setup Guide

This document explains how to wire Azure Data Factory (ADF) pipeline run metadata into DQS so that **Check 32 (Pipeline Failure Indicator)** can monitor DataStream and other ADF pipeline health.

---

## Why This Is Needed

Check 32 queries a `pipeline_runs_table` with this schema:

| Column | Type | Description |
|--------|------|-------------|
| `dataset_name` | VARCHAR | Name of the dataset/table this run loaded |
| `run_status` | VARCHAR | `'success'` \| `'failed'` \| `'running'` |
| `run_ts` | DATETIME | Timestamp when the run completed (or was last updated) |

ADF does not write this table natively, so you need to populate it. Two options are described below.

---

## Option A — Custom Run Log Table (Recommended)

### Step 1: Create the table in Synapse

Run this DDL in your Synapse dedicated pool (e.g. in the `dbo` schema of your DDS database):

```sql
CREATE TABLE dbo.dqs_pipeline_runs
(
    run_id          BIGINT IDENTITY(1,1) NOT NULL,
    dataset_name    NVARCHAR(256)        NOT NULL,
    pipeline_name   NVARCHAR(256)        NULL,
    run_status      NVARCHAR(50)         NOT NULL,   -- 'success' | 'failed' | 'running'
    run_ts          DATETIME2            NOT NULL DEFAULT SYSUTCDATETIME(),
    error_message   NVARCHAR(MAX)        NULL,
    row_count       BIGINT               NULL,
    duration_sec    INT                  NULL
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
);
```

### Step 2: Add an ADF Web Activity at the end of each pipeline

At the **end** of every ADF pipeline that loads a DQS-monitored dataset, add a **Stored Procedure Activity** (or **Script Activity**) that inserts a run record.

**Success path** (connect from the last successful activity):

```sql
INSERT INTO dbo.dqs_pipeline_runs
    (dataset_name, pipeline_name, run_status, run_ts, row_count)
VALUES
    ('@{pipeline().parameters.dataset_name}',
     '@{pipeline().Pipeline}',
     'success',
     SYSUTCDATETIME(),
     @{activity('CopyData').output.rowsCopied})
```

**Failure path** (connect with "On Failure" dependency from the last activity):

```sql
INSERT INTO dbo.dqs_pipeline_runs
    (dataset_name, pipeline_name, run_status, run_ts, error_message)
VALUES
    ('@{pipeline().parameters.dataset_name}',
     '@{pipeline().Pipeline}',
     'failed',
     SYSUTCDATETIME(),
     '@{activity('CopyData').error.message}')
```

> **Tip:** Add a pipeline parameter `dataset_name` to each ADF pipeline so the value flows through cleanly. This also makes it easy for DQS Check 32 to filter by dataset.

### Step 3: Configure DQS Check 32

In your scan YAML, add:

```yaml
- check_id: 32
  table: dbo.store_sales          # the table being monitored
  pipeline_runs_table: dbo.dqs_pipeline_runs
  dataset_name: store_sales       # matches the dataset_name inserted by ADF
```

DQS will then query:
```sql
SELECT
    COUNT(CASE WHEN LOWER(run_status) = 'failed' THEN 1 END) /
    NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS pipeline_failure_ratio
FROM dbo.dqs_pipeline_runs
WHERE LOWER(dataset_name) = LOWER('store_sales')
```

---

## Option B — ADF Diagnostic Logs via Log Analytics (Advanced)

ADF can stream pipeline run diagnostics to a **Log Analytics workspace** natively (via Diagnostic Settings in the Azure portal). From there, you can expose the data in Synapse via:

1. **Synapse Link for Log Analytics** — available in preview; exposes Log Analytics tables as external tables in Synapse
2. **Azure Data Explorer (ADX) export** → Synapse external table via PolyBase

This approach requires no changes to ADF pipelines but adds infrastructure complexity. It is recommended only if you already have Log Analytics integrated and need a zero-touch ADF instrumentation path.

The DQS query remains the same — point `pipeline_runs_table` at the exposed external table.

---

## Recommended Pipeline Coverage

| ADF Pipeline | Dataset Name | Priority |
|---|---|---|
| DataStream → DDS load | `compass_calls`, `rx_claims` | High |
| ADP-to-SAP employee file | `adp_employees` | High |
| IQVIA Rx weekly | `iqvia_rx` | High |
| FLOQAST journal export | `floqast_journal` | Medium |
| CONCUR expense extract | `concur_expenses` | Medium |
| MDM / Veeva enrichment | `hcp_golden_profile` | Medium |

---

## Threshold

Check 32 default threshold: **0.0** (any failed run triggers the check). To allow occasional failures, set `threshold: 0.1` in the check config (up to 10% of recent runs may fail before the check fires).
