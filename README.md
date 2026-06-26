# 🏎️ Formula 1 Race Analysis – Azure Databricks Lakehouse Project

End-to-end data engineering pipeline that ingests historical & incremental Formula 1 data, processes it through a **Medallion (Bronze → Silver → Gold) architecture** on **Azure Databricks**, and visualizes driver/constructor standings via **Databricks SQL Dashboards (Lakeview)**. Every run picks up only the *next unprocessed batch* via a custom control-table framework — no full reprocessing.

## 📐 Architecture

```
ADLS Gen2 (landing/<batch_id>/*.csv,*.json)
        │
        ▼
Databricks Workflows (Job)
  Identify Next Batch → Create Batch(in_progress) → Ingest Bronze → Transform Silver
  → Aggregate Gold → Complete Batch
        │
        ▼
🥉 BRONZE (raw + ingestion metadata)      circuits, races, drivers, constructors, results, sprints
        ▼
🥈 SILVER (cleansed, conformed)            dim_*, fact_results, fact_sprints
        ▼
🥇 GOLD (star schema + aggregates)         dim_drivers/constructors/circuits/races, fact_session_results
        ▼                                  → v_driver_standing, v_constructor_standing
Databricks SQL / Lakeview Dashboard        season standings, points, wins, podiums
```

All schemas/tables governed by **Unity Catalog** on **ADLS Gen2**; storage format is **Delta Lake** throughout.

## 🧱 Medallion Architecture
- **Bronze:** Raw files read via Spark DataFrame Reader API with **explicit, enforced schemas** (`FAILFAST` mode) — including nested JSON (driver names). A shared `add_ingestion_metadata()` helper stamps `source_file` + `ingestion_timestamp`; `write_to_bronze()` performs batch-aware, idempotent Delta writes.
- **Silver:** Deduplicated, type-corrected, conformed dimension/fact structures built from bronze.
- **Gold:** Curated **star schema** (`dim_drivers`, `dim_constructors`, `dim_circuits`, `dim_races`, `fact_session_results`) plus SQL views computing season standings using `RANK() OVER (PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC)`.

## 🔁 Incremental Processing (Control-Table Pattern)
| Step | Notebook | Role |
|---|---|---|
| 1 | `00_Create_Control_Tables.py` | Creates `control.batch_control` (batch_id, status, timestamps) |
| 2 | `01_Identify_Next_Batch.py` | Finds earliest landing batch not yet tracked; passes `batch_id` via `taskValues` |
| 3 | `02_Create_New_Batch.py` | Inserts batch record as `in_progress` |
| 4 | `01–06_Ingest_*_File.py` | Ingests only the current batch's files into bronze, per source |
| 5 | Silver/Gold notebooks | Process that batch through to gold using the same `batch_id` |
| 6 | `03_Complete_Batch.py` | Delta `MERGE` flips status `in_progress → completed` |

**Why:** idempotent (no duplicate loads on retry) · restartable (failed batch stays `in_progress`, retried automatically) · auditable (control table = full lineage log) · scalable (new race-weekend drops auto-detected, no code changes) · decouples orchestration schedule from data arrival.

## ⚙️ Tech Stack
**Cloud:** Microsoft Azure · **Compute:** Azure Databricks (Apache Spark/PySpark) · **Storage:** ADLS Gen2 · **Table format:** Delta Lake (ACID, schema enforcement, `MERGE`, time travel) · **Governance:** Unity Catalog · **Orchestration:** Databricks Workflows (scheduled multi-task Jobs) · **Languages:** Python (PySpark), SQL · **APIs:** Spark DataFrame Reader/Writer, `DeltaTable` MERGE API, `dbutils` (widgets, jobs.taskValues, fs) · **Formats ingested:** CSV, JSON & multi-line JSON · **Modeling:** Dimensional/Star Schema · **Visualization:** Databricks SQL Dashboards (Lakeview) · **Version Control:** Git/GitHub

## 📂 Structure
```
00_Create_Control_Tables.py · 01_Identify_Next_Batch.py · 02_Create_New_Batch.py · 03_Complete_Batch.py
01_Ingest_Circuits_File.py · 02_Ingest_Races_File.py · 03_Ingest_Constructors_File.py
04_Ingest_Drivers_File.py · 05_Ingest_Results_File.py · 06_Ingest_Sprints_File.py
01_Build_Driver_Standings_View.sql · 02_Build_Constructor_Standings_View.sql
circuits.csv · races.csv  (sample source data)
```

## 🚀 Highlights
Custom incremental ingestion framework (no external orchestrator needed) · strict `FAILFAST` schema contracts on every source · clean dimensional star schema for BI · transactional state management via Delta `MERGE` · fully governed via Unity Catalog · orchestrated end-to-end with Databricks Workflows.
