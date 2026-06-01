# 🚀 SpaceX Data Pipeline

An end-to-end data engineering project that collects, transforms, and analyzes SpaceX launch data to uncover insights about mission success rates, rocket performance, and launch pad reliability.

---

## 📋 Project Overview

This project builds a fully automated data pipeline that answers key business questions about SpaceX's launch history:

- How has SpaceX's launch success rate evolved over the years?
- Which rockets have the best performance and cost efficiency?
- Which launch pads have the highest reliability?

Data is collected from the public [SpaceX API v4](https://github.com/r-spacex/SpaceX-API), processed through a medallion architecture, and served as analytical dashboards in Apache Superset.

---

## 🏗️ Architecture

```
SpaceX API (v4)
      │
      ▼
┌─────────────────────────────────────────────┐
│              Apache Airflow                  │
│  (Orchestration + Scheduling — 0 6 * * *)   │
└──────────┬──────────────┬───────────────────┘
           │              │
    ┌──────▼──────┐ ┌─────▼──────┐
    │   launches  │ │  rockets   │  Bronze Layer
    │  (raw JSONB)│ │ launchpads │  (PostgreSQL)
    └──────┬──────┘ └─────┬──────┘
           │              │
           └──────┬───────┘
                  │  dbt run
                  ▼
    ┌─────────────────────────┐
    │     silver.launches     │
    │     silver.rockets      │  Silver Layer
    │     silver.launchpads   │  (PostgreSQL)
    │  silver.launches_       │
    │       enriched          │
    └────────────┬────────────┘
                 │  dbt run
                 ▼
    ┌─────────────────────────┐
    │  gold.launches_by_year  │
    │ gold.launches_by_rocket │  Gold Layer
    │  gold.launchpad_summary │  (ClickHouse)
    └────────────┬────────────┘
                 │
                 ▼
         Apache Superset
         (Dashboards & BI)
```

---

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| **Apache Airflow** | 2.9.1 | Pipeline orchestration & scheduling |
| **PostgreSQL** | 17 | Bronze & Silver layer storage (OLTP) |
| **dbt-core** | 1.7.0 | Data transformation & testing |
| **dbt-postgres** | 1.7.0 | dbt adapter for PostgreSQL |
| **dbt-clickhouse** | 1.7.0 | dbt adapter for ClickHouse |
| **ClickHouse** | 24.3 | Gold layer analytics storage (OLAP) |
| **Apache Superset** | 3.1.0 | Business intelligence & dashboards |
| **Docker Compose** | — | Container orchestration |
| **Python** | 3.12 | DAG logic & API ingestion |

---

## 📊 Data Pipeline

The pipeline runs automatically every day at 06:00 UTC via Apache Airflow.

**DAG: `spacex_launches_pipeline`**

```
create_bronze_tables
        │
        ├──────────────────────────────┐
        ▼                              ▼                        ▼
fetch_launches              fetch_rockets            fetch_launchpads
        │                              │                        │
        ▼                              ▼                        ▼
load_launches               load_rockets             load_launchpads
        │                              │                        │
        └──────────────────────────────┘
                           │
                           ▼
                    dbt_run_silver
                    (4 models: launches, rockets,
                     launchpads, launches_enriched)
                           │
                           ▼
                       dbt_test
                    (16 data quality tests)
                           │
                           ▼
                    dbt_run_gold
                    (3 ClickHouse models)
```

**Medallion Architecture:**

| Layer | Storage | Description |
|---|---|---|
| **Bronze** | PostgreSQL (`bronze` schema) | Raw JSONB data as received from API |
| **Silver** | PostgreSQL (`silver` schema) | Typed, cleaned, enriched with JOINs |
| **Gold** | ClickHouse (`gold` database) | Aggregated business metrics for analytics |

---

## 🗂️ Data Model

### Bronze Layer (Raw)
| Table | Rows | Description |
|---|---|---|
| `bronze.launches` | 205 | All SpaceX launches as raw JSONB |
| `bronze.rockets` | 4 | Rocket metadata as raw JSONB |
| `bronze.launchpads` | 6 | Launch pad metadata as raw JSONB |

### Silver Layer (Cleaned & Typed)
| Table | Description |
|---|---|
| `silver.launches` | Typed columns: `launch_id`, `mission_name`, `flight_number`, `launch_date_utc` (TIMESTAMPTZ), `is_success` (BOOLEAN), etc. |
| `silver.rockets` | `rocket_id`, `rocket_name`, `country`, `cost_per_launch`, `is_active`, `first_flight_date` |
| `silver.launchpads` | `launchpad_id`, `launchpad_name`, `region`, `latitude`, `longitude`, `status` |
| `silver.launches_enriched` | Launches joined with rocket and launchpad names |

### Gold Layer (Analytics — ClickHouse)
| Table | Description |
|---|---|
| `gold.launches_by_year` | Total launches, success rate per year (2006–2022) |
| `gold.launches_by_rocket` | Performance metrics per rocket (Falcon 1 / 9 / Heavy) |
| `gold.launchpad_summary` | Reliability metrics per launch pad by region |

---

## 📈 Results

**Key findings from the data:**

| Metric | Value |
|---|---|
| Total launches analyzed | **205** |
| Falcon 9 success rate | **98.32%** (179 launches) |
| KSC LC 39A success rate | **100%** (55 launches) |
| First SpaceX launch | 2006 (Falcon 1, 40% early success rate) |
| Peak year | 2022 — **44 launches** |

**Success rate evolution:**
- 2006–2008: ~40% (Falcon 1 early failures)
- 2009–2014: 100% (Falcon 9 established)
- 2015–2016: ~87% (CRS-7 and Amos-6 anomalies)
- 2017–2022: ~98%+ (industrial scale reliability)

---

## 🚀 How to Run

### Prerequisites
- Docker & Docker Compose installed
- 8 GB RAM available for containers

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/ustaznet-dotcom/spacex_data_pipeline.git
cd spacex_data_pipeline

# 2. Create environment file
cp .env.example .env
# Edit .env with your credentials (or use defaults for local dev)

# 3. Build and start all services
docker compose build
docker compose up -d

# 4. Wait ~2 minutes for initialization, then verify
docker compose ps
```

### Access Services

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| ClickHouse HTTP | http://localhost:8123/play | — |
| PostgreSQL | localhost:5432 | admin / admin |

### Run the Pipeline

Trigger manually in Airflow UI → DAGs → `spacex_launches_pipeline` → ▶ Trigger DAG

Or wait for the daily schedule at 06:00 UTC.

---

## 📁 Project Structure

```
spacex_data_pipeline/
├── dags/
│   └── spacex_launches_dag.py    # Airflow DAG
├── dbt/
│   └── spacex_dbt/
│       ├── models/
│       │   ├── silver/           # Silver layer models
│       │   └── gold/             # Gold layer models
│       └── profiles.yml          # dbt connection config
├── config/
│   └── superset_config.py        # Superset configuration
├── clickhouse/
│   └── users.d/                  # ClickHouse user config
├── Dockerfile.airflow             # Airflow + dbt image
├── Dockerfile.superset            # Superset + clickhouse-connect image
├── docker-compose.yaml
└── init-db.sql                   # PostgreSQL database initialization
```

---

## 🔧 Data Quality

The pipeline includes **16 automated dbt tests** that run after every Silver transformation:

- `unique` + `not_null` on all primary keys
- `not_null` on critical business columns (`mission_name`, `launch_date_utc`, `rocket_id`)
- Source-level tests on Bronze tables to catch API anomalies early

If any test fails, the pipeline stops before loading Gold — preventing bad data from reaching dashboards.
