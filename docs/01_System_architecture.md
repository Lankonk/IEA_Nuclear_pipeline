# IEA Nuclear Pipeline — System Architecture

## Overview

The IEA Nuclear Pipeline is an ETL (Extract, Transform, Load) system that fetches US nuclear outage data from the EIA API, processes it through a Delta Lake staging layer, and loads it into PostgreSQL for serving via a REST API and dashboard.

The pipeline supports **three datasets**:
- **facility-nuclear-outages** — facility-level outages
- **generator-nuclear-outages** — generator-level outages (includes generator name)
- **us-nuclear-outages** — U.S. national summary (no facility dimension)

**Project Requirements:**
- API Connection
- Error handling
- Logging
- Pagination
- Database
- REST API
- Frontend interface
- Unit tests

---

## High-Level Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌──────────────────┐
│   EIA API       │────▶│  Extract         │────▶│  Delta Lake     │────▶│  PostgreSQL      │
│   (External)    │     │  (Bronze)        │     │  (Silver)       │     │  (Gold)          │
└─────────────────┘     └──────────────────┘     └─────────────────┘     └────────┬─────────┘
         ▲                        │                         │                      │
         │                        │                         │                      │
         │                        └─────────────────────────┴──────────────────────┘
         │                                         main.py (ETL orchestration)
         │
         │                        ┌──────────────────┐
         └────────────────────────│  FastAPI         │◀──── Dashboard (static HTML)
                                  │  REST API        │
                                  └──────────────────┘
```

---

## Infrastructure

### Docker Compose

The system runs as two services on a shared bridge network (`data_network`):

| Service      | Image                | Port | Role                                                  |
|--------------|----------------------|------|-------------------------------------------------------|
| **postgres** | postgres:15-alpine   | 5432 | PostgreSQL database for serving data                  |
| **etl-pipeline** | Custom (Python) | 8000 | Runs ETL logic, FastAPI, and serves the dashboard     |

**Volumes:**
- `postgres_data` — persistent PostgreSQL storage
- `delta_lake_data` — Delta Lake tables at `/tmp/delta`

**Dependencies:** The ETL container waits for PostgreSQL to be healthy before starting (`depends_on` with `condition: service_healthy`).

---

## Data Flow (ETL Pipeline)

The pipeline follows a **Bronze → Silver → Gold** architecture:

| Layer  | Component        | Action                                                                 |
|--------|------------------|------------------------------------------------------------------------|
| Bronze | `extract_nuclear_data.py` | Fetch raw JSON from EIA API with pagination and retry logic          |
| Silver | `delta_pyspark.py`       | Transform and write to Delta Lake (overwrite per dataset); return DataFrame |
| Gold   | `postgres.py`            | Load DataFrame into PostgreSQL star-schema tables (fact + dimension)  |

Orchestration is handled by `main.py`:

```python
def run_pipeline(dataset_name: str):
    # 1. Extract
    raw_data = get_nuclear_outages(dataset_name=dataset_name)

    # 2. Transform & Stage (Delta)
    df = upsert_to_delta(raw_data, dataset_name)

    # 3. Load & Serve (PostgreSQL)
    if df:
        push_to_postgres(df, dataset_name)
```

The pipeline is **dataset-driven**: each run processes a single dataset (e.g. `facility-nuclear-outages`, `generator-nuclear-outages`, `us-nuclear-outages`).

---

## Module Reference

### `main.py`

- **Purpose:** ETL orchestration and entry point.
- **Signature:** `run_pipeline(dataset_name: str)` — runs the full pipeline for a single dataset.
- **Flow:** Extract → Delta write → PostgreSQL load. Aborts if extraction returns no data. Validates that Delta returns a DataFrame before loading to Postgres.

### `extract_nuclear_data.py`

- **Purpose:** Fetch nuclear outage data from the EIA v2 API.
- **Signature:** `get_nuclear_outages(dataset_name: str, frequency="daily", offset=0, length=1000)`
- **Endpoint:** `https://api.eia.gov/v2/nuclear-outages/{dataset_name}/data` (URL varies by dataset)
- **Features:**
  - Pagination (offset/length) until all records are retrieved
  - Retry session for transient HTTP errors (500, 502, 503, 504)
  - Safety limit of 5,000 records
  - Requires `API_KEY` environment variable

### `delta_pyspark.py`

- **Purpose:** Transform raw JSON into a Spark DataFrame and write to Delta Lake.
- **Signature:** `upsert_to_delta(raw_data, dataset_name)` → returns DataFrame.
- **Schema:** `period`, `facility`, `facilityName`, `generator`, `outage`, `outage-units` (universal schema; missing fields become null).
- **Logic:** Overwrites Delta table at `{DELTA_TABLE_PATH}/{dataset_name}`. Returns the DataFrame for direct pass-through to PostgreSQL (no re-read).
- **Spark:** Defines `get_spark_session()` inline with Delta and PostgreSQL JDBC support.

### `postgres.py`

- **Purpose:** Load data from the Spark DataFrame into PostgreSQL star-schema tables.
- **Signature:** `push_to_postgres(df, dataset_name)` — receives DataFrame directly, not Delta path.
- **Tables:**
  - **dim_facilities** (dimension): `facility_id`, `plant_name` — used by facility and generator datasets; mode `overwrite`
  - **fact_facility_outages**: `period`, `facility_id`, `outage` — for `facility-nuclear-outages`
  - **fact_generator_outages**: `period`, `facility_id`, `generator`, `outage` — for `generator-nuclear-outages`
  - **fact_us_outages**: `period`, `outage` — for `us-nuclear-outages` (no facility join)

### `spark_Setup.py`

- **Purpose:** Create a Spark session with Delta and PostgreSQL JDBC support.
- **Note:** Currently not imported by `delta_pyspark.py`, which defines its own `get_spark_session()`. Kept for possible standalone use.

### `config.py`

- **Purpose:** Central configuration from environment variables.
- **Config:** `API_KEY`, `EIA_BASE_URL`, `DB_*`, `DB_CONFIG` (dict for psycopg2), `PROPERTIES` (dict for JDBC), `JDBC_URL`, `DELTA_TABLE_PATH`.
- **DB_HOST default:** `"db"` (Docker service name).

### `postgres_connection.py`

- **Purpose:** PostgreSQL connection factory using `psycopg2`.
- **Note:** Not used by `api.py`; the API uses `get_db_conn()` with `DB_CONFIG` from `config.py`.

### `api.py`

- **Purpose:** FastAPI REST API and dashboard serving.
- **Endpoints:**

| Method | Path        | Description                                              |
|--------|-------------|----------------------------------------------------------|
| GET    | `/dashboard`| Serves the static HTML dashboard                         |
| POST   | `/refresh`  | Triggers the ETL pipeline for a dataset (background)     |
| GET    | `/data`     | Returns nuclear outage data for a dataset with filters   |

**`/data` query parameters:**
- `dataset` (default `facility-nuclear-outages`): `facility-nuclear-outages`, `generator-nuclear-outages`, or `us-nuclear-outages`
- `limit` (1–1000, default 100)
- `offset` (default 0)
- `period` (YYYY-MM-DD) — optional filter

**Dynamic SQL routing:** The `/data` endpoint routes to the appropriate fact/dimension tables based on `dataset`.

### `static/index.html`

- **Purpose:** Single-page dashboard ("Nuclear Outage Command Center").
- **Features:**
  - Dataset selector dropdown (facility, generator, US national)
  - Date filter (period)
  - Dynamic table headers/columns based on selected dataset
  - "Trigger Pipeline" button to run ETL for the selected dataset
  - Query and refresh operations pass `dataset` parameter to the API

---

## Database Schema (Star Schema)

| Table                   | Type     | Columns                                         | Dataset(s)              |
|-------------------------|----------|--------------------------------------------------|-------------------------|
| **dim_facilities**      | Dimension| `facility_id`, `plant_name`                      | facility, generator     |
| **fact_facility_outages**| Fact    | `period`, `facility_id`, `outage`                | facility-nuclear-outages|
| **fact_generator_outages**| Fact   | `period`, `facility_id`, `generator`, `outage`   | generator-nuclear-outages|
| **fact_us_outages**     | Fact     | `period`, `outage`                               | us-nuclear-outages      |

The `/data` API joins fact tables with `dim_facilities` when a facility dimension exists (facility and generator datasets); US national data has no dimension join.

---

## Design Rationale

### Delta Lake as a Middle Layer

Delta tables sit between extraction and PostgreSQL:

- **Staging:** Raw data is transformed into a structured DataFrame and persisted to Delta (one folder per dataset).
- **Pass-through:** The DataFrame is passed directly from Silver to Gold, avoiding a re-read from Delta.
- **PostgreSQL:** Stores only curated, star-schema data for fast reads.
- **Benefit:** Keeps complex transformations and large-volume processing out of PostgreSQL, improving query latency for the API and dashboard.

### PostgreSQL for Serving

PostgreSQL is used for:

- Fast access to pre-processed data
- Relational schema (fact/dimension) suitable for analytics
- JDBC integration with Spark for bulk loads

### Python

Python was chosen for:

- Broad library ecosystem (requests, PySpark, FastAPI, psycopg2)
- Simplicity and maintainability
- Scalability via Spark for data processing

---

## Environment Variables

| Variable        | Description              | Default      |
|-----------------|--------------------------|--------------|
| `API_KEY`       | EIA API key (required)   | —            |
| `DB_HOST`       | PostgreSQL host          | `db` (Docker)|
| `DB_PORT`       | PostgreSQL port          | `5432`       |
| `DB_USER`       | Database user            | `postgres`   |
| `DB_PASS`       | Database password        | `password`   |
| `DB_NAME`       | Database name            | `eia_data`   |
| `SPARK_DRIVER_MEMORY` | Spark driver memory | `2g`    |

---

## Dependencies

Core libraries (see Dockerfile / requirements):

- **requests** — EIA API calls
- **pyspark** (3.5.0) — Distributed processing
- **delta-spark** (3.1.0) — Delta Lake support
- **fastapi** — REST API
- **uvicorn** — ASGI server
- **psycopg2-binary** — PostgreSQL connectivity
