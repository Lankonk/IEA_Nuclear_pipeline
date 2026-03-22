# IEA Nuclear Pipeline — System Architecture

## Overview

The IEA Nuclear Pipeline is an ETL (Extract, Transform, Load) system that fetches US nuclear outage data from the EIA API, processes it through a Delta Lake staging layer, and loads it into PostgreSQL for serving via a REST API and dashboard.

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
| Silver | `delta_pyspark.py`       | Transform and upsert (merge) data into Delta tables via PySpark      |
| Gold   | `postgres.py`            | Load filtered data into PostgreSQL fact and dimension tables         |

Orchestration is handled by `main.py`:

```python
# 1. Extract
raw_data = get_nuclear_outages()

# 2. Transform & Stage (Delta)
upsert_to_delta(raw_data)

# 3. Load & Serve (PostgreSQL)
push_to_postgres()
```

---

## Module Reference

### `main.py`

- **Purpose:** ETL orchestration and entry point.
- **Flow:** Extract → Delta upsert → PostgreSQL load. Aborts if extraction returns no data.

### `extract_nuclear_data.py`

- **Purpose:** Fetch nuclear outage data from the EIA v2 API.
- **Endpoint:** `https://api.eia.gov/v2/nuclear-outages/facility-nuclear-outages/data`
- **Features:**
  - Pagination (offset/length) until all records are retrieved
  - Retry session for transient HTTP errors (500, 502, 503, 504)
  - Safety limit of 5,000 records
  - Requires `API_KEY` environment variable

### `delta_pyspark.py`

- **Purpose:** Transform raw data and upsert into Delta Lake.
- **Schema:** `period`, `facility`, `plant_name`, `outage`, `outage-units`, `state`, `capacity`
- **Logic:** MERGE on `(plant_name, period)` — update existing rows, insert new ones. Creates table if missing.

### `postgres.py`

- **Purpose:** Load data from Delta into PostgreSQL.
- **Tables:**
  - **plants** (dimension): `plant_name`, `state` — mode `ignore` to preserve schema
  - **annual_outages** (fact): `period`, `plant_name`, `capacity`, `outage` — mode `overwrite`

### `spark_Setup.py`

- **Purpose:** Create a Spark session with Delta and PostgreSQL JDBC support.
- **Extensions:** `DeltaSparkSessionExtension`, `DeltaCatalog`
- **Packages:** `delta-spark_2.12:3.1.0`, `postgresql:42.6.0`

### `config.py`

- **Purpose:** Central configuration from environment variables.
- **Config:** `API_KEY`, `EIA_BASE_URL`, `DB_*`, `JDBC_URL`, `DELTA_TABLE_PATH` (`/tmp/delta/eia_nuclear_outages`)

### `postgres_connection.py`

- **Purpose:** PostgreSQL connection factory using `psycopg2`.
- **Usage:** Used by the FastAPI `/data` endpoint for queries.

### `api.py`

- **Purpose:** FastAPI REST API and dashboard serving.
- **Endpoints:**

| Method | Path        | Description                                              |
|--------|-------------|----------------------------------------------------------|
| GET    | `/dashboard`| Serves the static HTML dashboard                         |
| POST   | `/refresh`  | Triggers the ETL pipeline as a background task           |
| GET    | `/data`     | Returns nuclear outage data with filters and pagination  |

**`/data` query parameters:**
- `limit` (1–1000, default 100)
- `offset` (default 0)
- `period` (YYYY-MM-DD)
- `plant_name` (partial, case-insensitive)
- `state` (partial, case-insensitive)

### `static/index.html`

- **Purpose:** Single-page dashboard for nuclear outage data.
- **Features:** Filters (limit, offset, date, state, plant), table display, and a button to trigger ETL refresh via POST `/refresh`.

---

## Design Rationale

### Delta Lake as a Middle Layer

Delta tables sit between extraction and PostgreSQL:

- **Filtering:** Heavy transformations and deduplication are done in Delta (upsert logic).
- **PostgreSQL:** Stores only curated, filtered data for fast reads.
- **Benefit:** Keeps complex filtering and large-volume processing out of PostgreSQL, improving query latency for the API and dashboard.

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
| `DB_HOST`       | PostgreSQL host          | `localhost`  |
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
