# IEA Nuclear Outage Pipeline

An automated ETL pipeline that tracks and visualizes nuclear power plant outages in the United States using data from the U.S. Energy Information Administration (EIA).

---

## Tech Stack

| Layer      | Technology                              |
|------------|-----------------------------------------|
| Language   | Python 3.10                             |
| Processing | PySpark 3.5.0, Delta Lake 3.1.0 (Silver)|
| Database   | PostgreSQL 15 (Gold, Star Schema)       |
| API        | FastAPI, Uvicorn                        |
| Runtime    | Docker, Docker Compose                  |

---

## Architecture

- **Extract (Bronze):** Raw JSON from [EIA v2 API](https://api.eia.gov/) with retry logic and pagination
- **Transform (Silver):** PySpark DataFrames → Delta Lake (ACID storage, one folder per dataset)
- **Load (Gold):** Clean data → PostgreSQL star schema (`dim_facilities` + fact tables)
- **Serve:** FastAPI REST API + HTML/JS dashboard

**Datasets supported:**

- `facility-nuclear-outages` — facility-level outages  
- `generator-nuclear-outages` — generator-level outages (includes generator name)  
- `us-nuclear-outages` — U.S. national summary  

→ See [docs/01_System_architecture.md](docs/01_System_architecture.md) for details.

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- [EIA API Key](https://www.eia.gov/opendata/register.php) (free)

---

## Setup

### 1. Clone and enter the project

```bash
cd IEA_Nuclear_pipeline
```

### 2. Create `.env` in the project root

```env
API_KEY=your_eia_api_key_here
```

> **Note:** Docker Compose provides `DB_HOST`, `DB_USER`, `DB_PASS`, `DB_NAME`, and `DB_PORT` for the default setup. Override them in `.env` only if needed.

### 3. Start the stack

From the **project root**:

```bash
docker compose -f docker/docker-compose.yml up --build
```

Or from the `docker/` directory:

```bash
cd docker && docker compose up --build
```

Wait until both containers are healthy (Postgres first, then ETL).

---

## Access

| URL                     | Description                         |
|-------------------------|-------------------------------------|
| http://localhost:8000/dashboard | Main command center (query & trigger pipeline) |
| http://localhost:8000/monitor   | System monitor view                 |

**API endpoints:**

- `GET /data?dataset=...&period=...&limit=100&offset=0` — Query outage data
- `POST /refresh?dataset=facility-nuclear-outages` — Trigger ETL for a dataset

---

## Project Structure

```
IEA_Nuclear_pipeline/
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── init.sql              # Star schema (dim_facilities, fact_*)
├── docs/
│   └── 01_System_architecture.md
├── src/
│   ├── api.py                # FastAPI routes
│   ├── main.py               # ETL orchestration
│   ├── extract_nuclear_data.py
│   ├── delta_pyspark.py      # Silver layer
│   ├── postgres.py           # Gold layer
│   ├── config.py
│   ├── postgres_connection.py
│   ├── spark_Setup.py
│   └── static/
│       ├── index.html        # Dashboard
│       └── monitor.html      # Monitor view
├── tests/
│   ├── conftest.py
│   └── unit_tests.py
├── .env                      # API_KEY (create; see Setup)
└── README.md
```

---

## Usage

1. **Trigger pipeline:** Use "Trigger Pipeline" in the dashboard or monitor. Choose the dataset (facility, generator, or US national) and run.
2. **Query data:** Select a dataset and optional date filter, then click "Query Database".
3. **Logs:** Follow ETL progress with `docker logs -f eia_pyspark_etl`.

---

## Testing

Unit tests run inside the ETL container:

```bash
docker compose -f docker/docker-compose.yml run etl-pipeline pytest /app/tests -v
```

---

## License

See [LICENSE](LICENSE).
