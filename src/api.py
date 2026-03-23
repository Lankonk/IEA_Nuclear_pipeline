import logging
import os
from fastapi.responses import FileResponse
from fastapi import FastAPI, Query, BackgroundTasks, HTTPException
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from config import DB_CONFIG
from main import run_pipeline

app = FastAPI(title="Nuclear Outages Data API")

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

@app.get("/data")
async def get_data(
    dataset: str = Query("facility-nuclear-outages"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    period: Optional[str] = Query(None),
    facility_id: Optional[str] = Query(None)
):
    valid_datasets = ["facility-nuclear-outages", "generator-nuclear-outages", "us-nuclear-outages", "dim_facilities"]
    if dataset not in valid_datasets:
        raise HTTPException(status_code=400, detail="Invalid dataset name")

    try:
        conn = get_db_conn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        #queries for each table
        if dataset == "facility-nuclear-outages":
            query = "SELECT f.period, d.plant_name, f.outage FROM fact_facility_outages f JOIN dim_facilities d ON f.facility_id = d.facility_id"
            prefix = "f" # used to distinguish between table columns
        elif dataset == "generator-nuclear-outages":
            query = "SELECT f.period, d.plant_name, f.generator, f.outage FROM fact_generator_outages f JOIN dim_facilities d ON f.facility_id = d.facility_id"
            prefix = "f"
        elif dataset == "us-nuclear-outages":
            query = "SELECT period, outage FROM fact_us_outages"
            prefix = "" # no join so no prefix
        else:
            query = "SELECT facility_id, plant_name FROM dim_facilities"
            prefix = ""

        # all filters to a list
        where_clauses = []
        params = []

        if period:
            where_clauses.append(f"{prefix + '.' if prefix else ''}period = %s")
            params.append(period)

        # Only filter by plant if a specific one is selected
        if facility_id and facility_id != "facility-nuclear-outages":
            where_clauses.append(f"{prefix + '.' if prefix else ''}facility_id = %s")
            params.append(facility_id)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        #add order by where the data can be ordered
        if dataset != "dim_facilities":
            query += f" ORDER BY {prefix + '.' if prefix else ''}period DESC"
        
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        cursor.execute(query, tuple(params))
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        return results

    except Exception as e:
        logging.error(f"Error querying data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/refresh")
async def refresh_data(
    background_tasks: BackgroundTasks,
    dataset: str = Query("facility-nuclear-outages")
):

    background_tasks.add_task(run_pipeline, dataset) #extracts info from the api
    return {"status": "accepted", "message": f"Pipeline started for {dataset}"}

@app.get("/facilities")
async def get_facilities():
    try:
        conn = get_db_conn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Simple query to get the lookup list
        query = "SELECT facility_id, plant_name FROM dim_facilities ORDER BY plant_name ASC" #gets all facilities
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        return results

    except Exception as e:
        logging.error(f"Error fetching metadata: {e}")
        raise HTTPException(status_code=500, detail="Could not retrieve facility list")

@app.get("/monitor")
async def serve_monitor():
    """Serves the new Cinder Outage Monitor page."""
    return FileResponse('/app/src/static/monitor.html')

@app.get("/dashboard")
async def serve_dashboard(): #loads index.html
    return FileResponse('/app/src/static/index.html')
