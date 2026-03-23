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
    dataset: str = Query("facility-nuclear-outages", description="The dataset to query"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    period: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)")
):
    """
    Dynamically fetches data from the selected Fact table, 
    joining with the Dimension table where necessary.
    """
    try:
        conn = get_db_conn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Dynamic SQL Router
        if dataset == "facility-nuclear-outages":
            query = """
                SELECT f.period, d.plant_name, f.outage 
                FROM fact_facility_outages f
                JOIN dim_facilities d ON f.facility_id = d.facility_id
            """
        elif dataset == "generator-nuclear-outages":
            query = """
                SELECT f.period, d.plant_name, f.generator, f.outage 
                FROM fact_generator_outages f
                JOIN dim_facilities d ON f.facility_id = d.facility_id
            """
        elif dataset == "us-nuclear-outages":
            # National data has no facility join
            query = "SELECT period, outage FROM fact_us_outages"
        else:
            raise HTTPException(status_code=400, detail="Invalid dataset name")

        # Add filtering and pagination
        params = []
        if period:
            query += " WHERE period = %s"
            params.append(period)

        query += " ORDER BY period DESC LIMIT %s OFFSET %s"
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

@app.get("/dashboard")
async def serve_dashboard(): #loads index.html
    return FileResponse('/app/src/static/index.html')