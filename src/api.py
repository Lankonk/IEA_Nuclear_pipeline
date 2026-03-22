import os
import logging
import psycopg2
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from typing import Optional
from psycopg2.extras import RealDictCursor
from postgres_connection import get_db_connection
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from main import run_pipeline

from config import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI(
    title="EIA Nuclear Outages API",
    description="API to serve and refresh US nuclear outage data."
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, replace "*" with your frontend's actual domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/dashboard")
async def serve_dashboard():
    """Serves the frontend HTML dashboard."""
    # This path is relative to where api.py is running (/app/src)
    file_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    
    if os.path.exists(file_path):
        return FileResponse(file_path)
    else:
        raise HTTPException(status_code=404, detail="Dashboard file not found.")

@app.post("/refresh")
async def refresh_data(background_tasks: BackgroundTasks):
    """
    Triggers the ETL pipeline to fetch new data from the EIA API.
    Runs as a background task so the API responds immediately.
    """
    background_tasks.add_task(run_pipeline)
    return {
        "status": "success", 
        "message": "ETL pipeline refresh triggered in the background. Check container logs for progress."
    }

@app.get("/data")
async def get_data(
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    period: Optional[str] = Query(None, description="Filter by specific date (YYYY-MM-DD)"),
    plant_name: Optional[str] = Query(None, description="Filter by plant name (partial match)"),
    state: Optional[str] = Query(None, description="Filter by state abbreviation")
):
    """Retrieves filtered nuclear outage data from the PostgreSQL database."""
    conn = get_db_connection()
    # RealDictCursor ensures the database rows are returned as Python dictionaries (easily converted to JSON)
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Build the base query joining the fact (annual_outages) and dimension (plants) tables
    query = """
        SELECT 
            o.period, 
            o.plant_name, 
            p.state, 
            o.capacity, 
            o.outage
        FROM annual_outages o
        LEFT JOIN plants p ON o.plant_name = p.plant_name
        WHERE 1=1
    """
    params = []

    # Dynamically add filters
    if period:
        query += " AND o.period = %s"
        params.append(period)
    if plant_name:
        query += " AND o.plant_name ILIKE %s" # ILIKE for case-insensitive partial matching
        params.append(f"%{plant_name}%")
    if state:
        query += " AND p.state ILIKE %s"
        params.append(f"%{state}%")

    # Add pagination and sorting
    query += " ORDER BY o.period DESC, o.plant_name ASC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    try:
        cursor.execute(query, params)
        records = cursor.fetchall()
        return {
            "count": len(records),
            "limit": limit,
            "offset": offset,
            "data": records
        }
    except Exception as e:
        logging.error(f"Error querying data: {e}")
        raise HTTPException(status_code=500, detail="Error querying database")
    finally:
        cursor.close()
        conn.close()