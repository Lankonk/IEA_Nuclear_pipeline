import logging
from extract_nuclear_data import get_nuclear_outages
from delta_pyspark import upsert_to_delta
from postgres import push_to_postgres

logging.basicConfig(level=logging.INFO)

def run_pipeline():
    logging.info("--- STARTING EIA ELT PIPELINE ---")
    
    # Step 1: Extract (The Bronze Layer)
    raw_data = get_nuclear_outages()
    
    if not raw_data:
        logging.error("Pipeline aborted: No data extracted.")
        return

    # Step 2: Transform & Stage (The Silver Layer)
    upsert_to_delta(raw_data)
    
    # Step 3: Load & Serve (The Gold Layer)
    push_to_postgres()
    
    logging.info("--- PIPELINE COMPLETE ---")

if __name__ == "__main__":
    run_pipeline()