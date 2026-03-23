import logging
from extract_nuclear_data import get_nuclear_outages
from delta_pyspark import upsert_to_delta
from postgres import push_to_postgres

logging.basicConfig(level=logging.INFO)

def run_pipeline(dataset_name: str):
    logging.info("--- STARTING EIA ELT PIPELINE ---")
    
    # Extract from the API
    raw_data = get_nuclear_outages(dataset_name=dataset_name)
    
    if not raw_data:
        logging.error("Pipeline aborted: No data extracted.")
        return

    # Delta lakes
    df = upsert_to_delta(raw_data,dataset_name)
    
    # Insert into postgres
    if df:
        push_to_postgres(df, dataset_name)
        logging.info("--- PIPELINE SUCCESSFUL ---")
    else:
        logging.error("Silver layer failed to return DataFrame.")
    

if __name__ == "__main__":
    run_pipeline()