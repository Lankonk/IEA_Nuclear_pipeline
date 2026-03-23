import os
import requests
import logging
import random
from typing import List, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_nuclear_outages(dataset_name: str,frequency: str = "daily", offset: int = 0, length: int = 1000):
    API_KEY = os.getenv("API_KEY")

    if not API_KEY:
        logging.error("Error: API_KEY is not set")
        return None

    base_url = f"https://api.eia.gov/v2/nuclear-outages/{dataset_name}/data"
    session = create_retry_session() #retry in case of network errors

    data = []
    offset = 0
    logging.info("Starting data extraction")

    while True:
        params = {
            "api_key": API_KEY,
            "frequency": frequency,
            "data[0]": "outage",
            "offset": offset,
            "length": length #length of each pagination
        }

        try:
            response = session.get(base_url, params=params, timeout=10)

            if response.status_code == 403:
                error_data = response.json()
                msg = error_data.get("error", {}).get("message", "Invalid API Key")
                logging.error(f"EIA AUTH ERROR: {msg}")
                return None

            response.raise_for_status()
            payload = response.json()
            
            # The EIA API nests the data inside a 'response' -> 'data' object
            data_chunk = payload.get("response", {}).get("data", [])

            required_fields = ["period", "outage"] #columns present in all

            if dataset_name != "us-nuclear-outages": #only us-nuclear-outages has no facility
                required_fields.append("facility")

            if not validate_data_with_sampling(data_chunk, required_fields, sample_rate=0.1):
                return None
            
            if not data_chunk:
                logging.info("No more data found. Ending pagination.")
                break
                
            data.extend(data_chunk)
            
            total_available_str = payload.get("response", {}).get("total", 0)
            total_available = int(total_available_str) if total_available_str else 0
            logging.info(f"Fetched {len(data_chunk)} rows. Total collected: {len(data)} / {total_available}")
            
            if len(data) >= total_available or len(data_chunk) < length:
                break #break if everything was extracted
                
            #go to next page
            offset += length

            if offset >= 5000: #max 5 pages
                logging.info("Safety brake engaged: Stopping at 5,000 records.")
                break

        except requests.exceptions.RequestException as req_err:
            logging.error(f"Network or API error occurred at offset {offset}: {req_err}")
            if hasattr(req_err, 'response') and req_err.response is not None:
                logging.error(f"EIA API ERROR DETAILS: {req_err.response.text}")
            break #return the data we have so far in case of error

    logging.info(f"Extraction complete. Total records retrieved: {len(data)}")
    return data

#retry if there are network errors, default is 1 retry
def create_retry_session(retries=1, backoff_factor=1.0) -> requests.Session:
    session = requests.Session()
    
    retry_strategy = Retry(
        total=retries,
        status_forcelist=[500, 502, 503, 504], # Only retry on network errors
        backoff_factor=backoff_factor # waits more time between retries
    )
    
    #Works for both http and https requests
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session

def validate_data_with_sampling(data_chunk: List[Dict[str, Any]], expected_keys: List[str], sample_rate: float = 0.2) -> bool:
    """
    Validates a subset of records using an iterator to ensure required fields exist.
    """
    if not isinstance(data_chunk, list) or not data_chunk:
        logging.warning("Validation Warning: No data provided for validation.")
        return True

    # Determine sample size
    total_records = len(data_chunk)
    k = max(1, int(total_records * sample_rate))
    
    # Randomly select data
    sampled_records = random.sample(data_chunk, k)
    
    logging.info(f"Validating sample of {k}/{total_records} records...")

    # Outer all() iterates through the sampled list.
    # Inner all() iterates through the expected keys for each record.
    is_valid = all(
        all(key in record for key in expected_keys) 
        for record in sampled_records
    ) #iterator

    if not is_valid:
        logging.error("Validation Failed: One or more sampled records are missing required fields.")
        return False

    logging.info(f"Validation Passed: Sample check successful.")
    return True