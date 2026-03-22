import os
import logging
from pyspark.sql import SparkSession

#Global Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#Environment Variables injected via Docker Compose
API_KEY = os.getenv("API_KEY")
EIA_BASE_URL = "https://api.eia.gov/v2/nuclear-outages"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_NAME = os.getenv("DB_NAME", "eia_data")

# JDBC Connection String
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
DELTA_TABLE_PATH = "/tmp/delta/eia_nuclear_outages"