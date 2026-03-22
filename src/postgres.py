import logging
from spark_Setup import get_spark_session
from config import DELTA_TABLE_PATH, JDBC_URL, DB_USER, DB_PASS

#recieves delta table path as argument
def push_to_postgres(delta_path: str = DELTA_TABLE_PATH):

    spark = get_spark_session()
    
    db_url = JDBC_URL
    db_properties = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified" 
    }

    logging.info(f"Reading Gold-tier data from Delta table at {delta_path}...")
    try:
        # Load the data from Delta
        df = spark.read.format("delta").load(delta_path)
    except Exception as e:
        logging.error(f"Failed to read Delta table: {e}. Did you run the extraction script first?")
        return

    logging.info("Extracting unique plants for the dimension table...")
    plants_df = df.select("plant_name", "state").distinct()
    
    #Ignore protects against overwriting the table structure
    logging.info("Writing to Postgres table: plants...")
    plants_df.write.jdbc(url=db_url, 
                         table="plants", 
                         mode="ignore", 
                         properties=db_properties)

    logging.info("Preparing fact table data...")
    outages_df = df.select("period", "plant_name", "capacity", "outage")
    
    #Since Delta already handled not inserting duplicates, we can safely overwrite the table
    logging.info("Writing to Postgres table: annual_outages...")
    outages_df.write.jdbc(url=db_url, 
                          table="annual_outages", 
                          mode="overwrite", 
                          properties=db_properties)
    
    logging.info("Data successfully loaded into PostgreSQL serving layer!")