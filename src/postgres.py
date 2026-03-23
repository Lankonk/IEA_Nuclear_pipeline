import logging
from pyspark.sql.functions import col
from config import JDBC_URL, PROPERTIES

def push_to_postgres(df, dataset_name):

    logging.info(f"Routing data for {dataset_name} to PostgreSQL serving layer...")

    # (Runs for facility and generator datasets only)
    if dataset_name in ["facility-nuclear-outages", "generator-nuclear-outages"]:
        logging.info("Updating Dimension Table: dim_facilities...")
        
        # Select facility ID and Name, rename them, and drop duplicates
        dim_facilities_df = df.select(
            col("facility").alias("facility_id"),
            col("facilityName").alias("plant_name")
        ).dropDuplicates(["facility_id"]).na.drop(subset=["facility_id"])
        
        # Write to Postgres using 'overwrite' to ensure the schema stays fresh
        dim_facilities_df.write.jdbc(
            url=JDBC_URL, 
            table="dim_facilities", 
            mode="overwrite", 
            properties=PROPERTIES
        )
    
    # Facility Level Outages
    if dataset_name == "facility-nuclear-outages":
        table_name = "fact_facility_outages"
        fact_df = df.select(
            col("period"), 
            col("facility").alias("facility_id"), 
            col("outage")
        )

    #  Generator Level Outages (With 'generator' column)
    elif dataset_name == "generator-nuclear-outages":
        table_name = "fact_generator_outages"
        fact_df = df.select(
            col("period"), 
            col("facility").alias("facility_id"), 
            col("generator"), 
            col("outage")
        )

    # CASE C: U.S. National Outages (Has no facility ID)
    elif dataset_name == "us-nuclear-outages":
        table_name = "fact_us_outages"
        fact_df = df.select(
            col("period"), 
            col("outage")
        )
    
    else:
        logging.error(f"Unknown dataset name received: {dataset_name}")
        return

    # Write the specific fact table to Postgres
    logging.info(f"Writing Fact Table: {table_name}...")
    fact_df.write.jdbc(
        url=JDBC_URL, 
        table=table_name, 
        mode="overwrite", 
        properties=PROPERTIES
    )
    
    logging.info(f"Successfully loaded {dataset_name} into PostgreSQL!")