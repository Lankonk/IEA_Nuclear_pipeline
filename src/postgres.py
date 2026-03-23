import logging
from pyspark.sql.functions import col, to_date
from config import JDBC_URL, PROPERTIES
from postgres_connection import get_db_connection

def push_to_postgres(df, dataset_name):
    """
    Orchestrates an atomic, zero-downtime load into PostgreSQL 
    while respecting foreign key constraints and a 4-table limit.
    """
    logging.info(f"Starting atomic push for {dataset_name} to PostgreSQL...")

    # Updates the 'Parent' table first without deleting existing rows
    if dataset_name in ["facility-nuclear-outages", "generator-nuclear-outages"]:
        logging.info("Upserting dim_facilities...")
        dim_df = df.select(
            col("facility").alias("facility_id"),
            col("facilityName").alias("plant_name")
        ).dropDuplicates(["facility_id"]).na.drop(subset=["facility_id"])

        stg_dim = "stg_dim_facilities"
        dim_df.write.jdbc(url=JDBC_URL, table=stg_dim, mode="overwrite", properties=PROPERTIES)

        _execute_transaction(f"""
            INSERT INTO dim_facilities (facility_id, plant_name)
            SELECT facility_id, plant_name FROM {stg_dim}
            ON CONFLICT (facility_id) DO UPDATE SET plant_name = EXCLUDED.plant_name;
            DROP TABLE {stg_dim};
        """)


    # Table identification
    if dataset_name == "facility-nuclear-outages":
        table_name = "fact_facility_outages"
        fact_df = df.select(
            to_date(col("period"), "yyyy-MM-dd").alias("period"),
            col("facility").alias("facility_id"),
            col("outage").cast("double").dropDuplicates(["period", "facility_id"])
        )
    elif dataset_name == "generator-nuclear-outages":
        table_name = "fact_generator_outages"
        fact_df = df.select(
            to_date(col("period"), "yyyy-MM-dd").alias("period"),
            col("facility").alias("facility_id"),
            col("generator"),
            col("outage").cast("double").dropDuplicates(["period", "facility_id"])
        )
    elif dataset_name == "us-nuclear-outages":
        table_name = "fact_us_outages"
        fact_df = df.select(
            to_date(col("period"), "yyyy-MM-dd").alias("period"),
            col("outage").cast("double")
        )
    else:
        logging.info("Error when pushing")
        return

    stg_table = f"stg_{table_name}"
    # stg_table with date and double types
    fact_df.write.jdbc(url=JDBC_URL, table=stg_table, mode="overwrite", properties=PROPERTIES)

    _execute_transaction(f"""
        TRUNCATE TABLE {table_name} CASCADE;
        INSERT INTO {table_name} SELECT * FROM {stg_table};
        DROP TABLE {stg_table};
    """)
    
    logging.info(f"Pipeline update complete for {dataset_name}!")

def _execute_transaction(sql_query):
    """Helper to ensure SQL operations are atomic and safe."""
    conn = get_db_connection()
    conn.autocommit = False  # Start manual transaction
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Database Transaction Failed: {e}")
        raise  # Re-raise so the pipeline knows it failed
    finally:
        cursor.close()
        conn.close()