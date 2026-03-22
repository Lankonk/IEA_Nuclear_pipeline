import logging
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from config import DELTA_TABLE_PATH
from spark_Setup import get_spark_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def upsert_to_delta(raw_data: list, delta_path: str = "/tmp/delta/eia_nuclear_outages"):
    """
    Takes raw dictionary data, converts it to a Spark DataFrame, 
    and performs a MERGE (upsert) into a Delta table.
    """
    if not raw_data:
        logging.warning("No data provided to upsert.")
        return

    spark = get_spark_session()

    #Schema definition
    schema = StructType([
        StructField("period", StringType(), True),
        StructField("facility", StringType(), True),
        StructField("facilityName", StringType(), True),
        StructField("outage", StringType(), True),
        StructField("outage-units", StringType(), True),
        # We leave state and capacity here in case the API occasionally includes them
        StructField("state", StringType(), True),
        StructField("capacity", StringType(), True)
    ])

    logging.info("Converting raw data to Spark DataFrame...")
    
    #Pyspark dataframe creation
    updates_df = spark.createDataFrame(raw_data, schema=schema)

    updates_df = updates_df.withColumnRenamed("facilityName", "plant_name") \
                           .withColumn("capacity", col("capacity").cast("double")) \
                           .withColumn("outage", col("outage").cast("double")) \
                           .fillna("Unknown", subset=["state"]) \
                           .fillna(0.0, subset=["capacity"])

    #If table exists, merge to only update existing info
    if DeltaTable.isDeltaTable(spark, delta_path):
        logging.info(f"Existing Delta table found at {delta_path}. Performing MERGE...")
        
        # Load Delta table
        delta_table = DeltaTable.forPath(spark, delta_path)

        #Merge operation
        (delta_table.alias("target") #existing table
         .merge(
            updates_df.alias("updates"), #new data
            "target.plant_name = updates.plant_name AND target.period = updates.period"
         )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll() #insert new data
         .execute()
        )
        logging.info("MERGE complete.")

    else:
        # If table doesn't exist, create new table
        logging.info(f"No existing Delta table found. Creating new table at {delta_path}...")
        updates_df.write.format("delta").mode("overwrite").save(delta_path)
        logging.info("Initial Delta table creation complete.")

