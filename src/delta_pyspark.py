import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
from config import DELTA_TABLE_PATH
from spark_Setup import get_spark_session

def upsert_to_delta(raw_data, dataset_name):
    """
    Converts raw JSON data to a Spark DataFrame, saves it to Delta Lake,
    and passes it to the PostgreSQL loader for relational modeling.
    """
    spark = get_spark_session()

    # Missing fields in the JSON,which depends on the database, will become null.
    schema = StructType([
        StructField("period", StringType(), True),
        StructField("facility", StringType(), True),
        StructField("facilityName", StringType(), True),
        StructField("generator", StringType(), True),
        StructField("outage", StringType(), True),
        StructField("outage-units", StringType(), True)
    ])

    logging.info(f"Converting {dataset_name} to Spark DataFrame...")
    df = spark.createDataFrame(raw_data, schema=schema)

    # String to double conversion
    df = df.withColumn("outage", col("outage").cast("double"))

    # New folder for each dataset
    delta_path = f"{DELTA_TABLE_PATH}/{dataset_name}"
    logging.info(f"Saving to Delta Lake at {delta_path}...")
    
    df.write.format("delta").mode("overwrite").save(delta_path)

    return df

