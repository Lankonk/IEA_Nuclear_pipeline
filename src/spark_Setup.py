import logging
from pyspark.sql import SparkSession

#Spark Session Setup for delta tables and postgres
def get_spark_session() -> SparkSession:
    logging.info("Booting up Spark Session...")
    
    builder = SparkSession.builder.appName("EIA_Nuclear_Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config(
            "spark.jars.packages", 
            "io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.6.0"
        )
    
    return builder.getOrCreate()