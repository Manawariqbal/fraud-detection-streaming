from pyspark.sql import SparkSession
import os
def get_spark_session(app_name: str="FraudDetectionApp")->SparkSession:
    """
    -Creates and returns a SparkSession configured for:
    - Local execution
    - Kafka integration
    - Structured Streaming
    """
    spark=(
        SparkSession.Builder
        .appName(app_name)
        .master("local[*]")# Use all available cores
        # Kafka package (comes with Spark 3.x, safe to keep explicit)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        # Streaming & fault tolerance configs
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()

        )
    spark.sparkContext.setLogLevel("WARN")
    return spark