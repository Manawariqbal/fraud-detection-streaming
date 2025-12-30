from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "FraudDetectionApp") -> SparkSession:
    """
    Create and return a SparkSession configured for local execution
    and Kafka Structured Streaming.
    """

    builder = SparkSession.builder

    spark = (
        builder
        .master("local[*]")
        .appName(app_name)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0"
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

