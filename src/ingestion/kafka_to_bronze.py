from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)
from src.utils.spark_session import get_spark_session


# ----------------------------
# Configuration
# ----------------------------
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "transactions"

BRONZE_PATH = "data/bronze/transactions"
CHECKPOINT_PATH = "data/bronze/checkpoints"


# ----------------------------
# Transaction Schema
# ----------------------------
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])


# ----------------------------
# Main Streaming Job
# ----------------------------
def main():
    spark = get_spark_session("KafkaToBronze")

    # Read from Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse Kafka value (JSON)
    parsed_df = (
        kafka_df
        .select(from_json(col("value").cast("string"), transaction_schema).alias("data"))
        .select("data.*")
    )

    # Write to Bronze layer
    query = (
        parsed_df.writeStream
        .format("parquet")  # later we can switch to Delta
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(BRONZE_PATH)
    )

    print("Kafka → Bronze streaming started...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
