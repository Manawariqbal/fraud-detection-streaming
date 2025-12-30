from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window

from src.utils.spark_session import get_spark_session


# Paths
SILVER_PATH = "data/silver/transactions"
GOLD_PATH = "data/gold/fraud_alerts"
CHECKPOINT_PATH = "data/gold/checkpoints"


def main():
    spark = get_spark_session("SilverToGoldFraud")

    # --------------------------------------------------
    # IMPORTANT:
    # Streaming file sources REQUIRE an explicit schema
    # --------------------------------------------------

    # Read schema from existing Silver data (batch read)
    silver_schema = spark.read.parquet(SILVER_PATH).schema

    # Read Silver layer as STREAM using predefined schema
    silver_df = (
        spark.readStream
        .schema(silver_schema)
        .parquet(SILVER_PATH)
    )

    # --------------------------------------------------
    # Fraud detection logic
    # --------------------------------------------------

    window_spec = Window.partitionBy("user_id")

    fraud_df = (
        silver_df
        .withColumn(
            "avg_transaction_amount",
            avg("amount").over(window_spec)
        )
        .withColumn(
            "is_high_value",
            col("amount") > 50000
        )
        .withColumn(
            "is_amount_anomaly",
            col("amount") > col("avg_transaction_amount") * 3
        )
        .withColumn(
            "is_fraud",
            col("is_high_value") | col("is_amount_anomaly")
        )
        .filter(col("is_fraud") == True)
    )

    # --------------------------------------------------
    # Write to Gold layer (STREAMING)
    # --------------------------------------------------

    query = (
        fraud_df
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", GOLD_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    print("Silver → Gold fraud detection started...")
    query.awaitTermination()


if __name__ == "__main__":
    main()

