from pyspark.sql.functions import col, when, lit
from src.utils.spark_session import get_spark_session


# ----------------------------
# Paths & Config
# ----------------------------
SILVER_PATH = "data/silver/transactions"
GOLD_PATH = "data/gold/fraud_alerts"
CHECKPOINT_PATH = "data/gold/checkpoints"

HIGH_VALUE_THRESHOLD = 50000


# ----------------------------
# Main Fraud Detection Job
# ----------------------------
def main():
    spark = get_spark_session("SilverToGoldFraudDetection")

    # Read Silver data as stream
    silver_df = (
        spark.readStream
        .parquet(SILVER_PATH)
    )

    # ----------------------------
    # Fraud Rules
    # ----------------------------

    fraud_df = (
        silver_df
        .withColumn(
            "is_amount_anomaly",
            col("amount") > (col("avg_transaction_amount") * 3)
        )
        .withColumn(
            "is_high_value",
            col("amount") > lit(HIGH_VALUE_THRESHOLD)
        )
        .withColumn(
            "is_location_mismatch",
            col("location") != col("country")
        )
        .withColumn(
            "is_fraud",
            when(
                col("is_amount_anomaly") |
                col("is_high_value") |
                col("is_location_mismatch"),
                lit(True)
            ).otherwise(lit(False))
        )
    )

    # Filter only fraud cases
    fraud_alerts_df = fraud_df.filter(col("is_fraud") == True)

    # Write to Gold layer
    query = (
        fraud_alerts_df.writeStream
        .format("parquet")   # can switch to Delta later
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(GOLD_PATH)
    )

    print("Silver → Gold fraud detection started...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
