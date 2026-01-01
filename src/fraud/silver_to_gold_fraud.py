from pyspark.sql.functions import col, avg, stddev, count, when, current_timestamp
from pyspark.sql.window import Window

from src.utils.spark_session import get_spark_session
from src.fraud.fraud_rules import apply_advanced_fraud_detection
from src.alerting.alert_system import apply_business_intelligence_enhancements, FinancialAlertSystem
from src.dashboard.dashboard_generator import create_business_intelligence_pipeline, calculate_financial_kpis


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
    # Read static user profile data (batch) for enrichment
    # --------------------------------------------------
    users_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/user_profiles.csv")
    )

    # --------------------------------------------------
    # Advanced fraud detection logic
    # --------------------------------------------------
    fraud_df = apply_advanced_fraud_detection(silver_df, users_df)
    
    # --------------------------------------------------
    # Apply business intelligence enhancements
    # --------------------------------------------------
    fraud_df = apply_business_intelligence_enhancements(fraud_df)
    
    # --------------------------------------------------
    # Initialize alert system
    # --------------------------------------------------
    alert_system = FinancialAlertSystem()

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

