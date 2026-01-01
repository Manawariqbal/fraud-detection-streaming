from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)
from src.utils.spark_session import get_spark_session
from src.processing.silver_enrichment import enrich_transaction_data


# ----------------------------
# Paths & Config
# ----------------------------
BRONZE_PATH = "data/bronze/transactions"
SILVER_PATH = "data/silver/transactions"
CHECKPOINT_PATH = "data/silver/checkpoints"
USER_PROFILE_PATH = "data/user_profiles.csv"


# ----------------------------
# Bronze Schema
# ----------------------------
bronze_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])


# ----------------------------
# Main Job
# ----------------------------
def main():
    spark = get_spark_session("BronzeToSilver")

    # Read static user profile data (batch)
    users_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(USER_PROFILE_PATH)
    )

    # Read Bronze transactions as stream
    bronze_df = (
        spark.readStream
        .schema(bronze_schema)
        .parquet(BRONZE_PATH)
    )

    # Enrich transactions with enhanced financial indicators
    silver_df = enrich_transaction_data(bronze_df, users_df)

    # Write to Silver layer
    query = (
        silver_df.writeStream
        .format("parquet")   # can be Delta later
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(SILVER_PATH)
    )

    print("Bronze → Silver enrichment started...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
