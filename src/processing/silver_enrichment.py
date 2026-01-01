from pyspark.sql.functions import col, to_timestamp, when, lit
from pyspark.sql.types import TimestampType


def enrich_transaction_data(transactions_df, users_df):
    """
    Enhanced data enrichment with financial indicators
    
    Financial Concept: Enriching transactions with user risk profiles
    """
    # Join transactions with user profiles
    enriched_df = transactions_df.join(users_df, on="user_id", how="left")
    
    # Add financial risk indicators
    enriched_df = enriched_df.withColumn(
        "is_high_value_user", 
        when(col("avg_transaction_amount") > 10000, True).otherwise(False)
    )
    
    # Add account type indicators
    enriched_df = enriched_df.withColumn(
        "account_risk_multiplier",
        when(col("account_type") == "PREMIUM", 1.2)
        .when(col("account_type") == "GOLD", 1.5)
        .otherwise(1.0)
    )
    
    # Convert timestamp string to proper timestamp type
    enriched_df = enriched_df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    )
    
    return enriched_df
