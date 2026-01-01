from pyspark.sql.functions import col, avg, stddev, count, sum, max, min, when, current_timestamp
from pyspark.sql.window import Window


def calculate_velocity_based_fraud(df):
    """
    Detect fraud based on transaction velocity (frequency-based detection)
    
    Financial Concept: Unusual transaction frequency can indicate fraudulent activity
    """
    # Window for counting transactions in last 10 minutes
    velocity_window = Window.partitionBy("user_id").orderBy("timestamp").rangeBetween(-600, 0)
    
    df = df.withColumn("transaction_count_10min", count("transaction_id").over(velocity_window))
    
    # Flag if more than 5 transactions in 10 minutes
    df = df.withColumn("is_velocity_anomaly", col("transaction_count_10min") > 5)
    
    return df

def calculate_amount_anomaly(df):
    """
    Detect fraud based on amount anomalies compared to user's historical behavior
    
    Financial Concept: Sudden large transactions compared to historical patterns
    """
    window_spec = Window.partitionBy("user_id")
    
    df = df.withColumn("avg_transaction_amount", avg("amount").over(window_spec))
    df = df.withColumn("stddev_transaction_amount", stddev("amount").over(window_spec))
    
    # Calculate z-score for amount anomaly
    df = df.withColumn(
        "z_score", 
        (col("amount") - col("avg_transaction_amount")) / col("stddev_transaction_amount")
    )
    
    # Mark as anomaly if z-score > 3 (3 standard deviations)
    df = df.withColumn("is_amount_anomaly", col("z_score") > 3.0)
    
    return df

def calculate_location_anomaly(df, users_df):
    """
    Detect fraud based on unusual location patterns
    
    Financial Concept: Transactions from unusual geographic locations
    """
    # Join with user profiles to get home country
    df_joined = df.join(users_df.select("user_id", "country"), on="user_id", how="left")
    
    # Flag if transaction location differs from user's home country
    df_joined = df_joined.withColumn(
        "is_location_anomaly", 
        when(col("location") != col("country"), True).otherwise(False)
    )
    
    return df_joined

def calculate_velocity_amount_combination(df):
    """
    Detect fraud based on combination of velocity and amount anomalies
    
    Financial Concept: Multiple suspicious patterns together increase fraud probability
    """
    df = df.withColumn(
        "is_combined_fraud", 
        col("is_velocity_anomaly") | col("is_amount_anomaly")
    )
    
    return df

def calculate_risk_score(df):
    """
    Calculate overall risk score based on multiple fraud indicators
    
    Financial Concept: Weighted risk scoring for decision making
    """
    df = df.withColumn(
        "risk_score",
        when(col("is_velocity_anomaly"), 30)  # High velocity = 30 points
        .when(col("is_amount_anomaly"), 40)   # Amount anomaly = 40 points
        .when(col("is_location_anomaly"), 25) # Location anomaly = 25 points
        .otherwise(0)
    ).withColumn(
        "risk_level",
        when(col("risk_score") >= 60, "HIGH")
        .when(col("risk_score") >= 30, "MEDIUM")
        .otherwise("LOW")
    )
    
    return df

def calculate_business_metrics(df):
    """
    Calculate business metrics for fraud detection performance
    
    Financial Concept: Measuring fraud detection effectiveness
    """
    # Add timestamp for metrics tracking
    df = df.withColumn("processed_at", current_timestamp())
    
    # Calculate fraud rate by account type
    fraud_by_account_type = df.filter(col("is_combined_fraud") == True) \
        .groupBy("account_type") \
        .agg(
            count("transaction_id").alias("fraud_count"),
            sum("amount").alias("fraud_amount")
        )
    
    return df, fraud_by_account_type

def apply_advanced_fraud_detection(df, users_df):
    """
    Apply comprehensive fraud detection with multiple algorithms
    """
    # Apply individual fraud detection methods
    df = calculate_velocity_based_fraud(df)
    df = calculate_amount_anomaly(df)
    df = calculate_location_anomaly(df, users_df)
    df = calculate_velocity_amount_combination(df)
    df = calculate_risk_score(df)
    
    # Filter for suspicious transactions
    suspicious_transactions = df.filter(
        (col("is_combined_fraud") == True) | 
        (col("risk_level") != "LOW")
    )
    
    # Select relevant columns for output
    fraud_alerts = suspicious_transactions.select(
        "transaction_id",
        "user_id", 
        "amount",
        "location",
        "timestamp",
        "is_velocity_anomaly",
        "is_amount_anomaly", 
        "is_location_anomaly",
        "is_combined_fraud",
        "risk_score",
        "risk_level",
        "avg_transaction_amount",
        "z_score"
    )
    
    return fraud_alerts
