"""
Business Metrics and Financial KPIs for Fraud Detection System

Financial Concepts Implemented:
- ROI of fraud detection
- Cost of fraud vs. cost of false positives
- Risk-adjusted returns
- Financial impact analysis
"""

from pyspark.sql.functions import col, sum, count, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


def calculate_fraud_detection_metrics(fraud_alerts_df):
    """
    Calculate key business metrics for fraud detection system
    
    Financial Concept: Measuring the effectiveness and ROI of fraud prevention
    """
    # Calculate total fraud amount detected
    fraud_metrics = fraud_alerts_df.agg(
        count("transaction_id").alias("total_fraud_alerts"),
        sum("amount").alias("total_potential_fraud_amount"),
        avg("risk_score").alias("average_risk_score")
    ).withColumn("calculated_at", current_timestamp())
    
    return fraud_metrics


def calculate_roi_metrics(transactions_df, fraud_alerts_df, cost_per_investigation=25.0):
    """
    Calculate Return on Investment for fraud detection system
    
    Financial Concept: Cost-benefit analysis of fraud prevention investment
    """
    # Count total transactions
    total_transactions = transactions_df.count()
    
    # Count fraud alerts
    fraud_alerts_count = fraud_alerts_df.count()
    
    # Calculate costs
    investigation_cost = fraud_alerts_count * cost_per_investigation
    
    # Calculate potential fraud prevented (based on fraud alerts)
    potential_fraud_prevented = fraud_alerts_df.agg(sum("amount")).collect()[0][0] or 0.0
    
    # Calculate ROI metrics
    roi_metrics = {
        "total_transactions": total_transactions,
        "fraud_alerts_issued": fraud_alerts_count,
        "potential_fraud_prevented": potential_fraud_prevented,
        "total_investigation_cost": investigation_cost,
        "net_benefit": potential_fraud_prevented - investigation_cost,
        "roi_percentage": ((potential_fraud_prevented - investigation_cost) / investigation_cost) * 100 if investigation_cost > 0 else 0
    }
    
    return roi_metrics


def calculate_risk_adjusted_returns(fraud_alerts_df):
    """
    Calculate risk-adjusted returns for the fraud detection system
    
    Financial Concept: Risk-adjusted performance measurement
    """
    risk_metrics = fraud_alerts_df.groupBy("risk_level").agg(
        count("transaction_id").alias("alert_count"),
        sum("amount").alias("total_amount_at_risk"),
        avg("risk_score").alias("average_risk_score")
    )
    
    return risk_metrics


def calculate_fraud_rate_by_segment(fraud_alerts_df):
    """
    Calculate fraud rates by different business segments
    
    Financial Concept: Segmented risk analysis
    """
    fraud_by_segment = fraud_alerts_df.groupBy("account_type").agg(
        count("transaction_id").alias("fraud_alerts"),
        sum("amount").alias("total_fraud_amount"),
        avg("amount").alias("average_fraud_amount")
    )
    
    return fraud_by_segment


def generate_business_dashboard_data(transactions_df, fraud_alerts_df):
    """
    Generate comprehensive business metrics for fraud detection dashboard
    
    Financial Concept: Executive-level fraud analytics and KPIs
    """
    # Overall fraud metrics
    fraud_metrics = calculate_fraud_detection_metrics(fraud_alerts_df)
    
    # ROI metrics
    roi_metrics = calculate_roi_metrics(transactions_df, fraud_alerts_df)
    
    # Risk-adjusted returns
    risk_metrics = calculate_risk_adjusted_returns(fraud_alerts_df)
    
    # Segment analysis
    segment_metrics = calculate_fraud_rate_by_segment(fraud_alerts_df)
    
    return {
        "fraud_metrics": fraud_metrics,
        "roi_metrics": roi_metrics,
        "risk_metrics": risk_metrics,
        "segment_metrics": segment_metrics
    }