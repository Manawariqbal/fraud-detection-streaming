"""
Batch Analytics for Fraud Detection System

Financial Concepts Implemented:
- Periodic business intelligence reporting
- Executive dashboard generation
- Financial KPI calculation
- ROI analysis
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, current_timestamp
from datetime import datetime
import json

from src.analytics.business_metrics import generate_business_dashboard_data
from src.dashboard.dashboard_generator import create_business_intelligence_pipeline, calculate_financial_kpis
from src.alerting.alert_system import FinancialAlertSystem


def run_batch_analytics():
    """
    Run comprehensive batch analytics on fraud detection data
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("FraudDetectionBatchAnalytics") \
        .config("spark.sql.streaming.checkpointLocation", "data/checkpoints/analytics") \
        .getOrCreate()
    
    try:
        # Read the latest fraud alerts data (Gold layer)
        fraud_alerts_df = spark.read.parquet("data/gold/fraud_alerts")
        
        # Calculate financial KPIs
        financial_kpis = calculate_financial_kpis(fraud_alerts_df)
        
        # Generate business intelligence dashboard
        dashboard_data = create_business_intelligence_pipeline(fraud_alerts_df)
        
        # Print summary
        print("\n" + "="*60)
        print("📊 FRAUD DETECTION BATCH ANALYTICS REPORT")
        print("="*60)
        print(f"Potential Loss Prevented: ${financial_kpis['potential_loss_prevented']:,.2f}")
        print(f"Total Alerts Generated: {financial_kpis['alerts_generated']}")
        print(f"Average Detection Accuracy: {financial_kpis['average_detection_accuracy']:.2f}")
        print(f"High Risk Exposure: ${financial_kpis['high_risk_exposure']:,.2f}")
        print(f"Medium Risk Exposure: ${financial_kpis['medium_risk_exposure']:,.2f}")
        print(f"Velocity Anomalies Detected: {financial_kpis['velocity_anomalies']}")
        print(f"Amount Anomalies Detected: {financial_kpis['amount_anomalies']}")
        print(f"Location Anomalies Detected: {financial_kpis['location_anomalies']}")
        print("="*60)
        
        # Save analytics results
        save_analytics_results(financial_kpis, dashboard_data)
        
    except Exception as e:
        print(f"Error in batch analytics: {str(e)}")
        raise
    finally:
        spark.stop()


def save_analytics_results(financial_kpis, dashboard_data):
    """
    Save analytics results to storage for reporting
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save financial KPIs
    kpi_file = f"data/analytics/kpi_report_{timestamp}.json"
    with open(kpi_file, 'w') as f:
        json.dump(financial_kpis, f, indent=2, default=str)
    
    # Save dashboard data
    dashboard_file = f"data/analytics/dashboard_report_{timestamp}.json"
    with open(dashboard_file, 'w') as f:
        json.dump(dashboard_data, f, indent=2, default=str)
    
    print(f"Analytics reports saved:")
    print(f"  - KPI Report: {kpi_file}")
    print(f"  - Dashboard Report: {dashboard_file}")


def generate_executive_summary():
    """
    Generate executive summary of fraud detection performance
    """
    spark = SparkSession.builder \
        .appName("FraudDetectionExecutiveSummary") \
        .getOrCreate()
    
    try:
        # Read fraud alerts
        fraud_alerts_df = spark.read.parquet("data/gold/fraud_alerts")
        
        # Calculate summary metrics
        summary = fraud_alerts_df.agg(
            count("*").alias("total_fraud_alerts"),
            sum("amount").alias("total_at_risk_amount"),
            avg("risk_score").alias("average_risk_score"),
            sum(col("business_impact_score")).alias("total_business_impact")
        ).collect()[0]
        
        # Generate executive summary
        exec_summary = {
            "executive_summary": {
                "report_date": str(current_timestamp()),
                "total_fraud_alerts": summary['total_fraud_alerts'],
                "total_at_risk_amount": float(summary['total_at_risk_amount']) if summary['total_at_risk_amount'] else 0.0,
                "average_risk_score": float(summary['average_risk_score']) if summary['average_risk_score'] else 0.0,
                "total_business_impact": float(summary['total_business_impact']) if summary['total_business_impact'] else 0.0,
                "financial_impact_assessment": "HIGH" if summary['total_at_risk_amount'] and float(summary['total_at_risk_amount']) > 100000 else "MODERATE"
            }
        }
        
        # Save executive summary
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        summary_file = f"data/analytics/executive_summary_{timestamp}.json"
        with open(summary_file, 'w') as f:
            json.dump(exec_summary, f, indent=2, default=str)
        
        print(f"\n📈 Executive Summary:")
        print(f"Total Fraud Alerts: {exec_summary['executive_summary']['total_fraud_alerts']}")
        print(f"Total Amount at Risk: ${exec_summary['executive_summary']['total_at_risk_amount']:,.2f}")
        print(f"Average Risk Score: {exec_summary['executive_summary']['average_risk_score']:.2f}")
        print(f"Business Impact: {exec_summary['executive_summary']['financial_impact_assessment']}")
        print(f"Report saved to: {summary_file}")
        
    except Exception as e:
        print(f"Error generating executive summary: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    print("Starting batch analytics for fraud detection system...")
    run_batch_analytics()
    generate_executive_summary()
    print("Batch analytics completed successfully!")