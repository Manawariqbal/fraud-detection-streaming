"""
Executive Dashboard Generator for Financial Fraud Detection System

Financial Concepts Implemented:
- Executive KPIs
- Risk-adjusted performance metrics
- Business impact visualization
- ROI tracking
"""

from pyspark.sql.functions import col, sum, count, avg, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import json
from datetime import datetime


class ExecutiveDashboard:
    """
    Generate executive-level dashboard for fraud detection system
    """
    
    def __init__(self):
        self.dashboard_data = {}
    
    def generate_overall_health_metrics(self, fraud_alerts_df):
        """
        Generate overall health metrics for the fraud detection system
        """
        health_metrics = fraud_alerts_df.agg(
            count("*").alias("total_alerts"),
            sum("amount").alias("total_amount_monitored"),
            avg("risk_score").alias("average_risk_score"),
            sum(when(col("risk_level") == "HIGH", 1).otherwise(0)).alias("high_risk_cases"),
            sum(when(col("risk_level") == "MEDIUM", 1).otherwise(0)).alias("medium_risk_cases"),
            sum(when(col("risk_level") == "LOW", 1).otherwise(0)).alias("low_risk_cases")
        )
        
        # Collect metrics
        metrics = health_metrics.collect()[0]
        
        return {
            "total_alerts": metrics['total_alerts'],
            "total_amount_monitored": float(metrics['total_amount_monitored']) if metrics['total_amount_monitored'] else 0.0,
            "average_risk_score": float(metrics['average_risk_score']) if metrics['average_risk_score'] else 0.0,
            "high_risk_cases": metrics['high_risk_cases'],
            "medium_risk_cases": metrics['medium_risk_cases'],
            "low_risk_cases": metrics['low_risk_cases'],
            "timestamp": str(current_timestamp())
        }
    
    def generate_risk_distribution_metrics(self, fraud_alerts_df):
        """
        Generate risk distribution across different dimensions
        """
        risk_by_type = fraud_alerts_df.groupBy("risk_level").agg(
            count("*").alias("alert_count"),
            sum("amount").alias("total_amount"),
            avg("risk_score").alias("avg_risk_score")
        ).collect()
        
        risk_distribution = {}
        for row in risk_by_type:
            risk_distribution[row['risk_level']] = {
                "alert_count": row['alert_count'],
                "total_amount": float(row['total_amount']),
                "avg_risk_score": float(row['avg_risk_score'])
            }
        
        return risk_distribution
    
    def generate_account_type_analysis(self, fraud_alerts_df):
        """
        Analyze fraud patterns by account type (business intelligence)
        """
        account_analysis = fraud_alerts_df.groupBy("account_type").agg(
            count("*").alias("fraud_alerts"),
            sum("amount").alias("total_fraud_amount"),
            avg("risk_score").alias("avg_risk_score"),
            avg("amount").alias("avg_fraud_amount")
        ).collect()
        
        analysis = {}
        for row in account_analysis:
            analysis[row['account_type']] = {
                "fraud_alerts": row['fraud_alerts'],
                "total_fraud_amount": float(row['total_fraud_amount']),
                "avg_risk_score": float(row['avg_risk_score']),
                "avg_fraud_amount": float(row['avg_fraud_amount'])
            }
        
        return analysis
    
    def generate_location_based_risk(self, fraud_alerts_df):
        """
        Analyze fraud risk by location (geographic intelligence)
        """
        location_risk = fraud_alerts_df.groupBy("location").agg(
            count("*").alias("fraud_alerts"),
            sum("amount").alias("total_amount"),
            avg("risk_score").alias("avg_risk_score")
        ).collect()
        
        location_analysis = {}
        for row in location_risk:
            location_analysis[row['location']] = {
                "fraud_alerts": row['fraud_alerts'],
                "total_amount": float(row['total_amount']),
                "avg_risk_score": float(row['avg_risk_score'])
            }
        
        return location_analysis
    
    def generate_roi_metrics(self, fraud_alerts_df, operational_cost_per_alert=25.0):
        """
        Calculate Return on Investment metrics for fraud detection
        
        Financial Concept: Measuring financial effectiveness of fraud prevention
        """
        # Calculate total potential fraud prevented
        total_potential_fraud = fraud_alerts_df.agg(sum("amount")).collect()[0][0] or 0.0
        
        # Calculate total alerts generated
        total_alerts = fraud_alerts_df.count()
        
        # Calculate operational costs
        total_operational_cost = total_alerts * operational_cost_per_alert
        
        # Calculate net benefit
        net_benefit = total_potential_fraud - total_operational_cost
        
        # Calculate ROI percentage
        roi_percentage = (net_benefit / total_operational_cost * 100) if total_operational_cost > 0 else 0
        
        return {
            "total_potential_fraud_prevented": float(total_potential_fraud),
            "total_alerts_generated": total_alerts,
            "total_operational_cost": float(total_operational_cost),
            "net_benefit": float(net_benefit),
            "roi_percentage": float(roi_percentage),
            "cost_per_fraud_prevented": float(total_operational_cost / total_alerts if total_alerts > 0 else 0)
        }
    
    def generate_time_series_metrics(self, fraud_alerts_df):
        """
        Generate time-series metrics for trend analysis
        """
        # This would typically group by time windows, but we'll simulate this
        # by creating basic time-based aggregations
        time_metrics = fraud_alerts_df.agg(
            count("*").alias("total_alerts"),
            sum("amount").alias("total_amount"),
            avg("risk_score").alias("avg_risk_score")
        ).collect()[0]
        
        return {
            "total_alerts": time_metrics['total_alerts'],
            "total_amount": float(time_metrics['total_amount']),
            "avg_risk_score": float(time_metrics['avg_risk_score']),
            "analysis_period": "Real-time"
        }
    
    def generate_complete_dashboard(self, fraud_alerts_df):
        """
        Generate complete executive dashboard with all metrics
        """
        dashboard = {
            "dashboard_id": f"FD_DASHBOARD_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "generated_at": str(current_timestamp()),
            "overall_health": self.generate_overall_health_metrics(fraud_alerts_df),
            "risk_distribution": self.generate_risk_distribution_metrics(fraud_alerts_df),
            "account_type_analysis": self.generate_account_type_analysis(fraud_alerts_df),
            "location_based_risk": self.generate_location_based_risk(fraud_alerts_df),
            "roi_metrics": self.generate_roi_metrics(fraud_alerts_df),
            "time_series_metrics": self.generate_time_series_metrics(fraud_alerts_df)
        }
        
        self.dashboard_data = dashboard
        return dashboard
    
    def export_dashboard_json(self, dashboard_data, filename=None):
        """
        Export dashboard data as JSON for visualization tools
        """
        if not filename:
            filename = f"fraud_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w') as f:
            json.dump(dashboard_data, f, indent=2, default=str)
        
        return filename


def create_business_intelligence_pipeline(fraud_alerts_df):
    """
    Create end-to-end business intelligence pipeline
    
    Financial Concept: Comprehensive business analytics for fraud management
    """
    # Initialize dashboard generator
    dashboard_gen = ExecutiveDashboard()
    
    # Generate complete dashboard
    dashboard_data = dashboard_gen.generate_complete_dashboard(fraud_alerts_df)
    
    # Export dashboard data
    export_file = dashboard_gen.export_dashboard_json(dashboard_data)
    
    # Print summary metrics
    overall_health = dashboard_data['overall_health']
    roi_metrics = dashboard_data['roi_metrics']
    
    print("="*60)
    print("📈 FRAUD DETECTION EXECUTIVE DASHBOARD")
    print("="*60)
    print(f"Total Alerts Generated: {overall_health['total_alerts']}")
    print(f"Total Amount Monitored: ${overall_health['total_amount_monitored']:,.2f}")
    print(f"Average Risk Score: {overall_health['average_risk_score']:.2f}")
    print(f"High Risk Cases: {overall_health['high_risk_cases']}")
    print(f"Potential Fraud Prevented: ${roi_metrics['total_potential_fraud_prevented']:,.2f}")
    print(f"Operational Cost: ${roi_metrics['total_operational_cost']:,.2f}")
    print(f"Net Benefit: ${roi_metrics['net_benefit']:,.2f}")
    print(f"ROI Percentage: {roi_metrics['roi_percentage']:.2f}%")
    print(f"Exported to: {export_file}")
    print("="*60)
    
    return dashboard_data


def calculate_financial_kpis(fraud_alerts_df):
    """
    Calculate key financial KPIs for fraud detection system
    
    Financial Concept: Financial performance indicators for risk management
    """
    # Calculate financial KPIs
    kpis = fraud_alerts_df.agg(
        # Loss Prevention KPIs
        sum("amount").alias("potential_loss_prevented"),
        
        # Efficiency KPIs
        count("*").alias("alerts_generated"),
        avg("risk_score").alias("average_detection_accuracy"),
        
        # Risk Concentration KPIs
        sum(when(col("risk_level") == "HIGH", col("amount")).otherwise(0)).alias("high_risk_exposure"),
        sum(when(col("risk_level") == "MEDIUM", col("amount")).otherwise(0)).alias("medium_risk_exposure"),
        
        # Business Impact KPIs
        sum(when(col("is_velocity_anomaly") == True, 1).otherwise(0)).alias("velocity_anomalies"),
        sum(when(col("is_amount_anomaly") == True, 1).otherwise(0)).alias("amount_anomalies"),
        sum(when(col("is_location_anomaly") == True, 1).otherwise(0)).alias("location_anomalies")
    )
    
    kpi_values = kpis.collect()[0]
    
    financial_kpis = {
        "potential_loss_prevented": float(kpi_values['potential_loss_prevented']) if kpi_values['potential_loss_prevented'] else 0.0,
        "alerts_generated": kpi_values['alerts_generated'],
        "average_detection_accuracy": float(kpi_values['average_detection_accuracy']) if kpi_values['average_detection_accuracy'] else 0.0,
        "high_risk_exposure": float(kpi_values['high_risk_exposure']) if kpi_values['high_risk_exposure'] else 0.0,
        "medium_risk_exposure": float(kpi_values['medium_risk_exposure']) if kpi_values['medium_risk_exposure'] else 0.0,
        "velocity_anomalies": kpi_values['velocity_anomalies'],
        "amount_anomalies": kpi_values['amount_anomalies'],
        "location_anomalies": kpi_values['location_anomalies'],
        "loss_prevention_rate": float(kpi_values['potential_loss_prevented']) / (kpi_values['alerts_generated'] or 1) if kpi_values else 0.0
    }
    
    return financial_kpis