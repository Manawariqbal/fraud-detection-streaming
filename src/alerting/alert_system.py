"""
Advanced Alerting System for Financial Fraud Detection

Financial Concepts Implemented:
- Real-time risk scoring
- Tiered alerting based on risk levels
- Business impact assessment
- Automated response triggers
"""

from pyspark.sql.functions import col, when, current_timestamp
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class FinancialAlertSystem:
    """
    Financial-grade alerting system with business intelligence capabilities
    """
    
    def __init__(self, alert_thresholds=None):
        """
        Initialize alert system with financial risk thresholds
        
        Args:
            alert_thresholds (dict): Risk thresholds for different alert levels
        """
        self.alert_thresholds = alert_thresholds or {
            'HIGH': 60,      # Risk score > 60 triggers high alert
            'MEDIUM': 30,    # Risk score 30-59 triggers medium alert
            'LOW': 10        # Risk score 10-29 triggers low alert
        }
        
    def categorize_alert(self, risk_score):
        """
        Categorize alert based on risk score using financial risk categories
        """
        if risk_score >= self.alert_thresholds['HIGH']:
            return 'HIGH'
        elif risk_score >= self.alert_thresholds['MEDIUM']:
            return 'MEDIUM'
        elif risk_score >= self.alert_thresholds['LOW']:
            return 'LOW'
        else:
            return 'INFO'
    
    def generate_alert_message(self, transaction_data):
        """
        Generate comprehensive alert message with financial impact assessment
        """
        alert_msg = {
            'alert_id': f"ALERT_{transaction_data['transaction_id']}",
            'timestamp': str(current_timestamp()),
            'transaction_id': transaction_data['transaction_id'],
            'user_id': transaction_data['user_id'],
            'amount': transaction_data['amount'],
            'risk_score': transaction_data['risk_score'],
            'risk_level': transaction_data['risk_level'],
            'fraud_indicators': {
                'velocity_anomaly': transaction_data.get('is_velocity_anomaly', False),
                'amount_anomaly': transaction_data.get('is_amount_anomaly', False),
                'location_anomaly': transaction_data.get('is_location_anomaly', False)
            },
            'financial_impact': self.assess_financial_impact(transaction_data),
            'recommended_action': self.get_recommended_action(transaction_data)
        }
        
        return alert_msg
    
    def assess_financial_impact(self, transaction_data):
        """
        Assess the financial impact of the potential fraud
        
        Financial Concept: Quantifying potential losses and business impact
        """
        amount = transaction_data['amount']
        risk_level = transaction_data['risk_level']
        
        # Calculate potential financial impact
        if risk_level == 'HIGH':
            impact_multiplier = 5.0
        elif risk_level == 'MEDIUM':
            impact_multiplier = 2.0
        else:
            impact_multiplier = 1.0
            
        potential_impact = amount * impact_multiplier
        
        impact_assessment = {
            'estimated_loss': potential_impact,
            'impact_level': risk_level,
            'business_risk_category': self.get_business_risk_category(transaction_data)
        }
        
        return impact_assessment
    
    def get_business_risk_category(self, transaction_data):
        """
        Categorize business risk based on transaction characteristics
        """
        if transaction_data['amount'] > 50000:
            return 'HIGH_VALUE_TRANSACTION'
        elif transaction_data['is_velocity_anomaly']:
            return 'HIGH_FREQUENCY'
        elif transaction_data['is_location_anomaly']:
            return 'GEOGRAPHIC_ANOMALY'
        elif transaction_data['is_amount_anomaly']:
            return 'AMOUNT_ANOMALY'
        else:
            return 'BEHAVIORAL_ANOMALY'
    
    def get_recommended_action(self, transaction_data):
        """
        Provide recommended action based on risk assessment
        
        Financial Concept: Risk-based decision making
        """
        risk_level = transaction_data['risk_level']
        
        if risk_level == 'HIGH':
            return {
                'action': 'BLOCK_TRANSACTION',
                'urgency': 'IMMEDIATE',
                'department': 'FRAUD_INVESTIGATION',
                'estimated_cost_savings': transaction_data['amount'] * 0.9
            }
        elif risk_level == 'MEDIUM':
            return {
                'action': 'HOLD_FOR_REVIEW',
                'urgency': 'WITHIN_2_HOURS',
                'department': 'RISK_ASSESSMENT',
                'estimated_cost_savings': transaction_data['amount'] * 0.6
            }
        elif risk_level == 'LOW':
            return {
                'action': 'MONITOR',
                'urgency': 'WITHIN_24_HOURS',
                'department': 'COMPLIANCE',
                'estimated_cost_savings': transaction_data['amount'] * 0.3
            }
        else:
            return {
                'action': 'ALLOW',
                'urgency': 'N/A',
                'department': 'N/A',
                'estimated_cost_savings': 0
            }
    
    def send_alert(self, alert_data, alert_channel='console'):
        """
        Send alert through specified channel
        """
        if alert_channel == 'console':
            print(f"🚨 FRAUD ALERT: {json.dumps(alert_data, indent=2)}")
        elif alert_channel == 'email':
            self.send_email_alert(alert_data)
        elif alert_channel == 'api':
            self.send_api_alert(alert_data)
        else:
            print(f"Alert: {alert_data}")
    
    def send_email_alert(self, alert_data):
        """
        Send email alert to relevant stakeholders
        """
        # This is a placeholder - in production, configure actual email settings
        print(f"📧 Email alert sent: {alert_data['alert_id']} for transaction {alert_data['transaction_id']}")
    
    def send_api_alert(self, alert_data):
        """
        Send alert via API to external fraud management system
        """
        # This is a placeholder - in production, integrate with actual fraud management API
        print(f"📡 API alert sent: {alert_data['alert_id']}")


def apply_business_intelligence_enhancements(df):
    """
    Apply business intelligence enhancements to fraud alerts
    
    Financial Concept: Adding business context to technical fraud detection
    """
    # Add business impact score
    df = df.withColumn(
        "business_impact_score",
        when(col("risk_level") == "HIGH", col("amount") * 5)
        .when(col("risk_level") == "MEDIUM", col("amount") * 2)
        .otherwise(col("amount"))
    )
    
    # Add priority level based on business impact
    df = df.withColumn(
        "priority_level",
        when(col("risk_level") == "HIGH", "CRITICAL")
        .when((col("risk_level") == "MEDIUM") & (col("amount") > 10000), "HIGH")
        .when(col("risk_level") == "MEDIUM", "MEDIUM")
        .otherwise("LOW")
    )
    
    # Add estimated investigation time
    df = df.withColumn(
        "estimated_investigation_time_hours",
        when(col("risk_level") == "HIGH", 2.0)
        .when(col("risk_level") == "MEDIUM", 1.0)
        .otherwise(0.5)
    )
    
    return df


def create_executive_dashboard_metrics(df):
    """
    Create executive-level dashboard metrics for fraud detection
    
    Financial Concept: Executive KPIs for fraud management
    """
    # Overall metrics
    dashboard_metrics = df.agg(
        # Count of alerts by level
        sum((col("risk_level") == "HIGH").cast("int")).alias("high_risk_alerts"),
        sum((col("risk_level") == "MEDIUM").cast("int")).alias("medium_risk_alerts"),
        sum((col("risk_level") == "LOW").cast("int")).alias("low_risk_alerts"),
        
        # Financial metrics
        sum("amount").alias("total_amount_monitored"),
        sum(when(col("risk_level") != "LOW", col("amount")).otherwise(0)).alias("total_at_risk_amount"),
        
        # Average metrics
        avg("risk_score").alias("average_risk_score"),
        avg("business_impact_score").alias("average_business_impact"),
        
        # Volume metrics
        count("*").alias("total_alerts_generated")
    )
    
    return dashboard_metrics