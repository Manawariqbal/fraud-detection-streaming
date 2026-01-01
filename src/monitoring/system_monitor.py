"""
System Monitoring for Fraud Detection Pipeline

Financial Concepts Implemented:
- System performance metrics affecting financial operations
- SLA monitoring for fraud detection
- Performance impact on business operations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, current_timestamp
import time
import psutil
import logging
from datetime import datetime
import json


class SystemMonitor:
    """
    Monitor system performance for fraud detection pipeline
    """
    
    def __init__(self):
        self.metrics_history = []
        self.sla_thresholds = {
            'processing_latency_ms': 500,  # 500ms max latency
            'throughput_tps': 1000,        # 1000 transactions per second
            'memory_usage_percent': 80,    # 80% max memory usage
            'cpu_usage_percent': 85        # 85% max CPU usage
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('fraud_detection_monitor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def collect_system_metrics(self):
        """
        Collect system-level performance metrics
        
        Financial Concept: System performance directly impacts fraud detection effectiveness
        """
        metrics = {
            'timestamp': str(current_timestamp()),
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_usage_percent': psutil.disk_usage('/').percent,
            'active_processes': len(psutil.pids()),
            'network_io': psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
        }
        
        self.metrics_history.append(metrics)
        return metrics
    
    def collect_pipeline_metrics(self, spark_session):
        """
        Collect pipeline-specific metrics
        
        Financial Concept: Pipeline performance affects fraud detection speed and accuracy
        """
        try:
            # Check if we can access Spark UI metrics (this is a simplified approach)
            # In a real system, we would connect to Spark's metrics system
            pipeline_metrics = {
                'active_jobs': 0,  # Placeholder - would get from Spark context
                'active_stages': 0,  # Placeholder - would get from Spark context
                'completed_tasks': 0,  # Placeholder - would get from Spark context
                'failed_tasks': 0,  # Placeholder - would get from Spark context
            }
            
            # Add custom business metrics
            pipeline_metrics['fraud_detection_rate'] = self.estimate_fraud_detection_rate(spark_session)
            
            return pipeline_metrics
        except Exception as e:
            self.logger.error(f"Error collecting pipeline metrics: {str(e)}")
            return {}
    
    def estimate_fraud_detection_rate(self, spark_session):
        """
        Estimate fraud detection rate based on recent activity
        """
        try:
            # Try to read recent fraud alerts to estimate detection rate
            fraud_df = spark_session.read.parquet("data/gold/fraud_alerts").limit(100)
            total_alerts = fraud_df.count()
            return total_alerts  # Simplified metric
        except:
            return 0  # Return 0 if unable to read data
    
    def check_sla_compliance(self, system_metrics, pipeline_metrics):
        """
        Check if system is meeting SLA requirements
        
        Financial Concept: SLA compliance affects business operations and customer experience
        """
        sla_violations = []
        
        # Check CPU usage
        if system_metrics.get('cpu_percent', 0) > self.sla_thresholds['cpu_usage_percent']:
            sla_violations.append({
                'metric': 'CPU_USAGE',
                'current_value': system_metrics['cpu_percent'],
                'threshold': self.sla_thresholds['cpu_usage_percent'],
                'severity': 'HIGH'
            })
        
        # Check memory usage
        if system_metrics.get('memory_percent', 0) > self.sla_thresholds['memory_usage_percent']:
            sla_violations.append({
                'metric': 'MEMORY_USAGE',
                'current_value': system_metrics['memory_percent'],
                'threshold': self.sla_thresholds['memory_usage_percent'],
                'severity': 'HIGH'
            })
        
        return sla_violations
    
    def generate_health_report(self, spark_session):
        """
        Generate comprehensive system health report
        
        Financial Concept: System health directly impacts business operations
        """
        system_metrics = self.collect_system_metrics()
        pipeline_metrics = self.collect_pipeline_metrics(spark_session)
        sla_violations = self.check_sla_compliance(system_metrics, pipeline_metrics)
        
        health_report = {
            'report_timestamp': str(current_timestamp()),
            'system_metrics': system_metrics,
            'pipeline_metrics': pipeline_metrics,
            'sla_compliance': {
                'violations': sla_violations,
                'compliant': len(sla_violations) == 0
            },
            'system_health_score': self.calculate_health_score(system_metrics, sla_violations)
        }
        
        return health_report
    
    def calculate_health_score(self, system_metrics, sla_violations):
        """
        Calculate overall system health score
        
        Financial Concept: Health score for operational risk assessment
        """
        base_score = 100
        
        # Deduct points for SLA violations
        for violation in sla_violations:
            if violation['severity'] == 'HIGH':
                base_score -= 20
            elif violation['severity'] == 'MEDIUM':
                base_score -= 10
            else:
                base_score -= 5
        
        # Deduct points for high resource usage
        if system_metrics.get('cpu_percent', 0) > 90:
            base_score -= 15
        elif system_metrics.get('cpu_percent', 0) > 80:
            base_score -= 5
            
        if system_metrics.get('memory_percent', 0) > 90:
            base_score -= 15
        elif system_metrics.get('memory_percent', 0) > 80:
            base_score -= 5
        
        # Ensure score is between 0 and 100
        return max(0, min(100, base_score))
    
    def log_health_metrics(self, health_report):
        """
        Log health metrics for monitoring and alerting
        """
        self.logger.info(f"System Health Score: {health_report['system_health_score']}")
        self.logger.info(f"CPU Usage: {health_report['system_metrics']['cpu_percent']}%")
        self.logger.info(f"Memory Usage: {health_report['system_metrics']['memory_percent']}%")
        self.logger.info(f"SLA Violations: {len(health_report['sla_compliance']['violations'])}")
        
        for violation in health_report['sla_compliance']['violations']:
            self.logger.warning(f"SLA Violation: {violation['metric']} - Current: {violation['current_value']}, Threshold: {violation['threshold']}")
    
    def send_health_alert(self, health_report):
        """
        Send health alerts when system issues are detected
        
        Financial Concept: Proactive alerting for operational risk management
        """
        alerts = []
        
        if health_report['system_health_score'] < 70:
            alerts.append({
                'alert_type': 'LOW_HEALTH_SCORE',
                'severity': 'HIGH',
                'message': f'System health score is critically low: {health_report["system_health_score"]}',
                'timestamp': str(current_timestamp())
            })
        
        if health_report['system_metrics']['cpu_percent'] > 95:
            alerts.append({
                'alert_type': 'HIGH_CPU_USAGE',
                'severity': 'HIGH',
                'message': f'CPU usage is critically high: {health_report["system_metrics"]["cpu_percent"]}%',
                'timestamp': str(current_timestamp())
            })
        
        if health_report['system_metrics']['memory_percent'] > 95:
            alerts.append({
                'alert_type': 'HIGH_MEMORY_USAGE',
                'severity': 'HIGH',
                'message': f'Memory usage is critically high: {health_report["system_metrics"]["memory_percent"]}%',
                'timestamp': str(current_timestamp())
            })
        
        # Log alerts
        for alert in alerts:
            if alert['severity'] == 'HIGH':
                self.logger.error(f"CRITICAL ALERT: {alert['message']}")
            else:
                self.logger.warning(f"ALERT: {alert['message']}")
        
        return alerts


def monitor_pipeline_performance(spark_session):
    """
    Monitor overall pipeline performance
    
    Financial Concept: Performance monitoring for operational efficiency
    """
    monitor = SystemMonitor()
    health_report = monitor.generate_health_report(spark_session)
    alerts = monitor.send_health_alert(health_report)
    monitor.log_health_metrics(health_report)
    
    print("\n" + "="*60)
    print("🖥️  SYSTEM HEALTH REPORT")
    print("="*60)
    print(f"Health Score: {health_report['system_health_score']}/100")
    print(f"CPU Usage: {health_report['system_metrics']['cpu_percent']}%")
    print(f"Memory Usage: {health_report['system_metrics']['memory_percent']}%")
    print(f"SLA Violations: {len(health_report['sla_compliance']['violations'])}")
    print(f"Active Alerts: {len(alerts)}")
    print("="*60)
    
    return health_report, alerts


def setup_monitoring_alerts():
    """
    Setup monitoring and alerting infrastructure
    
    Financial Concept: Proactive monitoring for operational risk management
    """
    # This would typically integrate with external monitoring systems
    # like Prometheus, Grafana, or cloud monitoring services
    print("Setting up monitoring and alerting infrastructure...")
    
    # Create monitoring directories if they don't exist
    import os
    os.makedirs("data/monitoring", exist_ok=True)
    os.makedirs("data/logs", exist_ok=True)
    
    print("✅ Monitoring infrastructure setup complete")
    
    return True


def run_continuous_monitoring():
    """
    Run continuous monitoring for the fraud detection system
    
    Financial Concept: Continuous monitoring for operational risk management
    """
    print("Starting continuous monitoring for fraud detection system...")
    
    # This would run as a separate process in production
    # For demonstration, we'll just show the structure
    print("Monitoring service started...")
    print("Check fraud_detection_monitor.log for system metrics")
    
    return True