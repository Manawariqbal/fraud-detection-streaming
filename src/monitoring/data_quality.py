"""
Data Quality Monitoring for Fraud Detection System

Financial Concepts Implemented:
- Data quality metrics for financial transactions
- Anomaly detection in data quality
- Quality scorecards for business stakeholders
"""

from pyspark.sql.functions import col, count, sum, avg, stddev, min, max, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from datetime import datetime


class DataQualityMonitor:
    """
    Monitor data quality for fraud detection system
    """
    
    def __init__(self):
        self.quality_thresholds = {
            'completeness': 0.95,  # 95% completeness required
            'accuracy': 0.98,      # 98% accuracy required
            'consistency': 0.97,   # 97% consistency required
            'timeliness': 300      # Data should be less than 5 minutes old
        }
    
    def calculate_completeness_metrics(self, df):
        """
        Calculate data completeness metrics
        
        Financial Concept: Complete data is essential for accurate fraud detection
        """
        total_records = df.count()
        
        completeness_metrics = {}
        for column in df.columns:
            non_null_count = df.filter(col(column).isNotNull()).count()
            completeness_metrics[column] = {
                'total_records': total_records,
                'non_null_count': non_null_count,
                'completeness_percentage': (non_null_count / total_records) * 100 if total_records > 0 else 0
            }
        
        return completeness_metrics
    
    def calculate_accuracy_metrics(self, df, reference_columns=None):
        """
        Calculate data accuracy metrics
        
        Financial Concept: Accurate data is critical for financial decision making
        """
        if reference_columns is None:
            reference_columns = ['amount', 'user_id', 'timestamp']
        
        accuracy_metrics = {}
        
        for col_name in reference_columns:
            if col_name in df.columns:
                # Check for valid ranges and patterns
                if col_name == 'amount':
                    # Amount should be positive
                    valid_amounts = df.filter(col(col_name) > 0).count()
                    total_amounts = df.count()
                    accuracy_metrics[col_name] = {
                        'valid_count': valid_amounts,
                        'total_count': total_amounts,
                        'accuracy_percentage': (valid_amounts / total_amounts) * 100 if total_amounts > 0 else 0
                    }
                elif col_name == 'user_id':
                    # User ID should be non-null and positive
                    valid_user_ids = df.filter(col(col_name).isNotNull() & (col(col_name) > 0)).count()
                    total_user_ids = df.count()
                    accuracy_metrics[col_name] = {
                        'valid_count': valid_user_ids,
                        'total_count': total_user_ids,
                        'accuracy_percentage': (valid_user_ids / total_user_ids) * 100 if total_user_ids > 0 else 0
                    }
                elif col_name == 'timestamp':
                    # Check for reasonable timestamp values
                    valid_timestamps = df.filter(col(col_name).isNotNull()).count()
                    total_timestamps = df.count()
                    accuracy_metrics[col_name] = {
                        'valid_count': valid_timestamps,
                        'total_count': total_timestamps,
                        'accuracy_percentage': (valid_timestamps / total_timestamps) * 100 if total_timestamps > 0 else 0
                    }
        
        return accuracy_metrics
    
    def calculate_consistency_metrics(self, df):
        """
        Calculate data consistency metrics
        
        Financial Concept: Consistent data patterns are important for fraud detection
        """
        consistency_metrics = {}
        
        # Check for consistency in amount patterns
        if 'amount' in df.columns:
            amount_stats = df.agg(
                avg('amount').alias('avg_amount'),
                stddev('amount').alias('stddev_amount'),
                min('amount').alias('min_amount'),
                max('amount').alias('max_amount')
            ).collect()[0]
            
            consistency_metrics['amount_consistency'] = {
                'avg_amount': float(amount_stats['avg_amount']) if amount_stats['avg_amount'] else 0.0,
                'stddev_amount': float(amount_stats['stddev_amount']) if amount_stats['stddev_amount'] else 0.0,
                'min_amount': float(amount_stats['min_amount']) if amount_stats['min_amount'] else 0.0,
                'max_amount': float(amount_stats['max_amount']) if amount_stats['max_amount'] else 0.0
            }
        
        # Check for consistency in user_id patterns
        if 'user_id' in df.columns:
            unique_users = df.select('user_id').distinct().count()
            total_records = df.count()
            
            consistency_metrics['user_distribution'] = {
                'unique_users': unique_users,
                'total_records': total_records,
                'records_per_user_avg': total_records / unique_users if unique_users > 0 else 0
            }
        
        return consistency_metrics
    
    def calculate_timeliness_metrics(self, df):
        """
        Calculate data timeliness metrics
        
        Financial Concept: Timely data is crucial for real-time fraud detection
        """
        if 'timestamp' in df.columns:
            latest_timestamp = df.agg(max('timestamp')).collect()[0][0]
            earliest_timestamp = df.agg(min('timestamp')).collect()[0][0]
            
            timeliness_metrics = {
                'latest_record_timestamp': str(latest_timestamp) if latest_timestamp else None,
                'earliest_record_timestamp': str(earliest_timestamp) if earliest_timestamp else None,
                'data_age_hours': None
            }
            
            return timeliness_metrics
        else:
            return {}
    
    def generate_data_quality_report(self, df):
        """
        Generate comprehensive data quality report
        
        Financial Concept: Quality scorecards for business stakeholders
        """
        completeness = self.calculate_completeness_metrics(df)
        accuracy = self.calculate_accuracy_metrics(df)
        consistency = self.calculate_consistency_metrics(df)
        timeliness = self.calculate_timeliness_metrics(df)
        
        quality_report = {
            'report_timestamp': str(current_timestamp()),
            'completeness_metrics': completeness,
            'accuracy_metrics': accuracy,
            'consistency_metrics': consistency,
            'timeliness_metrics': timeliness,
            'overall_quality_score': self.calculate_overall_quality_score(completeness, accuracy, consistency)
        }
        
        return quality_report
    
    def calculate_overall_quality_score(self, completeness, accuracy, consistency):
        """
        Calculate overall data quality score
        
        Financial Concept: Composite score for data quality assessment
        """
        # Calculate average completeness
        completeness_scores = [v['completeness_percentage'] for v in completeness.values()]
        avg_completeness = sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0
        
        # Calculate average accuracy
        accuracy_scores = [v['accuracy_percentage'] for v in accuracy.values()]
        avg_accuracy = sum(accuracy_scores) / len(accuracy_scores) if accuracy_scores else 0
        
        # For consistency, we'll use a simplified approach
        consistency_score = 95.0  # Placeholder - would be more complex in real implementation
        
        overall_score = (avg_completeness + avg_accuracy + consistency_score) / 3
        
        return overall_score
    
    def check_quality_alerts(self, quality_report):
        """
        Check if data quality falls below acceptable thresholds
        
        Financial Concept: Alerting for data quality issues that could impact fraud detection
        """
        alerts = []
        
        # Check overall quality score
        if quality_report['overall_quality_score'] < 90:
            alerts.append({
                'alert_type': 'LOW_QUALITY_SCORE',
                'severity': 'HIGH',
                'message': f'Overall data quality score is {quality_report["overall_quality_score"]:.2f}%, below acceptable threshold',
                'timestamp': str(current_timestamp())
            })
        
        # Check completeness for critical fields
        critical_fields = ['transaction_id', 'user_id', 'amount', 'timestamp']
        for field in critical_fields:
            if field in quality_report['completeness_metrics']:
                completeness_pct = quality_report['completeness_metrics'][field]['completeness_percentage']
                if completeness_pct < self.quality_thresholds['completeness'] * 100:
                    alerts.append({
                        'alert_type': 'LOW_COMPLETENESS',
                        'severity': 'HIGH',
                        'message': f'Completeness for {field} is {completeness_pct:.2f}%, below threshold of {self.quality_thresholds["completeness"] * 100}%',
                        'timestamp': str(current_timestamp())
                    })
        
        # Check accuracy for critical fields
        for field in ['amount', 'user_id']:
            if field in quality_report['accuracy_metrics']:
                accuracy_pct = quality_report['accuracy_metrics'][field]['accuracy_percentage']
                if accuracy_pct < self.quality_thresholds['accuracy'] * 100:
                    alerts.append({
                        'alert_type': 'LOW_ACCURACY',
                        'severity': 'HIGH',
                        'message': f'Accuracy for {field} is {accuracy_pct:.2f}%, below threshold of {self.quality_thresholds["accuracy"] * 100}%',
                        'timestamp': str(current_timestamp())
                    })
        
        return alerts


def apply_data_quality_checks(df):
    """
    Apply comprehensive data quality checks to fraud detection data
    
    Financial Concept: Ensuring data quality for accurate fraud detection
    """
    # Initialize quality monitor
    dq_monitor = DataQualityMonitor()
    
    # Generate quality report
    quality_report = dq_monitor.generate_data_quality_report(df)
    
    # Check for quality alerts
    alerts = dq_monitor.check_quality_alerts(quality_report)
    
    # Print quality summary
    print("\n" + "="*60)
    print("🔍 DATA QUALITY REPORT")
    print("="*60)
    print(f"Overall Quality Score: {quality_report['overall_quality_score']:.2f}%")
    print(f"Completeness: {quality_report['completeness_metrics']['transaction_id']['completeness_percentage']:.2f}% (transaction_id)")
    print(f"Accuracy (amount): {quality_report['accuracy_metrics'].get('amount', {}).get('accuracy_percentage', 0):.2f}%")
    print(f"Quality Alerts: {len(alerts)}")
    print("="*60)
    
    # Log quality alerts
    for alert in alerts:
        print(f"⚠️  ALERT: {alert['message']}")
    
    return quality_report, alerts


def validate_transaction_schema(df):
    """
    Validate transaction data against expected schema
    
    Financial Concept: Schema validation for financial transaction integrity
    """
    required_columns = ['transaction_id', 'user_id', 'amount', 'location', 'timestamp']
    
    missing_columns = []
    for col_name in required_columns:
        if col_name not in df.columns:
            missing_columns.append(col_name)
    
    if missing_columns:
        raise ValueError(f"Missing required columns for fraud detection: {missing_columns}")
    
    # Validate data types
    schema_validation = {
        'transaction_id': str(df.schema['transaction_id'].dataType) if 'transaction_id' in df.columns else 'MISSING',
        'user_id': str(df.schema['user_id'].dataType) if 'user_id' in df.columns else 'MISSING',
        'amount': str(df.schema['amount'].dataType) if 'amount' in df.columns else 'MISSING',
        'location': str(df.schema['location'].dataType) if 'location' in df.columns else 'MISSING',
        'timestamp': str(df.schema['timestamp'].dataType) if 'timestamp' in df.columns else 'MISSING'
    }
    
    return schema_validation


def detect_data_anomalies(df):
    """
    Detect data anomalies that could indicate data quality issues
    
    Financial Concept: Anomaly detection in data quality metrics
    """
    anomalies = []
    
    # Check for unusual amount patterns
    if 'amount' in df.columns:
        amount_stats = df.agg(
            avg('amount').alias('avg_amount'),
            stddev('amount').alias('stddev_amount')
        ).collect()[0]
        
        avg_amount = float(amount_stats['avg_amount']) if amount_stats['avg_amount'] else 0.0
        stddev_amount = float(amount_stats['stddev_amount']) if amount_stats['stddev_amount'] else 0.0
        
        # Look for transactions that are extremely high (potential data entry errors)
        extreme_threshold = avg_amount + (5 * stddev_amount)
        extreme_count = df.filter(col('amount') > extreme_threshold).count()
        
        if extreme_count > 0:
            anomalies.append({
                'type': 'EXTREME_AMOUNT',
                'count': extreme_count,
                'threshold': extreme_threshold,
                'message': f'Found {extreme_count} transactions with extremely high amounts (>5σ from mean)'
            })
    
    # Check for duplicate transaction IDs
    if 'transaction_id' in df.columns:
        total_count = df.count()
        distinct_count = df.select('transaction_id').distinct().count()
        duplicate_count = total_count - distinct_count
        
        if duplicate_count > 0:
            anomalies.append({
                'type': 'DUPLICATE_TRANSACTION_IDS',
                'count': duplicate_count,
                'message': f'Found {duplicate_count} duplicate transaction IDs'
            })
    
    return anomalies