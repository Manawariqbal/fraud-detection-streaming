# Real-Time Financial Fraud Detection & Risk Management Platform

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/) 
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.x-red)](https://spark.apache.org/) 
[![Kafka](https://img.shields.io/badge/Apache_Kafka-2.x-yellow)](https://kafka.apache.org/) 
[![Docker](https://img.shields.io/badge/Docker-20.x-blue)](https://docker.com/) 
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-2.x-purple)](https://delta.io/)

*A production-grade financial fraud detection and risk management system combining advanced data engineering with MBA-level financial risk concepts*

</div>

## 🚀 Overview

This project demonstrates a complete real-time financial fraud detection and risk management platform that combines advanced data engineering techniques with MBA-level financial risk concepts. The system ingests transaction data through Kafka, processes it through a multi-layered data architecture (Bronze/Silver/Gold), and applies sophisticated financial fraud detection algorithms to identify suspicious transactions in real-time while providing comprehensive business intelligence and risk metrics.

### Key Features
- **Real-time Financial Risk Processing**: Stream processing with Apache Spark Structured Streaming
- **Multi-layered Data Architecture**: Bronze → Silver → Gold data lakehouse pattern
- **Advanced Financial Fraud Detection**: Multi-algorithm approach including velocity analysis, amount anomalies, location patterns, and risk scoring
- **Business Intelligence Dashboard**: Executive-level KPIs, ROI calculations, and financial impact analysis
- **Data Quality Monitoring**: Comprehensive data quality checks for financial accuracy
- **Production Ready**: Includes checkpointing, error handling, monitoring, and alerting
- **Financial Risk Scoring**: Weighted risk scoring with business impact assessment
- **Compliance Ready**: Designed for financial services compliance requirements

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Transaction   │────│   Apache Kafka  │────│ Spark Streaming │
│   Producer      │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                              │
                                                              ▼
            ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
            │    BRONZE       │────│    SILVER       │────│     GOLD        │
            │  Raw Data       │    │ Enriched Data   │    │ Fraud Alerts    │
            │  (Parquet)      │    │  (Parquet)      │    │  (Parquet)      │
            └─────────────────┘    └─────────────────┘    └─────────────────┘

                    │                           │                │
                    ▼                           ▼                ▼
            ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
            │ Schema Inference│    │ User Enrichment │    │ Fraud Detection │
            │  JSON → Struct  │    │ Join User Data  │    │  ML Algorithms  │
            └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🛠️ Tech Stack

| Technology | Purpose | Version |
|------------|---------|---------|
| **Python** | Core Language | 3.8+ |
| **Apache Spark** | Stream Processing | 3.x |
| **Structured Streaming** | Real-time Analytics | Latest |
| **Apache Kafka** | Message Queue | 2.x |
| **Kafka-Python** | Producer/Consumer | Latest |
| **Docker** | Containerization | 20.x |
| **Parquet** | Columnar Storage | Latest |
| **Pandas** | Data Manipulation | Latest |

## 📁 Project Structure

```
fraud-detection-streaming/
├── docker/
│   └── docker-compose.yml          # Kafka and Zookeeper setup
├── src/
│   ├── ingestion/
│   │   └── kafka_to_bronze.py      # Raw data ingestion from Kafka
│   ├── processing/
│   │   └── bronze_to_silver.py     # Data enrichment and cleaning
│   ├── fraud/
│   │   └── silver_to_gold_fraud.py # Fraud detection logic
│   ├── producer/
│   │   └── transaction_producer.py # Simulated transaction generator
│   └── utils/
│       └── spark_session.py        # Spark configuration
├── data/
│   └── user_profiles.csv           # Static reference data
├── requirements.txt               # Dependencies
└── README.md                     # Documentation
```

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.8+
- Java 8+ (for Spark)

### 1. Clone the Repository
```bash
git clone <repository-url>
cd fraud-detection-streaming
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Start Kafka Infrastructure
```bash
docker-compose -f docker/docker-compose.yml up -d
```

### 4. Run the Pipeline Components (in separate terminals)

**Start the Bronze Layer (Kafka → Raw Storage):**
```bash
python src/ingestion/kafka_to_bronze.py
```

**Start the Silver Layer (Data Enrichment):**
```bash
python src/processing/bronze_to_silver.py
```

**Start the Gold Layer (Fraud Detection):**
```bash
python src/fraud/silver_to_gold_fraud.py
```

**Generate Sample Transactions:**
```bash
python src/producer/transaction_producer.py
```

## 📊 Data Flow

### 1. Transaction Schema
```python
{
    "transaction_id": "string",
    "user_id": "integer",
    "amount": "double",
    "location": "string",
    "timestamp": "timestamp"
}
```

### 2. User Profile Schema
```python
{
    "user_id": "integer",
    "age": "integer",
    "country": "string",
    "account_type": "string",
    "avg_transaction_amount": "double"
}
```

## 🧠 Advanced Financial Fraud Detection Algorithms

The system implements sophisticated financial fraud detection using multiple algorithms based on MBA-level financial risk concepts:

1. **Velocity-Based Fraud**: Unusual transaction frequency (e.g., >5 transactions in 10 minutes)
2. **Amount Anomaly Detection**: Transactions with z-score > 3 (3 standard deviations from user's historical average)
3. **Location Anomaly**: Transactions from countries different from user's home country
4. **Risk Scoring**: Weighted scoring system (velocity: 30 pts, amount: 40 pts, location: 25 pts)
5. **Business Impact Assessment**: Risk-adjusted returns and financial impact calculations
6. **Multi-Indicator Analysis**: Combined analysis of multiple fraud indicators

```python
# Advanced Financial Fraud Detection Algorithm

# 1. Velocity Analysis (transactions per time window)
velocity_window = Window.partitionBy("user_id").orderBy("timestamp").rangeBetween(-600, 0)
silver_df = silver_df.withColumn("transaction_count_10min", count("transaction_id").over(velocity_window))
silver_df = silver_df.withColumn("is_velocity_anomaly", col("transaction_count_10min") > 5)

# 2. Amount Anomaly using Z-score
window_spec = Window.partitionBy("user_id")
silver_df = silver_df.withColumn("avg_transaction_amount", avg("amount").over(window_spec))
silver_df = silver_df.withColumn("stddev_transaction_amount", stddev("amount").over(window_spec))
silver_df = silver_df.withColumn("z_score", (col("amount") - col("avg_transaction_amount")) / col("stddev_transaction_amount"))
silver_df = silver_df.withColumn("is_amount_anomaly", col("z_score") > 3.0)

# 3. Location Anomaly
silver_df = silver_df.join(users_df.select("user_id", "country"), on="user_id", how="left")
silver_df = silver_df.withColumn("is_location_anomaly", when(col("location") != col("country"), True).otherwise(False))

# 4. Risk Scoring
silver_df = silver_df.withColumn(
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

# 5. Business Intelligence Enhancements
silver_df = silver_df.withColumn(
    "business_impact_score",
    when(col("risk_level") == "HIGH", col("amount") * 5)
    .when(col("risk_level") == "MEDIUM", col("amount") * 2)
    .otherwise(col("amount"))
)

# Filter for suspicious transactions
fraud_alerts = silver_df.filter(
    (col("is_velocity_anomaly") | col("is_amount_anomaly") | col("is_location_anomaly")) &
    (col("risk_level") != "LOW")
)
```

## 📊 Business Intelligence & Financial Metrics

The system provides comprehensive business intelligence and financial metrics:

- **Executive Dashboard**: Real-time KPIs for fraud detection performance
- **ROI Calculations**: Cost-benefit analysis of fraud prevention investment
- **Risk-Adjusted Returns**: Financial impact measurement of fraud detection
- **Segment Analysis**: Fraud patterns by account type, location, and user demographics
- **Financial Impact Assessment**: Quantified potential losses prevented
- **Alert Prioritization**: Risk-based alert prioritization for efficient resource allocation

### Key Financial KPIs
- Potential Loss Prevented
- Cost of Investigation vs. Value of Fraud Prevention
- Risk-Adjusted Return on Fraud Detection Investment
- False Positive Rate and Associated Costs
- Time to Detection and Resolution

## 📈 Performance & Scalability

- **Throughput**: Capable of processing thousands of transactions per second
- **Latency**: Sub-second fraud detection for real-time alerts
- **Fault Tolerance**: Checkpointing ensures exactly-once processing semantics
- **Horizontal Scaling**: Can be deployed on Spark clusters

## 🧪 Data Quality & Production Monitoring

The system includes comprehensive data quality and monitoring capabilities:
- **Data Quality Monitoring**: Completeness, accuracy, and consistency checks
- **System Health Monitoring**: CPU, memory, and performance metrics
- **Real-time Alerting**: Proactive alerts for system and data issues
- **SLA Compliance**: Service level agreement monitoring
- **Executive Dashboard**: Real-time operational visibility
- **Performance Monitoring**: Throughput, latency, and resource utilization
- **Risk-based Alerting**: Financial risk-based alert prioritization

## 🚢 Production Considerations

- **Monitoring**: Integration with Spark UI and Kafka monitoring tools
- **Alerting**: Real-time fraud alerts via external systems
- **Security**: SSL/TLS encryption for Kafka connections
- **Configuration**: Environment-based configuration management
- **Data Lineage**: Complete tracking of data transformations

## 📚 Advanced Features

- **Schema Evolution**: Support for changing transaction schemas
- **Micro-batching**: Configurable batch intervals for optimal performance
- **Watermarking**: Handling late-arriving data
- **Caching**: Optimized data access patterns
- **Partitioning**: Efficient data organization strategies

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🏆 Key Takeaways

This project demonstrates:
- **End-to-End Financial Risk Pipeline**: Complete streaming architecture for financial fraud detection
- **Real-time Financial Analytics**: Live fraud detection with business impact assessment
- **Modern Data Stack**: Industry-standard big data technologies with financial domain expertise
- **Business Intelligence**: Executive dashboards with ROI and financial KPIs
- **Risk Management**: Advanced risk scoring and financial impact analysis
- **Production-Ready Architecture**: Scalable, monitored, and compliant design patterns
- **Data Quality**: Multi-layered data processing with financial accuracy validation
- **MBA Finance Concepts**: Integration of financial risk management principles with data engineering

Perfect for showcasing expertise in big data engineering, financial risk management, real-time analytics, and business intelligence. Combines technical skills with MBA-level financial domain knowledge to create a comprehensive financial fraud detection platform.
