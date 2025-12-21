# E-Commerce Data Warehouse & Real-Time Dashboard (PySpark + Streamlit)

A production-ready, modular data engineering project implementing a **medallion architecture** (Bronze/Silver/Gold) for an e-commerce data warehouse. This project demonstrates enterprise data engineering practices including batch ETL, real-time streaming, data quality analytics, and interactive dashboards.

## 🎯 Business Context

**Problem**: An online retail company needs to consolidate data from multiple sources (transactional orders, CRM customers, product catalog) for both historical business intelligence and real-time operational monitoring.

**Solution**: A comprehensive data platform that:
- **Ingests** raw data from CSV, JSON, and Parquet sources (Batch & Streaming)
- **Validates** data quality (nulls, duplicates, date ranges) across all layers
- **Transforms** data into dimensional models (fact tables, customer metrics)
- **Enriches** with business logic (RFM segmentation, anomaly detection)
- **Visualizes** insights through interactive Streamlit dashboards (Batch & Real-time)

**Business Value**:
- **Data-Driven Decision Making**: Consolidated analytics for strategic planning
- **Customer Intelligence**: RFM segmentation for targeted marketing
- **Operational Excellence**: Real-time monitoring of revenue and order flow
- **Early Warning System**: Automated anomaly detection for rapid response

---

## 📁 Project Structure

The project is organized into logical modules for better maintainability and scalability:

```
dw-dashboard/
├── config/                 # Centralized configuration
│   ├── settings.py         # Data paths and business rules
│   └── spark_config.py     # Spark session factory and optimization settings
├── dashboards/             # Interactive Streamlit dashboards
│   ├── enhanced_app.py     # Primary batch BI dashboard
│   ├── app_realtime.py     # Real-time streaming dashboard
│   └── app_simple_kafka.py # Lightweight Kafka-mimic dashboard
├── scripts/                # Entry points and automation scripts
│   ├── main.py             # Core ETL pipeline orchestrator
│   ├── run_batch_etl.py    # Batch ETL runner
│   ├── run_dashboard.py    # Dashboard launcher
│   └── init_raw_data.py    # Demo data initializer
├── streaming/              # Real-time processing pipelines
│   ├── file_based/         # File-trigger streaming
│   ├── kafka/              # Production Kafka streaming
│   └── simple_kafka/       # Embedded Kafka-mimic for development
├── utils/                  # Reusable utility functions
│   ├── analytics.py        # BI and reporting logic
│   ├── transformations.py  # Data modeling and enrichment
│   ├── quality_checks.py   # Data quality validation
│   └── io.py               # Data lake I/O operations
├── lake/                   # Medallion architecture (Bronze/Silver/Gold)
└── requirements.txt        # Project dependencies (Streamlit >= 1.40.1)
```

---

## 🚀 Getting Started

### 1. Prerequisites
- Python 3.8+
- Java 8/11 (for PySpark)
- [Optional] Docker (for full Kafka pipeline)

### 2. Installation
```bash
# Clone the repository
git clone <repository-url>
cd dw-dashboard

# Install dependencies
pip install -r requirements.txt
```

### 3. Quick Start (Batch Pipeline)
```bash
# 1. Initialize sample data
python scripts/init_raw_data.py

# 2. Run the ETL pipeline
python scripts/run_batch_etl.py

# 3. Launch the dashboard
python scripts/run_dashboard.py
```

### 4. Quick Start (Real-Time Pipeline)
```bash
# Start the lightweight streaming pipeline with dashboard
python scripts/run_simple_kafka_pipeline.py
```

---

## 🔧 Core Components

### Batch ETL Pipeline (`scripts/main.py`)
Implements a full medallion architecture:
1. **Bronze**: Raw ingestion from source systems
2. **Silver**: Cleaned data, schema enforcement, and dimensional modeling
3. **Gold**: Pre-aggregated, dashboard-optimized tables

### Data Quality Analytics
Integrated validation at every step:
- Null value detection and reporting
- Primary key uniqueness verification
- Business rule validation (e.g., date ranges, status codes)
- Before/After transformation comparisons

### Interactive Dashboards
Built with **Streamlit 1.40.1**, providing:
- **Business Intelligence**: Revenue trends, category performance, and customer segments
- **Customer Analytics**: RFM analysis (Recency, Frequency, Monetary)
- **Operational Monitoring**: Real-time order flow and system health
- **Data Quality Reports**: Visual assessment of data health across the pipeline

---

## 🛠️ Technology Stack
- **Processing**: PySpark (Spark SQL & Structured Streaming)
- **Visualization**: Streamlit, Plotly
- **Storage**: Parquet, CSV, JSON (Medallion Architecture)
- **Messaging**: Kafka (or embedded Simple Kafka fallback)
- **Environment**: Python, Docker (optional)

---

## 📈 Business Use Cases
- **Marketing**: Target "High Value" segments identified by RFM analysis.
- **Operations**: Monitor real-time order volume to identify system bottlenecks.
- **Finance**: Track revenue anomalies to detect potential billing issues or market shifts.
- **Data Governance**: Monitor data quality scores to ensure reliability of downstream reports.
