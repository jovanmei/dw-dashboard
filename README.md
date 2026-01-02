# 🛒 E-Commerce Data Warehouse & Real-Time Dashboard

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://dw-dashboard.streamlit.app/)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/Apache-Spark-orange.svg)](https://spark.apache.org/docs/latest/api/python/)
[![Medallion Architecture](https://img.shields.io/badge/Architecture-Medallion-green.svg)](#the-medallion-architecture)

A production-ready, modular data engineering project implementing a **Medallion Architecture** (Bronze/Silver/Gold) for an e-commerce data warehouse. This project demonstrates enterprise practices including batch ETL, real-time streaming, automated data quality analytics, and interactive BI dashboards.

---

## 📑 Table of Contents
1. [Business Context](#-business-context)
2. [Key Features](#-key-features)
3. [Architecture Deep Dive](#-architecture-deep-dive)
4. [File-by-File Technical Directory](#-file-by-file-technical-directory)
5. [Data Quality (DQ) Framework](#-data-quality-dq-framework)
6. [Coding Standards & Guidelines](#-coding-standards--guidelines)
7. [Getting Started](#-getting-started)
8. [Streaming Strategy](#-streaming-strategy)
9. [Interview Preparation](#-interview-preparation-key-technical-concepts)
10. [Full Command Reference](#-full-command-reference)
11. [License](#-license)

---

## ⚡ Quick Start (Batch Mode)
```bash
# 1. Initialize messy raw data
python src/batch/bootstrap.py

# 2. Run the full Medallion ETL pipeline
python scripts/run_batch.py

# 3. Launch the BI Dashboard
python scripts/run_dashboard.py
```

## ⚡ Quick Start (Streaming Mode)
```bash
# Launch the full streaming stack (Server + Generator + Pipeline + Dashboard)
python scripts/run_streaming_simple.py
```

### 3. Access Dashboard
- Open your browser to `http://localhost:8503` to see the real-time visualization.
- *Note: You can also use `python scripts/run_dashboard.py simple` to launch just the monitor.*

---

## 🎯 Business Context

**Problem**: Modern e-commerce platforms generate massive amounts of "dirty" data across siloed systems (Web logs, CRM, ERP). Businesses struggle to get a unified, reliable view of their performance in both historical and real-time contexts.

**Solution**: This platform provides an end-to-end data lakehouse solution that:
- **Cleanses** messy production data (missing values, duplicates, format inconsistencies).
- **Unifies** customer and order data into a dimensional model.
- **Predicts** customer value via RFM (Recency, Frequency, Monetary) segmentation.
- **Monitors** operational health through sub-second real-time dashboards.

---

## ✨ Key Features

- **🛡️ Robust ETL**: Medallion architecture ensures data lineage and reliability.
- **🔍 Auto-DQ (Data Quality)**: Integrated validation framework checking for nulls, duplicates, and referential integrity at every layer.
- **📈 Advanced Analytics**: 
    - **RFM Segmentation**: Automatically categorizes customers into "Champions", "At Risk", etc.
    - **Anomaly Detection**: Identifies revenue outliers using statistical thresholds.
    - **Fraud Monitoring**: Real-time detection of suspicious order patterns.
- **🚀 Dual-Mode Dashboards**:
    - **Batch BI**: Deep-dive historical analysis and portfolio-ready visualizations.
    - **Real-Time**: Low-latency monitoring via Spark Structured Streaming.
- **☁️ Cloud Optimized**: Fully compatible with Streamlit Cloud via an embedded "Simple Kafka" message broker.

---

## 🏗️ Architecture Deep Dive

### The Medallion Architecture
We use a three-layer approach to ensure data quality and scalability:

1.  **🥉 Bronze (Raw)**: 
    - **Source**: CSV/JSON/Parquet from multiple systems.
    - **Action**: Direct ingestion with minimal transformation. Preserves the "truth" of source data.
2.  **🥈 Silver (Enriched)**:
    - **Action**: Schema enforcement, data type conversion, name/email standardization, and joining across entities.
    - **Result**: A clean, queryable dimensional model (Fact & Dimension tables).
3.  **🥇 Gold (Curated)**:
    - **Action**: High-level aggregations and business logic (RFM, Monthly Revenue).
    - **Result**: Dashboard-ready datasets optimized for performance.

### Data Flow Diagram
```text
[Sources] -> [Bronze Layer] -> [DQ Checks] -> [Silver Layer] -> [Analytics] -> [Gold Layer] -> [Dashboards]
   ^              |               |               |              |              |              |
 (CSV/JSON)    (Parquet)      (Validation)     (Cleaned)      (RFM/Fraud)    (Aggregated)    (Streamlit)
```

---

## 📁 Project Structure
### 📦 Core Business Logic (`src/batch`)
- `pipeline.py`: **Medallion Orchestrator**. The heart of the batch ETL, managing transitions from Bronze → Silver → Gold layers.
- `generator.py`: **Data Engine**. Responsible for creating the "messy" synthetic data that mimics production anomalies.
- `bootstrap.py`: **Bootstrap Script**. Materializes the raw CSV/JSON/Parquet files needed to kick off the batch pipeline.

### ⚙️ Configuration (`src/config`)
- `settings.py`: **Centralized State Management**. Uses Python's `dataclasses` and `pathlib` for type-safe, immutable configurations. Defines environment paths for Bronze, Silver, and Gold layers.
- `spark_config.py`: **Performance Tuning**. Configures the Spark session with optimized settings for local execution, including `spark.sql.shuffle.partitions` and memory allocation to prevent OOM errors during large joins.

### 📊 Dashboards (`src/dashboards`)
- `batch_view.py`: **Executive BI Tool**. A multi-page Streamlit app that visualizes:
    - **KPI Overviews**: Revenue, order volume, and customer growth.
    - **Customer Analytics**: RFM segmentation and top-tier customer behavior.
    - **Data Health**: Interactive DQ reports showing null counts and duplicate trends.
- `broker_monitor.py`: **Operational Monitor**. A low-latency dashboard that connects to the `simple_kafka` REST proxy. It provides real-time visibility into the message broker's health and topic throughput.
- `realtime_view.py`: **Enterprise Streaming UI**. Designed for production Kafka environments, using Spark Structured Streaming to push live updates to the frontend.

### 🚀 Execution Scripts (`/scripts`)
- `run_batch.py`: **Batch Entry Point**. Triggers the full Medallion workflow (Bronze → Silver → Gold) with integrated logging and error handling.
- `run_streaming_simple.py`: **Streaming Entry Point**. Uses Python's `subprocess` to launch the entire "Simple Kafka" ecosystem (Broker, Generator, Dashboard) as a single unit.
- `run_streaming_spark.py`: **File-based Streaming Entry Point**. Demonstrates Spark Structured Streaming using file sources (JSON/CSV) as a stream.
- `inspect_simple_kafka.py`: **Data Inspection**. CLI tool to query the Simple Kafka SQLite database directly to view messages, topics, and consumer progress.
- `run_dashboard.py`: **BI Entry Point**. Launches the Streamlit dashboard. Supports `batch` (default), `spark` (real-time), and `simple` (monitor) modes.

### 🌊 Streaming Engine (`src/streaming`)
- `/simple`: **[Technical Highlight]** A lightweight, custom-built message broker.
    - `server.py`: **Broker Core**. Implements a REST API (Flask) over a SQLite backend.
    - `generator.py`: **Mock Producer**. Simulates high-velocity traffic for testing.
- `/spark`: **Enterprise Streaming**. Core logic for Spark Structured Streaming.
    - `pipeline.py`: **Stream Processor**. Implements windowed aggregations and fraud detection.
    - `data_generator.py`: **Stream Source**. Generates continuous JSON/Kafka events.
    - `runner.py`: **Orchestrator**. Manages the startup of streaming components.
- `/kafka`: **Kafka Integration**. Contains configurations and runners for a standard Kafka stack.

### 🛠️ Utilities & Logic (`src/utils`)
- `quality_checks.py`: **Data Integrity Engine**. A suite of Spark-based validation functions that compute:
    - **Completeness**: Null/NaN percentages per column.
    - **Uniqueness**: Duplicate primary key detection via `groupBy` and `count`.
    - **Validity**: Date range boundaries and business logic constraints.
- `transformations.py`: **The "Transformer"**. Contains modular Spark logic for:
    - **Star Schema Joins**: Linking Fact (Orders) with Dimensions (Customers, Products).
    - **Derived Columns**: Year/Month extraction and status flags.
    - **Customer Metrics**: Aggregating lifetime value (LTV) and tenure.
- `analytics.py`: **Business Insights Engine**. Implements advanced algorithms:
    - **RFM Logic**: Recency, Frequency, and Monetary scoring to segment customers.
    - **Anomaly Detection**: Statistical analysis to identify significant revenue drops vs. previous periods.
- `data_cleaning.py`: **PII & Standardization**. Rule-based cleaning for name casing, email formatting, and timestamp normalization.
- `io.py`: **Input/Output Helpers**. Reusable Spark write helpers for different storage layers.

### 🧪 Quality Assurance (`/tests`)
- `test_environment.py`: **Pre-flight Check**. Verifies Java, Python, and Spark dependencies are correctly configured before pipeline execution.
- `test_dashboard.py`: **E2E Validation**. Ensures that the Gold layer Parquet files are schema-compatible with the Streamlit visualization engine.
- `test_enhanced_kafka.py`: **Stress Testing**. Simulates high-load scenarios on the `simple_kafka` broker to validate SQLite concurrency and offset accuracy.
- `test_kafka_production.py`: **Integration Testing**. Validates connectivity and message flow for enterprise-grade Kafka clusters.

---

## 🛠️ Core Modules & Implementation Details

### 🛡️ Data Quality (DQ) Framework
The `quality_checks.py` module is the project's "safety net." It is designed to be **extensible**:
- **Implementation**: Uses Spark's `sum(col.isNull())` for efficient null counting across large datasets.
- **Quarantine Logic**: (Optional implementation) Records failing critical checks are flagged in the Silver layer, allowing downstream Gold aggregations to remain pristine.

### 📉 Dimensional Modeling
In `transformations.py`, we transition from a normalized relational structure to a **Star Schema**:
- **Fact Table**: `fact_orders` contains transactional data and foreign keys.
- **Dimensions**: `dim_customers` and `dim_products` provide descriptive context.
- **Why?**: This structure optimizes query performance for BI tools like Streamlit and Tableau.

### 📡 Simple Kafka: The REST Proxy Pattern
Built for environments where Docker is restricted, `simple_kafka` demonstrates a deep understanding of message broker architecture:
- **ACID Transactions**: SQLite ensures that message production and offset commits are atomic.
- **Consumer Offsets**: Tracking `current_offset` per `group_id` allows consumers to resume from where they left off, exactly like production Kafka.
- **Scalability**: While in-memory/SQLite based, the REST API design allows for multi-producer/multi-consumer interaction.
- **Data Inspection**: Use `python scripts/inspect_simple_kafka.py` to query the SQLite database directly and view messages, topics, and consumer progress.

---

## 🎓 Interview Preparation: Key Technical Concepts

If you are presenting this project in an interview, here are the core concepts and design decisions to highlight:

### 1. The "Simple Kafka" Architecture
**Question**: *Why did you build your own message broker?*
**Answer**: "I wanted to demonstrate streaming architecture on restricted platforms like Streamlit Cloud. I implemented a **REST Proxy Pattern** using a SQLite backend for ACID-compliant message storage. Key features include **offset management** (tracking consumer progress) and **consumer groups** (sharing workload). This proves I understand the 'under-the-hood' mechanics of how Kafka actually manages state and persistence."

### 2. Medallion Architecture & Data Reliability
**Question**: *How do you ensure data quality across the pipeline?*
**Answer**: "I follow the **Medallion Architecture**. **Bronze** layer preserves the raw 'source of truth'. **Silver** layer performs schema enforcement and joins (Fact & Dimension tables). **Gold** layer provides business-level aggregations. This ensures that if a bug is found in the logic, we can re-process data from the raw layer without losing information."

### 3. Data Quality (DQ) as a First-Class Citizen
**Question**: *How do you deal with 'dirty' data?*
**Answer**: "In this project, Data Quality isn't an afterthought. I built a custom **DQ Framework** (`utils/quality_checks.py`) that validates data at every transition. We track **Completeness, Uniqueness, and Validity**. Any data failing critical checks (like missing Customer IDs) is quarantined, ensuring the Gold layer remains 100% reliable for business decisions."

### 4. RFM Analytics & Business Value
**Question**: *How does this project drive business impact?*
**Answer**: "Beyond the engineering, the project provides actionable insights. The **RFM Analysis** automatically segments customers based on their behavior (Recency, Frequency, Monetary). This allows marketing teams to immediately identify 'Champions' for loyalty programs or 'At Risk' customers for re-engagement campaigns."

---

## 🛠️ Data Engineering Skills Demonstrated
| Skill | Implementation |
| :--- | :--- |
| **Apache Spark** | Structured Streaming, Spark SQL, Window Functions, Optimization. |
| **Python** | OOP, Flask (REST API), Subprocess Management, Dataclasses. |
| **Data Modeling** | Star Schema (Fact/Dimension), Medallion Architecture. |
| **Streaming** | Message Brokers, Offsets, Producers/Consumers, REST Proxy. |
| **DevOps** | Process Orchestration, Environment Validation, Logging. |

---

## 🔍 Data Quality (DQ) Framework

Our `quality_checks.py` module runs at every stage. It computes:

| Pillar | Description | Action if Failed |
| :--- | :--- | :--- |
| **Completeness** | Null/NaN percentage per column | Logged; nulls filled or records dropped in Silver |
| **Uniqueness** | Duplicate primary key detection | Duplicates removed using Window functions |
| **Validity** | Date range and business rule validation | Records flagged as 'invalid' in monitoring |
| **Referential Integrity** | Ensures orders match existing customers | Orphaned orders moved to a 'quarantine' view |

---

## 🎯 Coding Standards & Guidelines

This project follows comprehensive coding standards and architectural guidelines outlined in the `VIBE_CODING.md` document. These guidelines ensure:

- **Consistent Code Quality**: Standardized formatting, type hints, and docstrings
- **Modular Architecture**: Clear separation of concerns and independent components
- **Production Readiness**: Enterprise-grade practices for reliability and scalability
- **Maintainability**: Clean, understandable code that's easy to extend
- **Testability**: Well-structured code that's easy to test

### Key Guidelines

1. **Architecture Philosophy**: Medallion Architecture (Bronze/Silver/Gold) with clear separation of concerns
2. **Coding Style**: Python Black formatter, Google-style docstrings, and mandatory type hints
3. **File Organization**: Modular structure with clear responsibilities
4. **Testing Strategy**: Comprehensive unit, integration, and end-to-end tests
5. **Deployment Best Practices**: Local development, cloud deployment, and scaling considerations

### How to Use

```bash
# View the complete coding guidelines
cat VIBE_CODING.md
```

These guidelines are enforced across all project components to ensure a cohesive, professional codebase that follows enterprise data engineering best practices.

---

## 🚀 Getting Started

### 1. Prerequisites
- **Python**: 3.8 or higher.
- **Java**: JDK 8 or 11 (required for PySpark).
- **Environment**: Windows, Linux, or macOS.

### 2. Installation
```bash
# Clone the repository
git clone <repository-url>
cd dw-dashboard

# Install dependencies
pip install -r requirements.txt
```

---

## 📖 Full Command Reference

### 🏗️ Batch Pipeline
Run the full Medallion workflow from raw data to curated insights.
```bash
# 1. Generate messy raw data (CSV/JSON/Parquet)
python src/batch/bootstrap.py

# 2. Execute ETL (Bronze -> Silver -> Gold)
python scripts/run_batch.py

# 3. Launch Batch Dashboard
python scripts/run_dashboard.py batch
```

### 🌊 Streaming Pipelines
Choose between the lightweight "Simple" broker or enterprise Spark streaming.

#### Option A: Simple Kafka (REST Proxy)
Perfect for local development and cloud deployments (e.g., Streamlit Cloud).
```bash
# Launch full stack (Server + Generator + Dashboard)
python scripts/run_streaming_simple.py

# Manually launch components (Optional)
python -m src.streaming.simple.server serve    # Start Broker
python src.streaming.simple.generator.py       # Start Generator
python scripts/run_dashboard.py simple         # Start Monitor
```

#### Option B: Spark Structured Streaming
Enterprise-grade streaming using file-based or Kafka sources.
```bash
# Start file-based streaming (reads from data/streaming/)
python scripts/run_streaming_spark.py --mode file

# Start Kafka-based streaming (requires local Kafka)
python scripts/run_streaming_spark.py --mode kafka

# Start Real-Time Dashboard
python scripts/run_dashboard.py spark
```

### 🔍 Data Inspection & Maintenance
Utilities to peek under the hood and verify the environment.
```bash
# Inspect Simple Kafka broker state
python scripts/inspect_simple_kafka.py topics     # List topics
python scripts/inspect_simple_kafka.py messages   # View latest messages
python scripts/inspect_simple_kafka.py offsets    # View consumer progress

# Run environment pre-flight checks
python tests/test_environment.py

# Run dashboard E2E compatibility tests
python tests/test_dashboard.py
```

---

## ⚖️ License
Distributed under the MIT License. See `LICENSE` for more information.
