# Real-Time E-Commerce Data Streaming Pipeline

A comprehensive real-time data engineering pipeline using **Spark Structured Streaming** for continuous e-commerce data processing. This system demonstrates enterprise-grade streaming analytics, fraud detection, and live dashboards.

## 🚀 Quick Start

### **Option 1: All-in-One (Recommended)**
```bash
# Start everything with one command
python run_realtime_pipeline.py --with-dashboard

# Access dashboards:
# - Real-time: http://localhost:8502
# - Batch ETL: http://localhost:8501
```

### **Option 2: Step-by-Step Manual Setup**
```bash
# Terminal 1: Generate streaming events
python streaming_data_generator.py --interval 2.0

# Terminal 2: Process the stream
python streaming_pipeline.py --mode file

# Terminal 3: Real-time dashboard
streamlit run app_realtime.py --server.port 8502
```

### **Option 3: Test Setup First**
```bash
# Verify everything is working
python test_streaming_setup.py

# Then run the pipeline
python run_realtime_pipeline.py --with-dashboard
```

---

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Data Generator │───▶│ Streaming Files  │───▶│ Spark Streaming │
│  (Events)       │    │ (JSON)           │    │ Pipeline        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Real-Time       │◀───│ Memory Tables    │◀───│ Stream Processing│
│ Dashboard       │    │ (Live Data)      │    │ & Analytics     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │ Medallion Layers │◀───│ Batch Writes    │
                       │ (Bronze/Silver)  │    │ (Checkpointed)  │
                       └──────────────────┘    └─────────────────┘
```

### **Data Flow**
1. **Generator** → JSON files in `data/streaming/orders/`
2. **Spark Streaming** → Monitors directory for new files
3. **Processing** → Clean, enrich, validate, score for fraud
4. **Outputs**:
   - **Bronze**: `lake/bronze/orders_streaming/` (raw data)
   - **Silver**: `lake/silver/orders_enriched_streaming/` (processed)
   - **Memory**: In-memory tables for dashboard access
   - **Console**: Fraud alerts printed to terminal

---

## 📦 Components

### **1. Streaming Data Generator** (`streaming_data_generator.py`)
- **Purpose**: Generates continuous e-commerce events (orders, customers, order items)
- **Modes**: File-based streaming (default) or Kafka streaming
- **Features**:
  - Realistic order patterns with fraud scenarios
  - Configurable event frequency and duration
  - Multiple output formats (JSON files or Kafka topics)
  - Graceful error handling and encoding support

### **2. Streaming ETL Pipeline** (`streaming_pipeline.py`)
- **Purpose**: Real-time data processing using Spark Structured Streaming
- **Features**:
  - **Stream Ingestion**: Monitors file directories or Kafka topics
  - **Real-Time Transformations**: Data cleaning, enrichment, validation
  - **Windowed Analytics**: Revenue aggregations over time windows
  - **Fraud Detection**: Real-time suspicious pattern identification
  - **Fault Tolerance**: Checkpointing for exactly-once processing
  - **Multi-Layer Writes**: Bronze (raw), Silver (processed), Memory (dashboard)

### **3. Real-Time Dashboard** (`app_realtime.py`)
- **Purpose**: Live visualization of streaming data and analytics
- **Features**:
  - **Auto-Refresh**: Configurable refresh intervals (5-60 seconds)
  - **Real-Time Metrics**: Revenue, order count, customer analytics
  - **Fraud Monitoring**: Live fraud alerts and scoring
  - **Order Stream**: Recent orders with status distribution
  - **Fallback Data**: Reads from Bronze layer if memory tables unavailable
  - **Cross-Version Compatibility**: Works with different Streamlit versions

### **4. Pipeline Orchestrator** (`run_realtime_pipeline.py`)
- **Purpose**: One-command startup for the entire streaming system
- **Features**:
  - **Dependency Checking**: Validates required packages
  - **Process Management**: Starts and monitors all components
  - **Error Handling**: Graceful shutdown and error reporting
  - **Flexible Configuration**: Customizable intervals and modes

---

## ⚙️ Configuration Options

### **Data Generator Options**
```bash
python streaming_data_generator.py \
  --mode file \                    # file or kafka
  --interval 2.0 \                 # seconds between events
  --duration 300 \                 # run for 5 minutes (optional)
  --kafka-servers localhost:9092   # for Kafka mode
```

### **Streaming Pipeline Options**
```bash
python streaming_pipeline.py \
  --mode file \                           # file or kafka
  --input-path data/streaming/orders \    # input directory
  --kafka-servers localhost:9092 \        # Kafka servers
  --kafka-topic ecommerce_orders          # Kafka topic
```

### **Full Pipeline Options**
```bash
python run_realtime_pipeline.py \
  --with-dashboard \               # start dashboard too
  --generator-interval 1.5 \       # faster event generation
  --generator-duration 600 \       # run for 10 minutes
  --mode file                      # streaming mode
```

---

## 📊 Dashboard Features

### **Real-Time Metrics Tab**
- **Live KPIs**: Orders per minute, revenue, average order value, unique customers
- **Revenue Trends**: Time-series charts showing revenue over rolling windows
- **Order Volume**: Bar charts of order counts per time window
- **Fallback Data**: Shows Bronze layer data if streaming tables unavailable

### **Fraud Detection Tab**
- **Fraud Alerts**: Real-time suspicious order identification
- **Risk Scoring**: Fraud scores based on amount, velocity, and patterns
- **Alert Distribution**: Histograms of fraud score distributions
- **Timeline View**: Scatter plots showing fraud alerts over time

### **Recent Orders Tab**
- **Order Stream**: Latest 100 orders with full details
- **Status Distribution**: Pie charts of order status breakdown
- **Summary Metrics**: Total orders, revenue, and averages
- **Real-Time Updates**: Auto-refreshing order feed

---

## 🔧 Technical Details

### **Streaming Windows**
- **Tumbling Windows**: 1-minute non-overlapping windows for revenue aggregation
- **Watermarking**: 10-minute watermark to handle late-arriving data
- **Trigger Intervals**: 10-30 second micro-batch processing

### **Fraud Detection Algorithm**
```python
# Scoring criteria (real-time, per-order basis)
amount_score = {
    "> $5000": 4 points,
    "> $3000": 3 points, 
    "> $2000": 2 points
}

status_score = {
    "cancelled/refunded": 1 point,
    "completed": 0 points
}

# Alert threshold: fraud_score >= 3
```

### **Fault Tolerance**
- **Checkpointing**: `checkpoints/streaming/` for exactly-once processing
- **Watermarking**: Handles late data up to 10 minutes
- **Error Recovery**: Automatic restart from last checkpoint
- **Graceful Shutdown**: Ctrl+C stops all processes cleanly

---

## 🛠️ Installation & Setup

### **Prerequisites**
- **Python 3.8+**
- **Java 8+** (required for PySpark)
- Set `JAVA_HOME` environment variable

### **Install Dependencies**
```bash
pip install -r requirements.txt
```

### **Test Setup**
```bash
python test_streaming_setup.py
```

### **Create Directories** (automatic)
The pipeline automatically creates:
```
data/streaming/orders/
data/streaming/customers/
checkpoints/streaming/
lake/bronze/orders_streaming/
lake/silver/orders_enriched_streaming/
```

---

## 🎯 What You'll See

### **Expected Timeline**
1. **0-10s**: Data generator creates JSON files in `data/streaming/orders/`
2. **10-30s**: Streaming pipeline detects files and starts processing
3. **30-60s**: First micro-batch writes to Bronze layer
4. **60-90s**: Memory tables get populated for dashboard
5. **90s+**: Dashboard shows real-time data

### **Success Indicators**
- ✅ Files appear in `data/streaming/orders/` directory
- ✅ Pipeline console shows "Batch X" processing messages
- ✅ Dashboard shows data (either in memory tables or Bronze layer)
- ✅ Spark UI shows active streaming queries at http://localhost:4040

### **Console Output Examples**
```bash
# Data Generator
[START] Starting order stream...
   Generated 10 orders...
   Generated 20 orders...

# Streaming Pipeline
[START] Streaming pipeline started!
Batch: 0
Input total rows: 5
Processed rows: 5

# Dashboard
✅ Found 15 orders in Bronze layer
Revenue (Last Minute): $2,450.75
```

---

## 🔍 Troubleshooting

### **Common Issues**

#### **Dashboard shows "⏳ Waiting for streaming data..."**
**Possible causes:**
1. Pipeline not running - Check Terminal 2
2. Data generator not running - Check Terminal 1  
3. Not enough time passed - Wait 30-60 seconds
4. Memory tables not visible - Use "Recent Orders" tab instead

**Solutions:**
```bash
# Check if files are being created
ls -la data/streaming/orders/

# Check if pipeline is processing
ls -la lake/bronze/orders_streaming/

# Check Spark UI
# Visit http://localhost:4040
```

#### **No data in dashboard**
**Debug steps:**
1. Are files appearing in `data/streaming/orders/`?
2. Is Spark processing them? (Check console output)
3. Are there any errors in the pipeline terminal?
4. Check Spark UI at http://localhost:4040 for query status

#### **Memory issues**
```bash
# Reduce event frequency
python streaming_data_generator.py --interval 5.0

# Or increase Spark memory in spark_session.py
.config("spark.driver.memory", "4g")
```

#### **Kafka connection issues**
```bash
# Install Kafka dependencies
pip install kafka-python

# Check Kafka is running
kafka-topics.sh --list --bootstrap-server localhost:9092

# Use file mode as fallback
python run_realtime_pipeline.py --mode file
```

### **Normal Spark Warnings (Harmless)**
- `"Closing down clientserver connection"` - Normal Spark shutdown
- `"ProcfsMetricsGetter: Exception"` - Harmless Windows warning
- `"Service 'SparkUI' could not bind on port 4040"` - Uses port 4041 instead

### **Streamlit Compatibility**
- **Streamlit < 1.18.0**: Works but without Spark session caching
- **Streamlit >= 1.18.0**: Full functionality with session caching
- **Auto-refresh**: Multiple fallback methods for different versions

---

## 📈 Business Value & Use Cases

### **Real-Time Insights**
- **Immediate Visibility**: See revenue and order trends as they happen
- **Operational Monitoring**: Track system health and data flow
- **Customer Behavior**: Real-time customer activity patterns

### **Fraud Prevention**
- **Instant Alerts**: Detect suspicious orders within seconds
- **Risk Scoring**: Quantitative fraud assessment for prioritization
- **Pattern Recognition**: Identify unusual spending or velocity patterns

### **Scalability Demonstration**
- **Stream Processing**: Handles continuous data ingestion
- **Horizontal Scaling**: Spark can scale across multiple nodes
- **Fault Tolerance**: Production-ready error handling and recovery

---

## 🔮 Extensions & Next Steps

### **Advanced Features to Add**
- **Machine Learning**: Real-time ML model scoring for fraud detection
- **Complex Event Processing**: Multi-stream joins and correlations
- **Alerting**: Email/Slack notifications for critical events
- **Metrics Export**: Prometheus/Grafana integration
- **Data Quality**: Real-time data quality monitoring

### **Production Enhancements**
- **Kubernetes Deployment**: Container orchestration
- **Schema Registry**: Centralized schema management
- **Security**: Authentication, encryption, access controls
- **Monitoring**: Comprehensive observability stack
- **Testing**: Stream processing unit and integration tests

---

## 📚 Learning Outcomes

This streaming pipeline demonstrates:

1. **Spark Structured Streaming**: Production streaming patterns and exactly-once processing
2. **Real-Time Analytics**: Windowed aggregations and live dashboards  
3. **Fault Tolerance**: Checkpointing and watermarking for reliable processing
4. **System Integration**: Multi-component pipeline orchestration
5. **Error Handling**: Graceful degradation and recovery mechanisms
6. **Performance**: Optimized streaming configurations and memory management
7. **Monitoring**: Live system observability and alerting capabilities

Perfect for showcasing modern data engineering skills in real-time processing, streaming analytics, and production system design! 🚀

---

## 🛑 Stopping the Pipeline

### **All-in-One Script**
Press `Ctrl+C` in the terminal - it handles cleanup automatically.

### **Manual Setup**
Press `Ctrl+C` in each terminal:
1. Data generator (Terminal 1)
2. Streaming pipeline (Terminal 2)
3. Dashboard (Terminal 3)

### **Force Stop** (if needed)
```bash
# Windows
taskkill /f /im python.exe

# Linux/Mac
pkill -f "streaming_"
```

---

## 📝 Quick Reference

### **Key Commands**
```bash
# Test setup
python test_streaming_setup.py

# All-in-one
python run_realtime_pipeline.py --with-dashboard

# Manual components
python streaming_data_generator.py --interval 2.0
python streaming_pipeline.py --mode file
streamlit run app_realtime.py --server.port 8502
```

### **Key URLs**
- **Real-time Dashboard**: http://localhost:8502
- **Batch Dashboard**: http://localhost:8501
- **Spark UI**: http://localhost:4040 (or 4041, 4042...)

### **Key Directories**
- **Input**: `data/streaming/orders/`
- **Bronze**: `lake/bronze/orders_streaming/`
- **Silver**: `lake/silver/orders_enriched_streaming/`
- **Checkpoints**: `checkpoints/streaming/`

Happy streaming! 🎉

# 1. Install dependencies
pip install kafka-python

# 2. Set up Kafka (if not already running)
# Download and start Kafka, or use Docker

# 3. Set up topics and run pipeline
python setup_kafka.py  # Interactive setup
python run_kafka_pipeline.py --with-dashboard

# Access dashboard at http://localhost:8502
# Run the Windows-specific Kafka setup
python start_kafka_windows.py

# Choose option 1: Start Kafka with Docker
