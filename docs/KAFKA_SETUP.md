# Kafka Setup Guide for Real-Time Streaming Pipeline

This guide helps you set up Apache Kafka for the e-commerce real-time streaming pipeline.

## 🚀 Quick Start (All-in-One)

```bash
# 1. Install dependencies
pip install kafka-python

# 2. Set up Kafka topics
python setup_kafka.py

# 3. Run the complete Kafka pipeline
python run_kafka_pipeline.py --with-dashboard
```

**Access Points:**
- **Real-time Dashboard**: http://localhost:8502
- **Kafka Monitor**: `python kafka_monitor.py --once`

---

## 📋 Prerequisites

### **1. Java Installation**
Kafka requires Java 8 or later:

```bash
# Check Java version
java -version

# If not installed, download from:
# https://adoptopenjdk.net/ or https://www.oracle.com/java/
```

### **2. Python Dependencies**
```bash
# Install required packages
pip install -r requirements.txt

# Or install individually
pip install kafka-python pyspark streamlit plotly pandas
```

---

## 🏗️ Kafka Installation

### **Option 1: Download Apache Kafka**

1. **Download Kafka**:
   ```bash
   # Download Kafka 2.13-3.6.0 (or latest)
   wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
   tar -xzf kafka_2.13-3.6.0.tgz
   cd kafka_2.13-3.6.0
   ```

2. **Start Kafka Services**:
   ```bash
   # Terminal 1: Start Zookeeper
   bin/zookeeper-server-start.sh config/zookeeper.properties
   
   # Terminal 2: Start Kafka Server
   bin/kafka-server-start.sh config/server.properties
   ```

### **Option 2: Docker (Recommended for Development)**

1. **Create docker-compose.yml**:
   ```yaml
   version: '3.8'
   services:
     zookeeper:
       image: confluentinc/cp-zookeeper:latest
       environment:
         ZOOKEEPER_CLIENT_PORT: 2181
         ZOOKEEPER_TICK_TIME: 2000
       ports:
         - "2181:2181"
   
     kafka:
       image: confluentinc/cp-kafka:latest
       depends_on:
         - zookeeper
       ports:
         - "9092:9092"
       environment:
         KAFKA_BROKER_ID: 1
         KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
         KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
         KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
   ```

2. **Start with Docker**:
   ```bash
   docker-compose up -d
   ```

### **Option 3: Confluent Platform**
```bash
# Install Confluent CLI
curl -sL --http1.1 https://cnfl.io/cli | sh -s -- latest

# Start Confluent Platform
confluent local services start
```

---

## 🔧 Pipeline Setup

### **1. Verify Kafka Installation**
```bash
# Test Kafka connectivity
python setup_kafka.py
# Choose option 1: Check Kafka server status
```

### **2. Create Topics**
```bash
# Automatic setup (recommended)
python setup_kafka.py
# Choose option 2: Set up e-commerce topics

# Manual setup (alternative)
kafka-topics.sh --create --topic ecommerce_orders --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics.sh --create --topic ecommerce_customers --partitions 2 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics.sh --create --topic ecommerce_order_items --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics.sh --create --topic ecommerce_fraud_alerts --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

### **3. Test Connectivity**
```bash
# Run connectivity test
python setup_kafka.py
# Choose option 4: Test connectivity
```

---

## 🚀 Running the Pipeline

### **Option 1: All-in-One Orchestrator (Recommended)**
```bash
# Start everything with one command
python run_kafka_pipeline.py --with-dashboard

# Custom configuration
python run_kafka_pipeline.py \
  --kafka-servers localhost:9092 \
  --generator-interval 1.5 \
  --generator-duration 600 \
  --dashboard-port 8502
```

### **Option 2: Manual Step-by-Step**
```bash
# Terminal 1: Start data generator
python streaming_data_generator.py \
  --mode kafka \
  --kafka-servers localhost:9092 \
  --interval 2.0 \
  --topic ecommerce_orders

# Terminal 2: Start streaming pipeline
python streaming_pipeline.py \
  --mode kafka \
  --kafka-servers localhost:9092 \
  --kafka-topic ecommerce_orders

# Terminal 3: Start dashboard
streamlit run app_realtime.py --server.port 8502
```

---

## 📊 Monitoring & Management

### **1. Kafka Topic Monitoring**
```bash
# Real-time monitoring
python kafka_monitor.py --interval 30

# Single status check
python kafka_monitor.py --once

# Monitor with message sampling
python kafka_monitor.py --sample-messages
```

### **2. Pipeline Performance Monitoring**
```bash
# Monitor streaming performance
python monitor_streaming.py --interval 30

# Single performance check
python monitor_streaming.py --once
```

### **3. Kafka CLI Commands**
```bash
# List topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe topic
kafka-topics.sh --describe --topic ecommerce_orders --bootstrap-server localhost:9092

# Consumer groups
kafka-consumer-groups.sh --list --bootstrap-server localhost:9092

# Delete topic (if needed)
kafka-topics.sh --delete --topic ecommerce_orders --bootstrap-server localhost:9092
```

---

## 🎯 Expected Performance

### **Kafka vs File-Based Streaming**

| Metric | File-Based | Kafka-Based |
|--------|------------|-------------|
| **Throughput** | ~50 events/sec | ~1000+ events/sec |
| **Latency** | 30-60 seconds | 1-5 seconds |
| **Scalability** | Single node | Multi-node cluster |
| **Fault Tolerance** | File system | Replication + Partitioning |
| **Monitoring** | File counts | Rich metrics |

### **Resource Usage**
- **Memory**: 2-4 GB (Kafka + Spark + Dashboard)
- **CPU**: 2-4 cores recommended
- **Disk**: 1-5 GB for logs and checkpoints
- **Network**: Minimal (localhost communication)

---

## 🔍 Troubleshooting

### **Common Issues**

#### **1. "No brokers available" Error**
```bash
# Check if Kafka is running
jps | grep Kafka

# Check Kafka logs
tail -f kafka_2.13-3.6.0/logs/server.log

# Restart Kafka
bin/kafka-server-stop.sh
bin/kafka-server-start.sh config/server.properties
```

#### **2. "Topic does not exist" Error**
```bash
# List existing topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Create missing topics
python setup_kafka.py
# Choose option 2
```

#### **3. Consumer Lag Issues**
```bash
# Check consumer groups
kafka-consumer-groups.sh --list --bootstrap-server localhost:9092

# Check lag
kafka-consumer-groups.sh --describe --group streaming-pipeline --bootstrap-server localhost:9092

# Reset consumer group (if needed)
kafka-consumer-groups.sh --reset-offsets --to-earliest --group streaming-pipeline --topic ecommerce_orders --bootstrap-server localhost:9092
```

#### **4. Dashboard Shows No Data**
```bash
# Check if topics have messages
python kafka_monitor.py --once

# Check streaming pipeline logs
# Look for processing messages in Terminal 2

# Verify Spark is reading from Kafka
# Check Spark UI at http://localhost:4040
```

#### **5. High Memory Usage**
```bash
# Reduce batch size in streaming_pipeline.py
# Edit: .option("maxOffsetsPerTrigger", 1000)  # Reduce from default

# Increase Kafka retention cleanup
# Edit config/server.properties:
# log.retention.hours=1
# log.segment.bytes=104857600
```

---

## 🔧 Configuration Options

### **Kafka Server Configuration** (`config/server.properties`)
```properties
# Performance tuning
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Log retention (adjust based on disk space)
log.retention.hours=24
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

# Replication (for production)
default.replication.factor=3
min.insync.replicas=2
```

### **Producer Configuration** (in `streaming_data_generator.py`)
```python
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    batch_size=16384,        # Batch size for better throughput
    linger_ms=10,           # Wait time for batching
    compression_type='gzip', # Compression
    acks='all'              # Wait for all replicas
)
```

### **Consumer Configuration** (in `streaming_pipeline.py`)
```python
.option("kafka.bootstrap.servers", "localhost:9092")
.option("subscribe", "ecommerce_orders")
.option("startingOffsets", "latest")
.option("maxOffsetsPerTrigger", 1000)  # Limit messages per batch
.option("failOnDataLoss", "false")     # Handle missing data gracefully
```

---

## 📈 Scaling Considerations

### **Single Node (Development)**
- 1 Kafka broker
- 1-3 partitions per topic
- Local Spark execution

### **Multi-Node (Production)**
- 3+ Kafka brokers
- 6+ partitions per topic
- Spark cluster with multiple workers
- Load balancer for dashboard

### **Cloud Deployment**
- **AWS**: MSK (Managed Streaming for Kafka) + EMR (Spark)
- **Azure**: Event Hubs + HDInsight
- **GCP**: Cloud Pub/Sub + Dataproc

---

## 🎉 Success Indicators

When everything is working correctly, you should see:

1. **Kafka Monitor**: All topics showing active message flow
2. **Dashboard**: Real-time metrics updating every 15 seconds
3. **Console Logs**: Batch processing messages from Spark
4. **No Errors**: Clean logs without connection timeouts

**Example Success Output:**
```
📊 Kafka Status - 14:30:15
================================================================================
Topic                     Partitions   Messages     Rate/min     Status
--------------------------------------------------------------------------------
ecommerce_orders          3            1,247        45.2         ✅ Active
ecommerce_customers       2            156          5.1          ✅ Active
ecommerce_order_items     3            3,891        142.7        ✅ Active
ecommerce_fraud_alerts    1            23           0.8          ✅ Active
```

Happy streaming with Kafka! 🚀