# Kafka Setup Guide for Windows

This guide provides step-by-step instructions to set up Apache Kafka on Windows for the real-time streaming pipeline.

## 🚀 Quick Start (Docker - Recommended)

### **Prerequisites**
1. **Docker Desktop** installed and running
2. **Python 3.8+** with pip
3. **Git** (optional)

### **Step 1: Install Dependencies**
```bash
# Install Python packages
pip install kafka-python pyspark streamlit plotly pandas
```

### **Step 2: Start Kafka with Docker**
```bash
# Run the Windows Kafka setup script
python start_kafka_windows.py

# Choose option 1: Start Kafka with Docker
```

This will:
- Create a `docker-compose.yml` file
- Start Zookeeper and Kafka containers
- Start Kafka UI for monitoring
- Wait for services to be ready

### **Step 3: Test Kafka**
```bash
# Test basic connectivity
python test_kafka_simple.py
```

### **Step 4: Run the Pipeline**
```bash
# Run complete Kafka streaming pipeline
python run_kafka_pipeline.py --with-dashboard

# Access dashboard at: http://localhost:8502
# Access Kafka UI at: http://localhost:8080
```

---

## 🐳 Docker Setup (Detailed)

### **Install Docker Desktop**
1. Download from: https://www.docker.com/products/docker-desktop
2. Install and restart your computer
3. Start Docker Desktop
4. Verify: `docker --version`

### **Start Kafka Services**
```bash
# Option 1: Use our setup script
python start_kafka_windows.py

# Option 2: Manual Docker Compose
docker-compose up -d

# Check status
docker ps
```

### **Expected Output**
```
CONTAINER ID   IMAGE                             COMMAND                  CREATED         STATUS                   PORTS                                        NAMES
abc123def456   confluentinc/cp-kafka:7.4.0       "/etc/confluent/dock…"   2 minutes ago   Up 2 minutes (healthy)   0.0.0.0:9092->9092/tcp, 0.0.0.0:9101->9101/tcp   kafka
def456ghi789   confluentinc/cp-zookeeper:7.4.0   "/etc/confluent/dock…"   2 minutes ago   Up 2 minutes (healthy)   0.0.0.0:2181->2181/tcp                           zookeeper
ghi789jkl012   provectuslabs/kafka-ui:latest     "/bin/sh -c 'java --…"   2 minutes ago   Up 2 minutes             0.0.0.0:8080->8080/tcp                           kafka-ui
```

### **Access Points**
- **Kafka Broker**: localhost:9092
- **Kafka UI**: http://localhost:8080
- **Zookeeper**: localhost:2181

---

## 💻 Native Windows Setup (Alternative)

### **Download Kafka**
1. Go to: https://kafka.apache.org/downloads
2. Download: `kafka_2.13-3.6.0.tgz`
3. Extract to: `C:\kafka_2.13-3.6.0\`

### **Start Services**

**Terminal 1 (Administrator) - Zookeeper:**
```cmd
cd C:\kafka_2.13-3.6.0
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```

**Terminal 2 (Administrator) - Kafka:**
```cmd
cd C:\kafka_2.13-3.6.0
bin\windows\kafka-server-start.bat config\server.properties
```

### **Test Installation**
```cmd
# Terminal 3 - Test
cd C:\kafka_2.13-3.6.0

# Create test topic
bin\windows\kafka-topics.bat --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# List topics
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

---

## 🧪 Testing & Verification

### **Test 1: Basic Connectivity**
```bash
python test_kafka_simple.py
```

**Expected Output:**
```
🧪 Testing Kafka Basic Functionality
==================================================
📤 Testing Kafka Producer...
✅ Message sent successfully!
   Topic: test_topic
   Partition: 0
   Offset: 0

📥 Testing Kafka Consumer...
✅ Test message received!
   Content: {'id': '...', 'timestamp': '...', 'message': 'Hello Kafka!', 'test': True}

🎉 Kafka connectivity test PASSED!
```

### **Test 2: E-commerce Topics**
```bash
python setup_kafka.py
# Choose option 2: Set up e-commerce topics
```

### **Test 3: Full Pipeline**
```bash
python run_kafka_pipeline.py --with-dashboard
```

---

## 🔧 Troubleshooting

### **Common Issues**

#### **1. "No brokers available" Error**
```bash
# Check if Kafka is running
docker ps | findstr kafka

# Check Kafka logs
docker-compose logs kafka

# Restart Kafka
docker-compose restart kafka
```

#### **2. Docker Desktop Not Starting**
- Restart Docker Desktop
- Check Windows features: Hyper-V, WSL2
- Run as Administrator
- Check antivirus software

#### **3. Port Already in Use**
```bash
# Check what's using port 9092
netstat -ano | findstr :9092

# Kill process if needed (replace PID)
taskkill /PID <PID> /F
```

#### **4. kafka-python Import Error**
```bash
# Install kafka-python
pip install kafka-python

# If still failing, try:
pip uninstall kafka-python
pip install kafka-python==2.0.2
```

#### **5. Memory Issues**
Edit `docker-compose.yml` and add:
```yaml
kafka:
  environment:
    KAFKA_HEAP_OPTS: "-Xmx512M -Xms512M"
```

### **Verification Commands**

```bash
# Check Docker containers
docker ps

# Check Kafka topics
python kafka_monitor.py --once

# Check streaming pipeline
python monitor_streaming.py --once

# Test connectivity
python test_kafka_simple.py
```

---

## 📊 Performance Tuning

### **Docker Resource Limits**
In Docker Desktop settings:
- **Memory**: 4GB minimum, 8GB recommended
- **CPU**: 2 cores minimum, 4 cores recommended
- **Disk**: 20GB free space

### **Kafka Configuration**
Edit `docker-compose.yml`:
```yaml
kafka:
  environment:
    # Increase batch size for better throughput
    KAFKA_BATCH_SIZE: 16384
    KAFKA_LINGER_MS: 10
    
    # Increase buffer sizes
    KAFKA_SOCKET_SEND_BUFFER_BYTES: 102400
    KAFKA_SOCKET_RECEIVE_BUFFER_BYTES: 102400
    
    # Reduce log retention for development
    KAFKA_LOG_RETENTION_HOURS: 24
```

---

## 🎯 Expected Performance

### **Development Setup (Docker)**
- **Throughput**: 1,000-5,000 messages/second
- **Latency**: 1-10 milliseconds
- **Memory Usage**: 1-2 GB
- **Startup Time**: 30-60 seconds

### **Production Considerations**
- Use dedicated Kafka cluster
- Increase replication factor to 3
- Use SSD storage
- Monitor with Kafka UI or JMX

---

## 🚀 Next Steps

Once Kafka is running successfully:

1. **Run the streaming pipeline**:
   ```bash
   python run_kafka_pipeline.py --with-dashboard
   ```

2. **Monitor the system**:
   ```bash
   python kafka_monitor.py --interval 30
   ```

3. **Access dashboards**:
   - **Streaming Dashboard**: http://localhost:8502
   - **Kafka UI**: http://localhost:8080

4. **Scale up** (optional):
   - Add more Kafka partitions
   - Increase Spark parallelism
   - Deploy to cloud (AWS MSK, Azure Event Hubs)

---

## 📞 Support

If you encounter issues:

1. **Check logs**: `docker-compose logs kafka`
2. **Restart services**: `docker-compose restart`
3. **Clean restart**: `docker-compose down && docker-compose up -d`
4. **Test connectivity**: `python test_kafka_simple.py`

The Windows setup should now work smoothly with Docker! 🎉