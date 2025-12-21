# Quick Start Guide - Enhanced Simple Kafka

## 🚀 Running the Enhanced Simple Kafka Pipeline

The enhanced data generator now populates **all 4 Kafka topics**:

- 🛒 `ecommerce_orders` - Order events
- 👥 `ecommerce_customers` - Customer data
- 📦 `ecommerce_order_items` - Order line items
- 🚨 `ecommerce_fraud_alerts` - Fraud detection alerts

## Option 1: Run Everything at Once (Recommended)

```bash
python scripts/run_streaming.py --mode simple --with-dashboard
```

OR

```bash
python scripts/run_simple_kafka_pipeline.py
```

This will:

1. Start the Simple Kafka server
2. Launch the enhanced data generator with initial burst
3. Start the streaming pipeline
4. Open the dashboard at http://localhost:8502

## Option 2: Run Components Separately

### Step 1: Start Data Generator

```bash
python streaming/simple_kafka/data_generator_legacy.py --burst --interval 2.0
```

### Step 2: Open Dashboard

```bash
streamlit run dashboards/app_simple_kafka.py --server.port 8502
```

### Step 3: View Dashboard

Open your browser to: http://localhost:8502

## 📊 Dashboard Features

The enhanced dashboard now has **6 tabs**:

1. **🔄 Server Status** - View all topics and their message counts
2. **📊 Real-Time Metrics** - Live revenue, orders, and trends
3. **🛒 Orders** - Recent orders with filtering
4. **👥 Customers** - Customer events and segments
5. **📦 Order Items** - Product categories and order details
6. **🚨 Fraud Alerts** - Fraud detection results and analysis

## 🎯 What to Expect

After starting the data generator with `--burst`:

- **ecommerce_orders**: ~20+ messages (1 per event cycle)
- **ecommerce_customers**: ~4-6 messages (20% chance per cycle)
- **ecommerce_order_items**: ~40-100 messages (2-5 items per order)
- **ecommerce_fraud_alerts**: ~3-8 messages (15-40% chance based on order value)

## 🔧 Customization

### Adjust Generation Speed

```bash
# Faster generation (every 1 second)
python streaming/simple_kafka/data_generator_legacy.py --burst --interval 1.0
```

### Run for Limited Time

```bash
# Run for 60 seconds then stop
python streaming/simple_kafka/data_generator_legacy.py --burst --interval 2.0 --duration 60
```
