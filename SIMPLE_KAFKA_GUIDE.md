# Simple Kafka Dashboard Guide

## Problem Solved ✅

The Streamlit dashboard was showing "Simple Kafka server not available" because it was looking for the message broker server that didn't exist in its local environment.

## What's New

1. **Re-organized Structure** - All scripts are now in the `scripts/` directory for better organization.
2. **Consolidated Runner** - `scripts/run_simple_kafka_pipeline.py` starts everything you need in one command.
3. **Enhanced Data Generator** - Now in `streaming/simple_kafka/data_generator_legacy.py`.

## How to Use

### Option 1: Run Complete Pipeline (Recommended)
```bash
python scripts/run_simple_kafka_pipeline.py
```
This starts:
- Simple Kafka server (in-memory message broker)
- Data generator (creates test e-commerce data)
- Streamlit dashboard at http://localhost:8502

### Option 2: Run Components Separately

1. **Start the server:**
```bash
# The server is usually started by the generator or dashboard if not running
python streaming/simple_kafka/server_legacy.py
```

2. **Generate test data:**
```bash
python streaming/simple_kafka/data_generator_legacy.py --burst --duration 30
```

3. **Run the dashboard:**
```bash
streamlit run dashboards/app_simple_kafka.py --server.port 8502
```

## What You'll See

The dashboard shows:
- **Server Status** - Monitor Simple Kafka broker and topics
- **Real-Time Metrics** - Live revenue, orders, and customer analytics  
- **Orders** - Recent orders with filtering capabilities
- **Customers** - Customer events and segments
- **Order Items** - Product categories and order details
- **Fraud Alerts** - Suspicious pattern detection

## Data Generated

The system creates realistic e-commerce data:
- **Orders**: 1 per cycle with realistic amounts and statuses
- **Customers**: 20% chance per cycle, creates new or updates existing
- **Order Items**: 1-5 items per order with product details
- **Fraud Alerts**: Generated for ~15% of orders (higher for high-value orders)

## Stopping the Pipeline

Press `Ctrl+C` in the terminal where you ran the pipeline to stop all components gracefully.

## Troubleshooting

If you see "Simple Kafka server not available":
1. Run the complete pipeline with `python scripts/run_simple_kafka_pipeline.py`
2. Wait a few seconds for data generation before viewing the dashboard
3. Ensure you have the required dependencies: `pip install -r requirements.txt`
