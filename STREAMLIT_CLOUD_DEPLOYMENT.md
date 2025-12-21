# Streamlit Cloud Deployment Guide

## Overview

This project is optimized for deployment on Streamlit Cloud, specifically the "Simple Kafka" real-time dashboard which uses an in-memory message broker to avoid external infrastructure dependencies.

## What's Changed

1. **Re-organized Structure** - The dashboard is now in `dashboards/app_simple_kafka.py`.
2. **Cloud Entry Point** - `dashboards/cloud_app.py` is the main entry point for Streamlit Cloud.
3. **Self-Contained** - The dashboard can initialize its own in-memory broker and generate demo data directly.

## Deployment Steps

### For Streamlit Cloud:

1. **Deploy using `dashboards/cloud_app.py`** as your main file.

2. **Repository structure:**
   ```
   dw-dashboard/
   ├── dashboards/
   │   ├── cloud_app.py        # Entry point for Cloud
   │   └── app_simple_kafka.py # Dashboard code
   ├── streaming/
   │   └── simple_kafka/
   │       └── server.py       # In-memory message broker
   ├── requirements.txt        # Dependencies (Streamlit >= 1.40.1)
   └── README.md
   ```

3. **In Streamlit Cloud settings:**
   - Main file path: `dashboards/cloud_app.py`
   - Python version: 3.9+ (recommended)

### How It Works on Cloud

1. **Dashboard loads** - Automatically initializes the Simple Kafka server.
2. **Go to "Server Status" tab** - Click "Initialize Simple Kafka Server" if needed.
3. **Click "Generate Sample Data"** - Populates topics with demo e-commerce data.
4. **View live dashboard** - All tabs show real-time updates from the in-memory broker.

### Local Development

For local development, use the runners in the `scripts/` directory:
```bash
# Start everything at once
python scripts/run_simple_kafka_pipeline.py
```

### Troubleshooting

**If you see "Simple Kafka server not available":**
1. Navigate to the **"Server Status"** tab.
2. Click **"Initialize Simple Kafka Server"**.
3. Click **"Generate Sample Data"**.
4. Data should now appear in the metrics and order tabs.
