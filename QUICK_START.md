# Quick Start Guide - Enhanced Simple Kafka

## 🚀 Running the Enhanced Simple Kafka Pipeline

The enhanced data generator now populates **all 4 Kafka topics**:
- 🛒 `ecommerce_orders` - Order events
- 👥 `ecommerce_customers` - Customer data
- 📦 `ecommerce_order_items` - Order line items
- 🚨 `ecommerce_fraud_alerts` - Fraud detection alerts

## Option 1: Run Everything at Once (Recommended)

```bash
python run_streaming.py --mode simple --with-dashboard
```

This will:
1. Start the Simple Kafka server
2. Launch the enhanced data generator with initial burst
3. Start the streaming pipeline
4. Open the dashboard at http://localhost:8502

## Option 2: Run Components Separately

### Step 1: Start Data Generator
```bash
python streaming_data_generator_simple.py --burst --interval 2.0
```

The `--burst` flag generates 20 events immediately to populate all topics quickly.

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
python streaming_data_generator_simple.py --burst --interval 1.0

# Slower generation (every 5 seconds)
python streaming_data_generator_simple.py --burst --interval 5.0
```

### Run for Limited Time
```bash
# Run for 60 seconds then stop
python streaming_data_generator_simple.py --burst --interval 2.0 --duration 60
```

### Generate More Initial Data
Edit `streaming_data_generator_simple.py` and change the burst count:
```python
# In main() function, change:
for i in range(20):  # Change 20 to 50 or 100
    generator.generate_and_send_events()
```

## 📈 Monitoring

### View Generation Statistics
The data generator prints statistics every 10 events:
```
📊 Generated: Orders=10, Customers=2, Items=28, Fraud Alerts=1
📊 Generated: Orders=20, Customers=5, Items=54, Fraud Alerts=3
```

### Check Topic Status
In the dashboard, go to **🔄 Server Status** tab to see:
- Total messages per topic
- Active vs empty topics
- Partition information

## 🐛 Troubleshooting

### All Topics Show "⚠️ Empty"
**Solution**: Run the data generator with `--burst` flag:
```bash
python streaming_data_generator_simple.py --burst
```

### Only Orders Topic Has Data
**Solution**: You're using the old data generator. Make sure you're using the enhanced version:
```bash
# Check file size - enhanced version is ~10KB+
ls -lh streaming_data_generator_simple.py

# Re-run with burst
python streaming_data_generator_simple.py --burst
```

### Dashboard Shows "No Data Available"
**Solution**: 
1. Ensure data generator is running
2. Wait 5-10 seconds for data to generate
3. Refresh the dashboard (it auto-refreshes every 10 seconds)

### Import Errors
**Solution**: Make sure you're running from the project root:
```bash
cd /path/to/dw-dashborad
python streaming_data_generator_simple.py --burst
```

## 🎉 Success Indicators

You'll know everything is working when you see:

1. **Data Generator Output**:
   ```
   🚀 Enhanced Simple Kafka Data Generator
   ⚡ Generating initial burst of data...
   📊 Generated: Orders=5, Customers=1, Items=12, Fraud Alerts=0
   📊 Generated: Orders=10, Customers=2, Items=26, Fraud Alerts=1
   ✅ Initial burst complete
   ```

2. **Dashboard Status Tab**:
   ```
   Topic                        Partitions  Messages  Status
   🛒 ecommerce_orders          3           148       ✅ Active
   👥 ecommerce_customers       2           20        ✅ Active
   📦 ecommerce_order_items     3           30        ✅ Active
   🚨 ecommerce_fraud_alerts    1           10        ✅ Active
   ```

3. **All Dashboard Tabs Show Data**: Each tab displays relevant metrics and visualizations

## 📚 Next Steps

- Explore the **📊 Real-Time Metrics** tab for business insights
- Check **🚨 Fraud Alerts** tab for suspicious transactions
- Use **📦 Order Items** tab to analyze product categories
- Review **👥 Customers** tab for customer segmentation

Enjoy your enhanced Simple Kafka streaming pipeline! 🎊
