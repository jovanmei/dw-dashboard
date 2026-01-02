"""
Simple test script to verify the real-time dashboard functions work correctly.
"""

import sys
import os
from pathlib import Path

# Add current directory to path
sys.path.append('.')

def test_fraud_detection():
    """Test fraud detection function."""
    print("🔍 Testing fraud detection function...")
    
    try:
        import pandas as pd
        from src.utils.analytics import detect_monthly_revenue_anomalies
        
        # Create test data with monthly revenue
        test_data = pd.DataFrame({
            'month': ['2023-01', '2023-02', '2023-03', '2023-04', '2023-05'],
            'total_revenue': [10000, 12000, 11500, 80000, 13000]  # Anomaly in April
        })
        
        anomalies = detect_monthly_revenue_anomalies(test_data)
        print(f"✅ Fraud detection completed")
        print(f"   Test months: {len(test_data)}")
        print(f"   Anomalies detected: {len(anomalies)}")
        
        if len(anomalies) > 0:
            print(f"   Anomaly months: {anomalies['month'].tolist()}")
        
        return True
        
    except Exception as e:
        print(f"❌ Fraud detection test failed: {e}")
        return False


def test_metrics_calculation():
    """Test metrics calculation function."""
    print("\n🔍 Testing metrics calculation function...")
    
    try:
        from src.utils.transformations import build_customer_metrics
        from pyspark.sql import SparkSession
        
        # Create Spark session for testing
        spark = SparkSession.builder \
            .appName("TestMetrics") \
            .master("local[1]") \
            .getOrCreate()
        
        # Create test data
        orders_data = [
            (1, 101, '2023-01-01', 'completed', 100.0, 1),
            (2, 102, '2023-01-02', 'completed', 200.0, 1),
            (3, 101, '2023-01-03', 'completed', 150.0, 1),
            (4, 103, '2023-01-04', 'pending', 300.0, 0)
        ]
        
        customers_data = [
            (101, 'Customer 1', 'cust1@example.com'),
            (102, 'Customer 2', 'cust2@example.com'),
            (103, 'Customer 3', 'cust3@example.com')
        ]
        
        orders_df = spark.createDataFrame(orders_data, [
            'order_id', 'customer_id', 'order_date', 'status', 'total_amount', 'is_valid_order'
        ])
        
        customers_df = spark.createDataFrame(customers_data, [
            'customer_id', 'name', 'email'
        ])
        
        # Calculate metrics
        metrics_df = build_customer_metrics(orders_df, customers_df)
        print(f"✅ Metrics calculation completed")
        print(f"   Customers with metrics: {metrics_df.count()}")
        
        # Stop Spark session
        spark.stop()
        
        return True
        
    except Exception as e:
        print(f"❌ Metrics calculation test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("🧪 Testing Dashboard Components")
    print("=" * 50)
    
    tests = [
        test_fraud_detection,
        test_metrics_calculation
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Dashboard components should work correctly.")
    else:
        print("⚠️ Some tests failed. Check the errors above.")
    
    print("\n💡 To test the full dashboard:")
    print("   python scripts/run_dashboard.py batch")
    print("   python scripts/run_dashboard.py spark")
    print("   python scripts/run_dashboard.py simple")


if __name__ == "__main__":
    main()