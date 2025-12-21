"""
Test script to verify the real-time streaming setup is working correctly.

This script performs basic checks on all streaming components and provides
troubleshooting guidance if issues are found.
"""

import os
import sys
import time
from pathlib import Path

def check_dependencies():
    """Check if required dependencies are available."""
    print("🔍 Checking Dependencies...")
    
    issues = []
    
    # Check PySpark
    try:
        import pyspark
        print(f"✅ PySpark {pyspark.__version__} - OK")
    except ImportError:
        issues.append("❌ PySpark not found. Install with: pip install pyspark")
    
    # Check Streamlit
    try:
        import streamlit
        print(f"✅ Streamlit {streamlit.__version__} - OK")
    except ImportError:
        issues.append("❌ Streamlit not found. Install with: pip install streamlit")
    
    # Check Plotly
    try:
        import plotly
        print(f"✅ Plotly {plotly.__version__} - OK")
    except ImportError:
        issues.append("❌ Plotly not found. Install with: pip install plotly")
    
    # Check Pandas
    try:
        import pandas
        print(f"✅ Pandas {pandas.__version__} - OK")
    except ImportError:
        issues.append("❌ Pandas not found. Install with: pip install pandas")
    
    # Check optional Kafka (not required for file-based streaming)
    try:
        import kafka
        print(f"✅ kafka-python {kafka.__version__} - OK (optional)")
    except ImportError:
        print("ℹ️  kafka-python not found (optional - only needed for Kafka mode)")
    
    return issues

def check_directories():
    """Check if required directories exist or can be created."""
    print("\n📁 Checking Directories...")
    
    required_dirs = [
        "data/streaming/orders",
        "data/streaming/customers", 
        "checkpoints/streaming",
        "lake/bronze",
        "lake/silver",
        "lake/gold"
    ]
    
    for directory in required_dirs:
        try:
            Path(directory).mkdir(parents=True, exist_ok=True)
            print(f"✅ {directory} - OK")
        except Exception as e:
            print(f"❌ {directory} - Error: {e}")

def test_spark_session():
    """Test if Spark session can be created."""
    print("\n⚡ Testing Spark Session...")
    
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("StreamingTest") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        # Test basic functionality
        test_data = [(1, "test"), (2, "data")]
        df = spark.createDataFrame(test_data, ["id", "value"])
        count = df.count()
        
        spark.stop()
        
        print(f"✅ Spark session created and tested successfully (processed {count} rows)")
        return True
        
    except Exception as e:
        print(f"❌ Spark session test failed: {e}")
        return False

def test_data_generator():
    """Test the streaming data generator."""
    print("\n🔄 Testing Data Generator...")
    
    try:
        from streaming.file_based.data_generator import StreamingDataGenerator
        
        # Create generator in file mode
        generator = StreamingDataGenerator(use_kafka=False)
        
        # Generate a single test event
        test_event = generator.generate_order_event(999)
        
        if test_event and 'order_id' in test_event:
            print("✅ Data generator working correctly")
            print(f"   Sample event: Order {test_event['order_id']} for ${test_event['total_amount']}")
            return True
        else:
            print("❌ Data generator produced invalid event")
            return False
            
    except Exception as e:
        print(f"❌ Data generator test failed: {e}")
        return False

def test_streaming_pipeline():
    """Test streaming pipeline components."""
    print("\n🌊 Testing Streaming Pipeline...")
    
    try:
        from streaming.file_based.pipeline import StreamingETLPipeline, ORDER_SCHEMA
        
        # Test schema definition
        if ORDER_SCHEMA and len(ORDER_SCHEMA.fields) > 0:
            print("✅ Streaming schemas defined correctly")
        else:
            print("❌ Streaming schemas not properly defined")
            return False
        
        # Test pipeline creation (without starting streams)
        pipeline = StreamingETLPipeline()
        if pipeline.spark:
            print("✅ Streaming pipeline can be initialized")
            pipeline.spark.stop()
            return True
        else:
            print("❌ Streaming pipeline initialization failed")
            return False
            
    except Exception as e:
        print(f"❌ Streaming pipeline test failed: {e}")
        return False

def run_quick_integration_test():
    """Run a quick end-to-end test."""
    print("\n🧪 Running Quick Integration Test...")
    
    try:
        # Generate a few test files
        from streaming.file_based.data_generator import StreamingDataGenerator
        
        generator = StreamingDataGenerator(use_kafka=False)
        test_dir = "data/streaming/test"
        os.makedirs(test_dir, exist_ok=True)
        
        # Generate 3 test events
        for i in range(3):
            event = generator.generate_order_event(1000 + i)
            generator.stream_to_file(f"{test_dir}/test_order_{i}.json", event)
        
        # Check files were created
        test_files = [f for f in os.listdir(test_dir) if f.endswith('.json')]
        
        if len(test_files) >= 3:
            print(f"✅ Integration test passed - generated {len(test_files)} test files")
            
            # Clean up test files
            for file in test_files:
                os.remove(os.path.join(test_dir, file))
            os.rmdir(test_dir)
            
            return True
        else:
            print(f"❌ Integration test failed - only {len(test_files)} files generated")
            return False
            
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        return False

def main():
    """Run all tests and provide setup guidance."""
    print("🚀 Real-Time Streaming Setup Test")
    print("=" * 50)
    
    # Run all tests
    dependency_issues = check_dependencies()
    check_directories()
    spark_ok = test_spark_session()
    generator_ok = test_data_generator()
    pipeline_ok = test_streaming_pipeline()
    integration_ok = run_quick_integration_test()
    
    print("\n" + "=" * 50)
    print("📋 Test Summary")
    print("=" * 50)
    
    # Report results
    if dependency_issues:
        print("❌ Dependency Issues Found:")
        for issue in dependency_issues:
            print(f"   {issue}")
        print("\n💡 Fix: pip install -r requirements.txt")
    else:
        print("✅ All dependencies OK")
    
    if spark_ok and generator_ok and pipeline_ok and integration_ok:
        print("✅ All streaming components working correctly!")
        print("\n🎉 Ready to run real-time streaming pipeline!")
        print("\n🚀 Quick Start Commands:")
        print("   python run_realtime_pipeline.py --with-dashboard")
        print("   # OR")
        print("   python streaming_data_generator.py  # Terminal 1")
        print("   python streaming_pipeline.py        # Terminal 2") 
        print("   streamlit run app_realtime.py       # Terminal 3")
    else:
        print("❌ Some components have issues. Please fix the errors above.")
        
        if not spark_ok:
            print("\n💡 Spark Issues:")
            print("   - Check JAVA_HOME is set correctly")
            print("   - Ensure Java 8+ is installed")
            print("   - Try: pip install --upgrade pyspark")
        
        if not generator_ok or not pipeline_ok:
            print("\n💡 Streaming Issues:")
            print("   - Check file permissions in project directory")
            print("   - Ensure all Python files are present")
            print("   - Try running from project root directory")

if __name__ == "__main__":
    main()