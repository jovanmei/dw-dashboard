"""
Debug script for streaming pipeline troubleshooting.

This script helps diagnose issues with the real-time streaming pipeline
by checking all components and data flow.
"""

import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


def check_directories():
    """Check if all required directories exist and have data."""
    print("🔍 Checking directories...")
    
    directories = {
        'Input': 'data/streaming/orders',
        'Bronze': 'lake/bronze/orders_streaming', 
        'Silver': 'lake/silver/orders_enriched_streaming',
        'Checkpoints': 'checkpoints/streaming'
    }
    
    status = {}
    for name, path in directories.items():
        path_obj = Path(path)
        exists = path_obj.exists()
        
        if exists:
            if name == 'Input':
                files = list(path_obj.glob("*.json"))
                count = len(files)
                latest = max([f.stat().st_mtime for f in files]) if files else None
            else:
                files = list(path_obj.rglob("*.parquet"))
                count = len(files)
                latest = max([f.stat().st_mtime for f in files]) if files else None
            
            status[name] = {
                'exists': True,
                'files': count,
                'latest': datetime.fromtimestamp(latest) if latest else None
            }
            
            print(f"  ✅ {name}: {count} files")
            if latest:
                print(f"     Latest: {datetime.fromtimestamp(latest)}")
        else:
            status[name] = {'exists': False, 'files': 0, 'latest': None}
            print(f"  ❌ {name}: Directory not found")
    
    return status


def check_spark_data():
    """Check data accessibility through Spark."""
    if not SPARK_AVAILABLE:
        print("❌ PySpark not available")
        return
    
    print("\n🔍 Checking Spark data access...")
    
    try:
        spark = SparkSession.builder \
            .appName("StreamingDebug") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        print("  ✅ Spark session created")
        
        # Check Bronze layer
        bronze_path = "lake/bronze/orders_streaming"
        if os.path.exists(bronze_path):
            try:
                df = spark.read.parquet(bronze_path)
                count = df.count()
                print(f"  ✅ Bronze layer: {count} orders")
                
                # Show sample data
                if count > 0:
                    print("     Sample data:")
                    df.show(5, truncate=False)
            except Exception as e:
                print(f"  ❌ Bronze layer error: {e}")
        else:
            print("  ⚠️ Bronze layer: No data directory")
        
        # Check Silver layer
        silver_path = "lake/silver/orders_enriched_streaming"
        if os.path.exists(silver_path):
            try:
                df = spark.read.parquet(silver_path)
                count = df.count()
                print(f"  ✅ Silver layer: {count} processed orders")
            except Exception as e:
                print(f"  ❌ Silver layer error: {e}")
        else:
            print("  ⚠️ Silver layer: No data directory")
        
        spark.stop()
        
    except Exception as e:
        print(f"  ❌ Spark error: {e}")


def check_processes():
    """Check if streaming processes are running."""
    print("\n🔍 Checking running processes...")
    
    try:
        import psutil
        
        streaming_processes = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                if any(keyword in cmdline for keyword in ['streaming_data_generator', 'streaming_pipeline', 'app_realtime']):
                    streaming_processes.append({
                        'pid': proc.info['pid'],
                        'name': proc.info['name'],
                        'cmdline': cmdline
                    })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        if streaming_processes:
            print(f"  ✅ Found {len(streaming_processes)} streaming processes:")
            for proc in streaming_processes:
                print(f"     PID {proc['pid']}: {proc['cmdline'][:80]}...")
        else:
            print("  ⚠️ No streaming processes found")
            
    except ImportError:
        print("  ⚠️ psutil not available, cannot check processes")


def generate_test_data():
    """Generate a small test file to verify pipeline."""
    print("\n🔧 Generating test data...")
    
    import json
    from datetime import datetime
    
    # Create input directory
    input_dir = Path("data/streaming/orders")
    input_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate test order
    test_order = {
        "event_type": "order_created",
        "event_timestamp": datetime.now().isoformat() + ".000",
        "order_id": 99999,
        "customer_id": 1001,
        "order_date": "2024-12-20",
        "status": "completed",
        "total_amount": 1500.00,
        "source_system": "debug_test"
    }
    
    # Write test file
    test_file = input_dir / f"debug_test_{int(time.time())}.json"
    with open(test_file, 'w') as f:
        json.dump(test_order, f)
    
    print(f"  ✅ Created test file: {test_file}")
    print(f"     Content: {test_order}")
    
    return test_file


def monitor_pipeline(duration=60):
    """Monitor pipeline activity for a specified duration."""
    print(f"\n👀 Monitoring pipeline for {duration} seconds...")
    
    start_time = time.time()
    initial_status = check_directories()
    
    print("  Initial state:")
    for name, info in initial_status.items():
        print(f"    {name}: {info['files']} files")
    
    print(f"\n  Monitoring... (Press Ctrl+C to stop early)")
    
    try:
        while time.time() - start_time < duration:
            time.sleep(10)  # Check every 10 seconds
            
            current_status = check_directories()
            
            # Check for changes
            changes = []
            for name, info in current_status.items():
                initial_files = initial_status[name]['files']
                current_files = info['files']
                
                if current_files > initial_files:
                    changes.append(f"{name}: +{current_files - initial_files} files")
            
            if changes:
                elapsed = int(time.time() - start_time)
                print(f"  [{elapsed}s] Changes detected: {', '.join(changes)}")
            
            # Update initial status
            initial_status = current_status
    
    except KeyboardInterrupt:
        print("\n  Monitoring stopped by user")
    
    print("\n  Final state:")
    final_status = check_directories()
    for name, info in final_status.items():
        print(f"    {name}: {info['files']} files")


def main():
    """Main debug function."""
    print("🔧 Streaming Pipeline Debug Tool")
    print("=" * 50)
    
    # Check directories
    dir_status = check_directories()
    
    # Check Spark data access
    check_spark_data()
    
    # Check running processes
    check_processes()
    
    # Recommendations
    print("\n💡 Recommendations:")
    
    if not dir_status['Input']['exists']:
        print("  1. Create input directory: mkdir -p data/streaming/orders")
    elif dir_status['Input']['files'] == 0:
        print("  1. Start data generator: python streaming_data_generator.py")
    else:
        print("  ✅ Input data available")
    
    if not dir_status['Bronze']['exists'] or dir_status['Bronze']['files'] == 0:
        print("  2. Start streaming pipeline: python streaming_pipeline.py")
    else:
        print("  ✅ Pipeline processing data")
    
    if dir_status['Input']['files'] > 0 and dir_status['Bronze']['files'] == 0:
        print("  ⚠️ Data generator running but pipeline not processing")
        print("     Check pipeline logs for errors")
    
    # Interactive options
    print("\n🛠️ Debug Options:")
    print("  1. Generate test data")
    print("  2. Monitor pipeline (60s)")
    print("  3. Exit")
    
    try:
        choice = input("\nEnter choice (1-3): ").strip()
        
        if choice == "1":
            test_file = generate_test_data()
            print(f"\n  Test file created. Monitor Bronze layer for processing:")
            print(f"  watch -n 2 'ls -la lake/bronze/orders_streaming/'")
            
        elif choice == "2":
            monitor_pipeline(60)
            
        elif choice == "3":
            print("  Exiting debug tool")
            
        else:
            print("  Invalid choice")
            
    except KeyboardInterrupt:
        print("\n  Debug tool interrupted")


if __name__ == "__main__":
    main()