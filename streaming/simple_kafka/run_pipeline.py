"""
Simple Kafka pipeline using in-memory message broker.

This provides a fallback solution when Docker/Kafka is not available,
using an in-memory message broker that mimics Kafka behavior.
"""

import subprocess
import sys
import time
import signal
import os
from typing import List


def check_dependencies():
    """Check if required dependencies are available."""
    print("🔍 Checking dependencies...")
    
    required_packages = ['pyspark', 'streamlit', 'plotly', 'pandas']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} installed")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} not found")
    
    if missing_packages:
        print(f"\n❌ Missing packages: {', '.join(missing_packages)}")
        install = input("Install missing packages? (y/n): ").lower().strip()
        if install == 'y':
            try:
                subprocess.run([sys.executable, "-m", "pip", "install"] + missing_packages, 
                              check=True)
                print("✅ Packages installed successfully")
                return True
            except subprocess.CalledProcessError:
                print("❌ Failed to install packages")
                return False
        else:
            return False
    
    return True


def start_simple_kafka_server():
    """Start the simple Kafka server."""
    print("🚀 Starting Simple Kafka Server...")
    
    try:
        from streaming.simple_kafka.server import start_simple_kafka_server
        server = start_simple_kafka_server()
        return server
    except ImportError:
        print("❌ simple_kafka_server.py not found")
        return None
    except Exception as e:
        print(f"❌ Error starting simple Kafka server: {e}")
        return None


def start_data_generator(interval: float = 2.0, duration: int = None):
    """Start the enhanced data generator using simple Kafka."""
    print("🚀 Starting enhanced data generator (Simple Kafka mode)...")
    
    cmd = [
        sys.executable, "streaming_data_generator_simple.py",
        "--interval", str(interval),
        "--burst"  # Generate initial burst to populate all topics quickly
    ]
    
    if duration:
        cmd.extend(["--duration", str(duration)])
    
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"✅ Enhanced data generator started (PID: {process.pid})")
        print("   Populating all topics: orders, customers, order_items, fraud_alerts")
        return process
    except Exception as e:
        print(f"❌ Failed to start data generator: {e}")
        return None


def start_streaming_pipeline():
    """Start the streaming pipeline using simple Kafka."""
    print("🚀 Starting streaming pipeline (Simple Kafka mode)...")
    
    cmd = [
        sys.executable, "streaming_pipeline_simple.py"
    ]
    
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"✅ Streaming pipeline started (PID: {process.pid})")
        return process
    except Exception as e:
        print(f"❌ Failed to start streaming pipeline: {e}")
        return None


def start_dashboard(port: int = 8502):
    """Start the Simple Kafka dashboard."""
    print(f"🚀 Starting Simple Kafka dashboard on port {port}...")
    
    cmd = [
        sys.executable, "-m", "streamlit", "run", "app_simple_kafka.py",
        "--server.port", str(port),
        "--server.headless", "true"
    ]
    
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"✅ Simple Kafka dashboard started (PID: {process.pid})")
        print(f"   Access at: http://localhost:{port}")
        return process
    except Exception as e:
        print(f"❌ Failed to start dashboard: {e}")
        return None


def create_simple_data_generator():
    """Create a simple data generator script."""
    generator_code = '''"""
Simple data generator using in-memory Kafka server.
"""

import json
import time
import random
from datetime import datetime
from streaming.simple_kafka.server import SimpleKafkaProducer

def generate_order_event(order_id: int) -> dict:
    """Generate a single order event."""
    customer_id = random.randint(101, 300)
    total_amount = round(random.uniform(10.99, 2999.99), 2)
    
    # Occasionally generate high-value orders
    if random.random() < 0.05:
        total_amount = round(random.uniform(3000, 10000), 2)
    
    return {
        "event_type": "order",
        "event_timestamp": datetime.now().isoformat(),
        "order_id": order_id,
        "customer_id": customer_id,
        "order_date": datetime.now().strftime("%Y-%m-%d"),
        "status": random.choice(["pending", "processing", "completed", "cancelled"]),
        "total_amount": total_amount,
        "source_system": "simple_kafka"
    }

def main():
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--duration", type=int, default=None)
    args = parser.parse_args()
    
    producer = SimpleKafkaProducer(
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    order_id = 1000
    start_time = time.time()
    
    print(f"[START] Generating orders every {args.interval}s...")
    
    try:
        while True:
            if args.duration and (time.time() - start_time) > args.duration:
                break
            
            order_event = generate_order_event(order_id)
            producer.send('ecommerce_orders', order_event)
            
            order_id += 1
            
            if order_id % 10 == 0:
                print(f"   Generated {order_id - 1000} orders...")
            
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print(f"\\n[STOP] Generated {order_id - 1000} orders total")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
'''
    
    with open("streaming_data_generator_simple.py", "w") as f:
        f.write(generator_code)
    
    print("✅ Created streaming_data_generator_simple.py")


def create_simple_streaming_pipeline():
    """Create a simple streaming pipeline script."""
    pipeline_code = '''"""
Simple streaming pipeline using in-memory Kafka server.
"""

import time
import json
from datetime import datetime
from streaming.simple_kafka.server import SimpleKafkaConsumer

def process_orders():
    """Process orders from the simple Kafka server."""
    consumer = SimpleKafkaConsumer(
        'ecommerce_orders',
        value_deserializer=lambda m: m,
        consumer_timeout_ms=5000
    )
    
    print("[START] Processing orders from simple Kafka...")
    
    processed_count = 0
    
    try:
        while True:
            messages_processed = 0
            
            for message in consumer:
                if message.value:
                    order = message.value
                    
                    # Simple processing - just log high-value orders
                    if order.get('total_amount', 0) > 1000:
                        print(f"[FRAUD?] High-value order: {order['order_id']} - ${order['total_amount']}")
                    
                    processed_count += 1
                    messages_processed += 1
                    
                    if processed_count % 10 == 0:
                        print(f"   Processed {processed_count} orders...")
            
            if messages_processed == 0:
                time.sleep(1)  # Wait if no messages
                
    except KeyboardInterrupt:
        print(f"\\n[STOP] Processed {processed_count} orders total")
    finally:
        consumer.close()

if __name__ == "__main__":
    process_orders()
'''
    
    with open("streaming_pipeline_simple.py", "w") as f:
        f.write(pipeline_code)
    
    print("✅ Created streaming_pipeline_simple.py")


def main():
    """Main function to run the simple Kafka pipeline."""
    print("🚀 Simple Kafka Streaming Pipeline")
    print("=" * 50)
    print("This uses an in-memory message broker instead of Docker/Kafka")
    print()
    
    # Check dependencies
    if not check_dependencies():
        print("❌ Dependencies not available")
        return
    
    # Start simple Kafka server
    server = start_simple_kafka_server()
    if not server:
        print("❌ Failed to start simple Kafka server")
        return
    
    # Create helper scripts
    create_simple_data_generator()
    create_simple_streaming_pipeline()
    
    processes = []
    
    def cleanup():
        print("\\n🛑 Stopping all processes...")
        for process in processes:
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
    
    def signal_handler(signum, frame):
        cleanup()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start data generator
        generator_process = start_data_generator(interval=2.0)
        if generator_process:
            processes.append(generator_process)
        
        time.sleep(2)
        
        # Start streaming pipeline
        pipeline_process = start_streaming_pipeline()
        if pipeline_process:
            processes.append(pipeline_process)
        
        time.sleep(3)
        
        # Start dashboard
        dashboard_process = start_dashboard(8502)
        if dashboard_process:
            processes.append(dashboard_process)
        
        print("\\n✅ All components started!")
        print("\\n📊 Access Points:")
        print("   Simple Kafka Dashboard: http://localhost:8502")
        print("   (Shows live data from Simple Kafka in-memory broker)")
        print("\\n⏹️ Press Ctrl+C to stop")
        
        # Monitor processes
        while True:
            time.sleep(10)
            
            # Check if processes are still running
            for i, process in enumerate(processes):
                if process and process.poll() is not None:
                    print(f"⚠️ Process {i+1} stopped unexpectedly")
        
    except KeyboardInterrupt:
        print("\\n🛑 Pipeline stopped by user")
    finally:
        cleanup()


if __name__ == "__main__":
    main()