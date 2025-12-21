"""
Kafka-based real-time streaming pipeline orchestrator.

This script sets up and runs the complete Kafka-based streaming pipeline
including topic creation, data generation, stream processing, and dashboard.
"""

import subprocess
import sys
import time
import argparse
import os
import signal
from pathlib import Path
from typing import List, Optional


class KafkaStreamingOrchestrator:
    """Orchestrates the complete Kafka streaming pipeline."""
    
    def __init__(self, kafka_servers: str = "localhost:9092"):
        self.kafka_servers = kafka_servers
        self.processes: List[subprocess.Popen] = []
        self.topics = [
            "ecommerce_orders",
            "ecommerce_customers", 
            "ecommerce_order_items",
            "ecommerce_fraud_alerts"
        ]
    
    def check_dependencies(self):
        """Check if all required dependencies are available."""
        print("🔍 Checking dependencies...")
        
        # Check kafka-python
        try:
            import kafka
            print(f"✅ kafka-python {kafka.__version__} installed")
        except ImportError:
            print("❌ kafka-python not found")
            install = input("Install kafka-python? (y/n): ").lower().strip()
            if install == 'y':
                try:
                    subprocess.run([sys.executable, "-m", "pip", "install", "kafka-python"], 
                                  check=True)
                    print("✅ kafka-python installed successfully")
                except subprocess.CalledProcessError:
                    print("❌ Failed to install kafka-python")
                    return False
            else:
                return False
        
        # Check PySpark
        try:
            import pyspark
            print(f"✅ PySpark {pyspark.__version__} installed")
        except ImportError:
            print("❌ PySpark not found. Install with: pip install pyspark")
            return False
        
        # Check Streamlit
        try:
            import streamlit
            print(f"✅ Streamlit {streamlit.__version__} installed")
        except ImportError:
            print("❌ Streamlit not found. Install with: pip install streamlit")
            return False
        
        return True
    
    def check_kafka_server(self):
        """Check if Kafka server is accessible."""
        print(f"🔍 Checking Kafka server at {self.kafka_servers}...")
        
        try:
            from kafka import KafkaProducer
            from kafka.errors import NoBrokersAvailable
            
            producer = KafkaProducer(
                bootstrap_servers=[self.kafka_servers],
                request_timeout_ms=5000
            )
            producer.close()
            print("✅ Kafka server is accessible")
            return True
            
        except NoBrokersAvailable:
            print("❌ Kafka server not accessible")
            print("   Make sure Kafka is running:")
            print("   1. Start Zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties")
            print("   2. Start Kafka: bin/kafka-server-start.sh config/server.properties")
            return False
        except Exception as e:
            print(f"❌ Error connecting to Kafka: {e}")
            return False
    
    def setup_topics(self):
        """Set up required Kafka topics."""
        print("🏗️ Setting up Kafka topics...")
        
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            from kafka.errors import TopicAlreadyExistsError
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=[self.kafka_servers],
                client_id='pipeline_setup'
            )
            
            topic_configs = [
                NewTopic(name="ecommerce_orders", num_partitions=3, replication_factor=1),
                NewTopic(name="ecommerce_customers", num_partitions=2, replication_factor=1),
                NewTopic(name="ecommerce_order_items", num_partitions=3, replication_factor=1),
                NewTopic(name="ecommerce_fraud_alerts", num_partitions=1, replication_factor=1)
            ]
            
            try:
                fs = admin_client.create_topics(new_topics=topic_configs, validate_only=False)
                
                # Wait for topics to be created
                for topic, f in fs.items():
                    try:
                        f.result()  # The result itself is None
                        print(f"✅ Topic '{topic}' created")
                    except TopicAlreadyExistsError:
                        print(f"ℹ️ Topic '{topic}' already exists")
                    except Exception as e:
                        print(f"❌ Failed to create topic '{topic}': {e}")
                        
            except Exception as e:
                print(f"❌ Error creating topics: {e}")
                return False
            
            admin_client.close()
            return True
            
        except Exception as e:
            print(f"❌ Error setting up topics: {e}")
            return False
    
    def start_data_generator(self, interval: float = 2.0, duration: Optional[int] = None):
        """Start the Kafka data generator."""
        print("🚀 Starting Kafka data generator...")
        
        cmd = [
            sys.executable, "streaming_data_generator.py",
            "--mode", "kafka",
            "--kafka-servers", self.kafka_servers,
            "--interval", str(interval),
            "--topic", "ecommerce_orders"
        ]
        
        if duration:
            cmd.extend(["--duration", str(duration)])
        
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.processes.append(process)
            print(f"✅ Data generator started (PID: {process.pid})")
            return process
        except Exception as e:
            print(f"❌ Failed to start data generator: {e}")
            return None
    
    def start_streaming_pipeline(self):
        """Start the Spark Structured Streaming pipeline."""
        print("🚀 Starting Spark streaming pipeline...")
        
        cmd = [
            sys.executable, "streaming_pipeline.py",
            "--mode", "kafka",
            "--kafka-servers", self.kafka_servers,
            "--kafka-topic", "ecommerce_orders"
        ]
        
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.processes.append(process)
            print(f"✅ Streaming pipeline started (PID: {process.pid})")
            return process
        except Exception as e:
            print(f"❌ Failed to start streaming pipeline: {e}")
            return None
    
    def start_dashboard(self, port: int = 8502):
        """Start the real-time dashboard."""
        print(f"🚀 Starting real-time dashboard on port {port}...")
        
        cmd = [
            sys.executable, "-m", "streamlit", "run", "app_realtime.py",
            "--server.port", str(port),
            "--server.headless", "true"
        ]
        
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.processes.append(process)
            print(f"✅ Dashboard started (PID: {process.pid})")
            print(f"   Access at: http://localhost:{port}")
            return process
        except Exception as e:
            print(f"❌ Failed to start dashboard: {e}")
            return None
    
    def monitor_processes(self):
        """Monitor running processes and show their status."""
        print("\n📊 Process Status:")
        print("-" * 50)
        
        for i, process in enumerate(self.processes):
            if process.poll() is None:
                print(f"✅ Process {i+1} (PID: {process.pid}) - Running")
            else:
                print(f"❌ Process {i+1} (PID: {process.pid}) - Stopped (code: {process.returncode})")
    
    def stop_all_processes(self):
        """Stop all running processes."""
        print("\n🛑 Stopping all processes...")
        
        for i, process in enumerate(self.processes):
            if process.poll() is None:
                try:
                    process.terminate()
                    process.wait(timeout=10)
                    print(f"✅ Process {i+1} stopped")
                except subprocess.TimeoutExpired:
                    process.kill()
                    print(f"⚠️ Process {i+1} force killed")
                except Exception as e:
                    print(f"❌ Error stopping process {i+1}: {e}")
        
        self.processes.clear()
    
    def run_pipeline(self, generator_interval: float = 2.0, 
                    generator_duration: Optional[int] = None,
                    with_dashboard: bool = True,
                    dashboard_port: int = 8502):
        """Run the complete Kafka streaming pipeline."""
        print("🚀 Starting Kafka Streaming Pipeline")
        print("=" * 60)
        
        # Setup signal handler for graceful shutdown
        def signal_handler(signum, frame):
            print("\n\n⚠️ Shutdown signal received...")
            self.stop_all_processes()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            # Check dependencies
            if not self.check_dependencies():
                return False
            
            # Check Kafka server
            if not self.check_kafka_server():
                return False
            
            # Setup topics
            if not self.setup_topics():
                return False
            
            # Start components
            print("\n🚀 Starting pipeline components...")
            
            # Start data generator
            generator_process = self.start_data_generator(generator_interval, generator_duration)
            if not generator_process:
                return False
            
            # Wait a bit for generator to start
            time.sleep(3)
            
            # Start streaming pipeline
            pipeline_process = self.start_streaming_pipeline()
            if not pipeline_process:
                return False
            
            # Wait for pipeline to initialize
            time.sleep(5)
            
            # Start dashboard if requested
            if with_dashboard:
                dashboard_process = self.start_dashboard(dashboard_port)
                if not dashboard_process:
                    print("⚠️ Dashboard failed to start, but pipeline will continue")
            
            print("\n✅ All components started successfully!")
            print("\n📊 Pipeline Information:")
            print(f"   Kafka servers: {self.kafka_servers}")
            print(f"   Generator interval: {generator_interval}s")
            if generator_duration:
                print(f"   Generator duration: {generator_duration}s")
            if with_dashboard:
                print(f"   Dashboard: http://localhost:{dashboard_port}")
            
            print("\n⏹️ Press Ctrl+C to stop the pipeline")
            
            # Monitor processes
            while True:
                time.sleep(30)  # Check every 30 seconds
                self.monitor_processes()
                
                # Check if any critical process has stopped
                if generator_process.poll() is not None:
                    print("⚠️ Data generator stopped")
                if pipeline_process.poll() is not None:
                    print("⚠️ Streaming pipeline stopped")
                
        except KeyboardInterrupt:
            print("\n\n🛑 Pipeline stopped by user")
        except Exception as e:
            print(f"\n❌ Pipeline error: {e}")
        finally:
            self.stop_all_processes()
        
        return True


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Run Kafka-based streaming pipeline")
    parser.add_argument("--kafka-servers", default="localhost:9092",
                       help="Kafka bootstrap servers")
    parser.add_argument("--generator-interval", type=float, default=2.0,
                       help="Data generator interval in seconds")
    parser.add_argument("--generator-duration", type=int,
                       help="Data generator duration in seconds (None = infinite)")
    parser.add_argument("--with-dashboard", action="store_true", default=True,
                       help="Start dashboard along with pipeline")
    parser.add_argument("--no-dashboard", action="store_true",
                       help="Don't start dashboard")
    parser.add_argument("--dashboard-port", type=int, default=8502,
                       help="Dashboard port")
    
    args = parser.parse_args()
    
    # Handle dashboard flags
    with_dashboard = args.with_dashboard and not args.no_dashboard
    
    orchestrator = KafkaStreamingOrchestrator(args.kafka_servers)
    
    success = orchestrator.run_pipeline(
        generator_interval=args.generator_interval,
        generator_duration=args.generator_duration,
        with_dashboard=with_dashboard,
        dashboard_port=args.dashboard_port
    )
    
    if success:
        print("✅ Pipeline completed successfully")
    else:
        print("❌ Pipeline failed")
        sys.exit(1)


if __name__ == "__main__":
    main()