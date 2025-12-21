"""
Kafka setup and management script for the streaming pipeline.

This script helps set up Kafka topics and provides utilities for managing
the Kafka-based streaming pipeline.
"""

import json
import time
import subprocess
import sys
from typing import List, Dict, Optional


def check_kafka_installation():
    """Check if Kafka is installed and accessible."""
    print("🔍 Checking Kafka installation...")
    
    try:
        # Try to run kafka-topics command
        result = subprocess.run(
            ["kafka-topics.bat", "--version"] if sys.platform == "win32" else ["kafka-topics.sh", "--version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("✅ Kafka CLI tools found")
            return True
        else:
            print("❌ Kafka CLI tools not found in PATH")
            return False
            
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("❌ Kafka CLI tools not found")
        return False


def check_kafka_server(bootstrap_servers: str = "localhost:9092"):
    """Check if Kafka server is running."""
    print(f"🔍 Checking Kafka server at {bootstrap_servers}...")
    
    try:
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable
        
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            request_timeout_ms=5000,
            api_version=(0, 10, 1)
        )
        producer.close()
        print("✅ Kafka server is running")
        return True
        
    except ImportError:
        print("❌ kafka-python not installed. Run: pip install kafka-python")
        return False
    except NoBrokersAvailable:
        print("❌ Kafka server not accessible")
        return False
    except Exception as e:
        print(f"❌ Error connecting to Kafka: {e}")
        return False


def create_topic(topic_name: str, partitions: int = 3, replication_factor: int = 1, 
                bootstrap_servers: str = "localhost:9092"):
    """Create a Kafka topic."""
    print(f"📝 Creating topic '{topic_name}'...")
    
    try:
        cmd = [
            "kafka-topics.bat" if sys.platform == "win32" else "kafka-topics.sh",
            "--create",
            "--topic", topic_name,
            "--partitions", str(partitions),
            "--replication-factor", str(replication_factor),
            "--bootstrap-server", bootstrap_servers
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print(f"✅ Topic '{topic_name}' created successfully")
            return True
        elif "already exists" in result.stderr:
            print(f"ℹ️ Topic '{topic_name}' already exists")
            return True
        else:
            print(f"❌ Failed to create topic: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error creating topic: {e}")
        return False


def list_topics(bootstrap_servers: str = "localhost:9092"):
    """List all Kafka topics."""
    print("📋 Listing Kafka topics...")
    
    try:
        cmd = [
            "kafka-topics.bat" if sys.platform == "win32" else "kafka-topics.sh",
            "--list",
            "--bootstrap-server", bootstrap_servers
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            topics = result.stdout.strip().split('\n')
            topics = [t for t in topics if t.strip()]
            
            if topics:
                print("✅ Available topics:")
                for topic in topics:
                    print(f"   - {topic}")
            else:
                print("ℹ️ No topics found")
            return topics
        else:
            print(f"❌ Failed to list topics: {result.stderr}")
            return []
            
    except Exception as e:
        print(f"❌ Error listing topics: {e}")
        return []


def delete_topic(topic_name: str, bootstrap_servers: str = "localhost:9092"):
    """Delete a Kafka topic."""
    print(f"🗑️ Deleting topic '{topic_name}'...")
    
    try:
        cmd = [
            "kafka-topics.bat" if sys.platform == "win32" else "kafka-topics.sh",
            "--delete",
            "--topic", topic_name,
            "--bootstrap-server", bootstrap_servers
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print(f"✅ Topic '{topic_name}' deleted successfully")
            return True
        else:
            print(f"❌ Failed to delete topic: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error deleting topic: {e}")
        return False


def setup_ecommerce_topics(bootstrap_servers: str = "localhost:9092"):
    """Set up all topics needed for the e-commerce streaming pipeline."""
    print("🏗️ Setting up e-commerce streaming topics...")
    
    topics = [
        {"name": "ecommerce_orders", "partitions": 3, "description": "Order events"},
        {"name": "ecommerce_customers", "partitions": 2, "description": "Customer events"},
        {"name": "ecommerce_order_items", "partitions": 3, "description": "Order item events"},
        {"name": "ecommerce_fraud_alerts", "partitions": 1, "description": "Fraud detection alerts"}
    ]
    
    success_count = 0
    
    for topic in topics:
        print(f"\n📝 Setting up {topic['name']} ({topic['description']})...")
        if create_topic(topic["name"], topic["partitions"], 1, bootstrap_servers):
            success_count += 1
    
    print(f"\n📊 Setup Summary: {success_count}/{len(topics)} topics ready")
    return success_count == len(topics)


def test_kafka_connectivity(bootstrap_servers: str = "localhost:9092"):
    """Test Kafka connectivity by sending and receiving a test message."""
    print("🧪 Testing Kafka connectivity...")
    
    try:
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.errors import KafkaError
        import uuid
        
        test_topic = "test_connectivity"
        test_message = {"test": True, "timestamp": time.time(), "id": str(uuid.uuid4())}
        
        # Create test topic
        create_topic(test_topic, 1, 1, bootstrap_servers)
        
        # Test producer
        print("📤 Testing producer...")
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=10000
        )
        
        future = producer.send(test_topic, test_message)
        producer.flush()
        
        try:
            record_metadata = future.get(timeout=10)
            print(f"✅ Message sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
        except KafkaError as e:
            print(f"❌ Failed to send message: {e}")
            return False
        finally:
            producer.close()
        
        # Test consumer
        print("📥 Testing consumer...")
        consumer = KafkaConsumer(
            test_topic,
            bootstrap_servers=[bootstrap_servers],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000,
            auto_offset_reset='earliest'
        )
        
        message_received = False
        for message in consumer:
            if message.value.get("id") == test_message["id"]:
                print(f"✅ Test message received: {message.value}")
                message_received = True
                break
        
        consumer.close()
        
        # Clean up test topic
        delete_topic(test_topic, bootstrap_servers)
        
        if message_received:
            print("✅ Kafka connectivity test passed")
            return True
        else:
            print("❌ Test message not received")
            return False
            
    except ImportError:
        print("❌ kafka-python not installed. Run: pip install kafka-python")
        return False
    except Exception as e:
        print(f"❌ Connectivity test failed: {e}")
        return False


def install_kafka_python():
    """Install kafka-python package."""
    print("📦 Installing kafka-python...")
    
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "kafka-python"], 
                      check=True, capture_output=True)
        print("✅ kafka-python installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install kafka-python: {e}")
        return False


def main():
    """Main setup function."""
    print("🚀 Kafka Setup for E-Commerce Streaming Pipeline")
    print("=" * 60)
    
    # Check if kafka-python is installed
    try:
        import kafka
        print("✅ kafka-python is installed")
    except ImportError:
        print("❌ kafka-python not found")
        install = input("Install kafka-python? (y/n): ").lower().strip()
        if install == 'y':
            if not install_kafka_python():
                return
        else:
            print("❌ kafka-python is required for Kafka streaming")
            return
    
    bootstrap_servers = input("Enter Kafka bootstrap servers (default: localhost:9092): ").strip()
    if not bootstrap_servers:
        bootstrap_servers = "localhost:9092"
    
    print(f"\n🔧 Using Kafka servers: {bootstrap_servers}")
    
    # Menu
    while True:
        print("\n" + "=" * 60)
        print("Kafka Management Options:")
        print("  1. Check Kafka server status")
        print("  2. Set up e-commerce topics")
        print("  3. List all topics")
        print("  4. Test connectivity")
        print("  5. Delete topic")
        print("  6. Exit")
        
        choice = input("\nEnter choice (1-6): ").strip()
        
        if choice == "1":
            check_kafka_server(bootstrap_servers)
            
        elif choice == "2":
            setup_ecommerce_topics(bootstrap_servers)
            
        elif choice == "3":
            list_topics(bootstrap_servers)
            
        elif choice == "4":
            test_kafka_connectivity(bootstrap_servers)
            
        elif choice == "5":
            topic_name = input("Enter topic name to delete: ").strip()
            if topic_name:
                confirm = input(f"Delete topic '{topic_name}'? (y/n): ").lower().strip()
                if confirm == 'y':
                    delete_topic(topic_name, bootstrap_servers)
            
        elif choice == "6":
            print("👋 Exiting Kafka setup")
            break
            
        else:
            print("❌ Invalid choice")
    
    print("\n💡 Next Steps:")
    print("  1. Make sure Kafka server is running")
    print("  2. Run: python streaming_data_generator.py --mode kafka")
    print("  3. Run: python streaming_pipeline.py --mode kafka")
    print("  4. Run: streamlit run app_realtime.py --server.port 8502")


if __name__ == "__main__":
    main()