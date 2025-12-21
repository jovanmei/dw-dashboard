"""
Simple Kafka connectivity test for the streaming pipeline.

This script performs basic Kafka operations to verify the setup is working.
"""

import json
import time
import uuid
from datetime import datetime


def test_kafka_basic():
    """Test basic Kafka producer and consumer functionality."""
    print("🧪 Testing Kafka Basic Functionality")
    print("=" * 50)
    
    try:
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.errors import KafkaError, NoBrokersAvailable
        
        bootstrap_servers = ['localhost:9092']
        test_topic = 'test_topic'
        
        print("📤 Testing Kafka Producer...")
        
        # Test Producer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=10000
        )
        
        # Send test message
        test_message = {
            'id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'message': 'Hello Kafka!',
            'test': True
        }
        
        try:
            future = producer.send(test_topic, test_message)
            record_metadata = future.get(timeout=10)
            print(f"✅ Message sent successfully!")
            print(f"   Topic: {record_metadata.topic}")
            print(f"   Partition: {record_metadata.partition}")
            print(f"   Offset: {record_metadata.offset}")
        except KafkaError as e:
            print(f"❌ Failed to send message: {e}")
            return False
        finally:
            producer.close()
        
        print("\n📥 Testing Kafka Consumer...")
        
        # Test Consumer
        consumer = KafkaConsumer(
            test_topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000,
            auto_offset_reset='earliest'
        )
        
        message_received = False
        start_time = time.time()
        
        for message in consumer:
            if message.value and message.value.get('id') == test_message['id']:
                print(f"✅ Test message received!")
                print(f"   Content: {message.value}")
                message_received = True
                break
            
            # Timeout after 10 seconds
            if time.time() - start_time > 10:
                break
        
        consumer.close()
        
        if message_received:
            print("\n🎉 Kafka connectivity test PASSED!")
            return True
        else:
            print("\n❌ Test message not received")
            return False
            
    except ImportError:
        print("❌ kafka-python not installed")
        print("   Install with: pip install kafka-python")
        return False
    except NoBrokersAvailable:
        print("❌ No Kafka brokers available")
        print("   Make sure Kafka is running on localhost:9092")
        return False
    except Exception as e:
        print(f"❌ Kafka test failed: {e}")
        return False


def test_ecommerce_topics():
    """Test the e-commerce specific topics."""
    print("\n🛒 Testing E-Commerce Topics")
    print("=" * 50)
    
    try:
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError
        
        bootstrap_servers = ['localhost:9092']
        
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='test_admin'
        )
        
        # Define e-commerce topics
        topics = [
            NewTopic(name="ecommerce_orders", num_partitions=3, replication_factor=1),
            NewTopic(name="ecommerce_customers", num_partitions=2, replication_factor=1),
            NewTopic(name="ecommerce_order_items", num_partitions=3, replication_factor=1),
        ]
        
        print("📝 Creating e-commerce topics...")
        
        try:
            fs = admin_client.create_topics(new_topics=topics, validate_only=False)
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"✅ Topic '{topic}' created")
                except TopicAlreadyExistsError:
                    print(f"ℹ️ Topic '{topic}' already exists")
                except Exception as e:
                    print(f"❌ Failed to create topic '{topic}': {e}")
        except Exception as e:
            print(f"❌ Error creating topics: {e}")
        
        admin_client.close()
        
        # Test sending sample e-commerce data
        print("\n📤 Testing e-commerce data...")
        
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Sample order
        sample_order = {
            "event_type": "order",
            "event_timestamp": datetime.now().isoformat(),
            "order_id": 12345,
            "customer_id": 101,
            "order_date": "2024-12-20",
            "status": "completed",
            "total_amount": 299.99,
            "source_system": "test"
        }
        
        # Send to orders topic
        future = producer.send('ecommerce_orders', sample_order)
        record_metadata = future.get(timeout=10)
        print(f"✅ Sample order sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
        
        producer.close()
        
        print("\n🎉 E-commerce topics test PASSED!")
        return True
        
    except Exception as e:
        print(f"❌ E-commerce topics test failed: {e}")
        return False


def main():
    """Run all Kafka tests."""
    print("🔍 Kafka Connectivity Tests")
    print("=" * 60)
    
    # Test 1: Basic Kafka functionality
    basic_test = test_kafka_basic()
    
    if basic_test:
        # Test 2: E-commerce specific topics
        ecommerce_test = test_ecommerce_topics()
        
        if ecommerce_test:
            print("\n" + "=" * 60)
            print("🎉 ALL TESTS PASSED!")
            print("✅ Kafka is ready for the streaming pipeline")
            print("\n💡 Next steps:")
            print("   1. Run: python run_kafka_pipeline.py --with-dashboard")
            print("   2. Access dashboard: http://localhost:8502")
            print("   3. Monitor Kafka: python kafka_monitor.py --once")
        else:
            print("\n❌ E-commerce topics test failed")
    else:
        print("\n❌ Basic Kafka test failed")
        print("\n🔧 Troubleshooting:")
        print("   1. Make sure Kafka is running: python start_kafka_windows.py")
        print("   2. Check Docker containers: docker ps")
        print("   3. Check Kafka logs: docker-compose logs kafka")


if __name__ == "__main__":
    main()