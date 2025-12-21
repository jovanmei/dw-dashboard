"""
Simple embedded Kafka server for development using kafka-python.

This provides a lightweight alternative to Docker for testing Kafka functionality.
Uses a file-based message broker that mimics Kafka behavior and persists across processes.
"""

import json
import time
import threading
import os
from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict, deque
from pathlib import Path
import pickle


class SimpleKafkaServer:
    """
    Simple file-based Kafka-like message broker for development.
    
    This persists messages to files so they can be shared across processes.
    """
    
    def __init__(self, data_dir: str = "simple_kafka_data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.topics_file = self.data_dir / "topics.json"
        self.messages_dir = self.data_dir / "messages"
        self.messages_dir.mkdir(exist_ok=True)
        self.lock = threading.Lock()
        
        # Load existing topics
        self._load_topics()
        
    def _load_topics(self):
        """Load topics from file."""
        if self.topics_file.exists():
            try:
                with open(self.topics_file, 'r') as f:
                    self.topics_info = json.load(f)
            except:
                self.topics_info = {}
        else:
            self.topics_info = {}
    
    def _save_topics(self):
        """Save topics to file."""
        with open(self.topics_file, 'w') as f:
            json.dump(self.topics_info, f, indent=2)
    
    def _get_topic_file(self, topic: str, partition: int) -> Path:
        """Get file path for topic partition."""
        return self.messages_dir / f"{topic}_p{partition}.jsonl"
    
    def create_topic(self, topic: str, partitions: int = 1):
        """Create a topic with specified partitions."""
        with self.lock:
            self.topics_info[topic] = {
                'partitions': partitions,
                'created_at': datetime.now().isoformat()
            }
            
            # Create partition files
            for partition in range(partitions):
                topic_file = self._get_topic_file(topic, partition)
                if not topic_file.exists():
                    topic_file.touch()
            
            self._save_topics()
        print(f"✅ Created topic '{topic}' with {partitions} partition(s)")
    
    def send_message(self, topic: str, message: dict, partition: int = 0):
        """Send a message to a topic partition."""
        with self.lock:
            if topic not in self.topics_info:
                self.create_topic(topic, 1)
            
            # Get current offset
            topic_file = self._get_topic_file(topic, partition)
            
            # Count existing messages to get offset
            offset = 0
            if topic_file.exists():
                with open(topic_file, 'r') as f:
                    offset = sum(1 for _ in f)
            
            # Add metadata to message
            enriched_message = {
                **message,
                '_kafka_timestamp': datetime.now().isoformat(),
                '_kafka_offset': offset,
                '_kafka_partition': partition,
                '_kafka_topic': topic
            }
            
            # Append to file
            with open(topic_file, 'a') as f:
                f.write(json.dumps(enriched_message) + '\n')
            
        return offset
    
    def consume_messages(self, topic: str, partition: int = 0, 
                        from_offset: Optional[int] = None, limit: int = 1000) -> List[dict]:
        """Consume messages from a topic partition."""
        topic_file = self._get_topic_file(topic, partition)
        
        if not topic_file.exists():
            return []
        
        messages = []
        try:
            with open(topic_file, 'r') as f:
                for line_num, line in enumerate(f):
                    if from_offset is not None and line_num < from_offset:
                        continue
                    
                    try:
                        message = json.loads(line.strip())
                        messages.append(message)
                        
                        if len(messages) >= limit:
                            break
                    except json.JSONDecodeError:
                        continue
        except FileNotFoundError:
            pass
        
        return messages
    
    def get_topic_info(self, topic: str) -> dict:
        """Get information about a topic."""
        if topic not in self.topics_info:
            return {'error': 'Topic not found'}
        
        partitions = {}
        total_messages = 0
        
        for partition in range(self.topics_info[topic]['partitions']):
            topic_file = self._get_topic_file(topic, partition)
            
            message_count = 0
            if topic_file.exists():
                try:
                    with open(topic_file, 'r') as f:
                        message_count = sum(1 for _ in f)
                except:
                    pass
            
            total_messages += message_count
            
            partitions[partition] = {
                'messages': message_count,
                'latest_offset': message_count,
                'earliest_offset': 0
            }
        
        return {
            'partitions': partitions,
            'total_messages': total_messages,
            'partition_count': len(partitions)
        }
    
    def list_topics(self) -> List[str]:
        """List all topics."""
        self._load_topics()  # Refresh from file
        return list(self.topics_info.keys())
    
    def clear_topic(self, topic: str):
        """Clear all messages from a topic."""
        if topic in self.topics_info:
            for partition in range(self.topics_info[topic]['partitions']):
                topic_file = self._get_topic_file(topic, partition)
                if topic_file.exists():
                    topic_file.unlink()
                    topic_file.touch()
            print(f"✅ Cleared topic '{topic}'")
    
    def delete_topic(self, topic: str):
        """Delete a topic."""
        with self.lock:
            if topic in self.topics_info:
                # Delete partition files
                for partition in range(self.topics_info[topic]['partitions']):
                    topic_file = self._get_topic_file(topic, partition)
                    if topic_file.exists():
                        topic_file.unlink()
                
                # Remove from topics info
                del self.topics_info[topic]
                self._save_topics()
                print(f"✅ Deleted topic '{topic}'")


# Global server instance
_server = None


def get_server():
    """Get the global server instance."""
    global _server
    if _server is None:
        _server = SimpleKafkaServer()
    return _server


class SimpleKafkaProducer:
    """Simple Kafka producer that works with SimpleKafkaServer."""
    
    def __init__(self, bootstrap_servers=None, **kwargs):
        self.server = get_server()
        self.value_serializer = kwargs.get('value_serializer', lambda x: x)
        self.key_serializer = kwargs.get('key_serializer', lambda x: x)
    
    def send(self, topic: str, value=None, key=None, partition=0):
        """Send a message to a topic."""
        try:
            serialized_value = self.value_serializer(value) if value else None
            serialized_key = self.key_serializer(key) if key else None
            
            message = {
                'key': serialized_key,
                'value': json.loads(serialized_value.decode('utf-8')) if isinstance(serialized_value, bytes) else serialized_value
            }
            
            offset = self.server.send_message(topic, message, partition)
            
            # Return a future-like object
            class MockFuture:
                def __init__(self, topic, partition, offset):
                    self.topic = topic
                    self.partition = partition
                    self.offset = offset
                
                def get(self, timeout=None):
                    class MockRecordMetadata:
                        def __init__(self, topic, partition, offset):
                            self.topic = topic
                            self.partition = partition
                            self.offset = offset
                    
                    return MockRecordMetadata(self.topic, self.partition, self.offset)
            
            return MockFuture(topic, partition, offset)
            
        except Exception as e:
            print(f"Error sending message: {e}")
            raise
    
    def flush(self):
        """Flush any pending messages (no-op for simple server)."""
        pass
    
    def close(self):
        """Close the producer (no-op for simple server)."""
        pass


class SimpleKafkaConsumer:
    """Simple Kafka consumer that works with SimpleKafkaServer."""
    
    def __init__(self, *topics, bootstrap_servers=None, **kwargs):
        self.server = get_server()
        self.topics = topics
        self.value_deserializer = kwargs.get('value_deserializer', lambda x: x)
        self.auto_offset_reset = kwargs.get('auto_offset_reset', 'latest')
        self.consumer_timeout_ms = kwargs.get('consumer_timeout_ms', 1000)
        self.current_offsets = defaultdict(int)
        
        # Create topics if they don't exist
        for topic in topics:
            if topic not in self.server.list_topics():
                self.server.create_topic(topic, 1)
    
    def __iter__(self):
        """Iterate over messages."""
        start_time = time.time()
        timeout_seconds = self.consumer_timeout_ms / 1000.0
        
        while True:
            messages_found = False
            
            for topic in self.topics:
                # Get all messages from all partitions
                all_messages = []
                topic_info = self.server.get_topic_info(topic)
                
                if 'error' not in topic_info:
                    for partition in range(topic_info.get('partition_count', 1)):
                        messages = self.server.consume_messages(
                            topic, 
                            partition=partition, 
                            from_offset=self.current_offsets[f"{topic}_{partition}"],
                            limit=100
                        )
                        all_messages.extend(messages)
                
                # Sort by offset and yield new messages
                all_messages.sort(key=lambda x: x.get('_kafka_offset', 0))
                
                for message in all_messages:
                    partition_key = f"{topic}_{message.get('_kafka_partition', 0)}"
                    if message['_kafka_offset'] >= self.current_offsets[partition_key]:
                        # Create message object
                        class MockMessage:
                            def __init__(self, topic, partition, offset, value, timestamp):
                                self.topic = topic
                                self.partition = partition
                                self.offset = offset
                                self.value = value
                                self.timestamp = timestamp
                        
                        try:
                            deserialized_value = self.value_deserializer(message['value']) if message.get('value') else message
                        except:
                            deserialized_value = message
                        
                        mock_message = MockMessage(
                            topic=topic,
                            partition=message.get('_kafka_partition', 0),
                            offset=message['_kafka_offset'],
                            value=deserialized_value,
                            timestamp=int(time.time() * 1000)
                        )
                        
                        self.current_offsets[partition_key] = message['_kafka_offset'] + 1
                        messages_found = True
                        yield mock_message
            
            # Check timeout
            if time.time() - start_time > timeout_seconds:
                break
            
            if not messages_found:
                time.sleep(0.1)  # Small delay to avoid busy waiting
    
    def close(self):
        """Close the consumer (no-op for simple server)."""
        pass


def start_simple_kafka_server():
    """Start the simple Kafka server and create default topics."""
    print("🚀 Starting Simple Kafka Server (In-Memory)")
    print("=" * 50)
    
    server = get_server()
    
    # Create e-commerce topics
    topics = [
        ("ecommerce_orders", 3),
        ("ecommerce_customers", 2),
        ("ecommerce_order_items", 3),
        ("ecommerce_fraud_alerts", 1)
    ]
    
    for topic_name, partitions in topics:
        server.create_topic(topic_name, partitions)
    
    print("\n✅ Simple Kafka Server is ready!")
    print("   This is an in-memory message broker for development")
    print("   Topics created:", [topic for topic, _ in topics])
    print("   Use SimpleKafkaProducer and SimpleKafkaConsumer classes")
    
    return server


def test_simple_kafka():
    """Test the simple Kafka server."""
    print("\n🧪 Testing Simple Kafka Server")
    print("=" * 40)
    
    # Start server
    server = start_simple_kafka_server()
    
    # Test producer
    producer = SimpleKafkaProducer(
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    test_message = {
        'id': 12345,
        'message': 'Hello Simple Kafka!',
        'timestamp': datetime.now().isoformat()
    }
    
    future = producer.send('ecommerce_orders', test_message)
    metadata = future.get()
    
    print(f"✅ Message sent to {metadata.topic}:{metadata.partition}:{metadata.offset}")
    
    # Test consumer
    consumer = SimpleKafkaConsumer(
        'ecommerce_orders',
        value_deserializer=lambda m: m,
        consumer_timeout_ms=2000
    )
    
    print("📥 Testing consumer...")
    message_received = False
    
    for message in consumer:
        if message.value and message.value.get('id') == 12345:
            print(f"✅ Message received: {message.value}")
            message_received = True
            break
    
    consumer.close()
    producer.close()
    
    if message_received:
        print("\n🎉 Simple Kafka Server test PASSED!")
        return True
    else:
        print("\n❌ Simple Kafka Server test FAILED!")
        return False


if __name__ == "__main__":
    test_simple_kafka()