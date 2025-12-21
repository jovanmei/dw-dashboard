"""
Kafka monitoring dashboard for the streaming pipeline.

This script provides real-time monitoring of Kafka topics, consumer lag,
and message throughput for the e-commerce streaming pipeline.
"""

import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict, deque


class KafkaMonitor:
    """Monitor Kafka topics and consumer performance."""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.topics = [
            "ecommerce_orders",
            "ecommerce_customers", 
            "ecommerce_order_items",
            "ecommerce_fraud_alerts"
        ]
        self.message_history = defaultdict(deque)
        self.consumer_lag_history = defaultdict(deque)
        
    def check_kafka_connection(self):
        """Check if Kafka is accessible."""
        try:
            from kafka import KafkaProducer
            from kafka.errors import NoBrokersAvailable
            
            producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                request_timeout_ms=5000
            )
            producer.close()
            return True
            
        except ImportError:
            print("❌ kafka-python not installed")
            return False
        except NoBrokersAvailable:
            print("❌ Kafka server not accessible")
            return False
        except Exception as e:
            print(f"❌ Kafka connection error: {e}")
            return False
    
    def get_topic_metadata(self):
        """Get metadata for all topics."""
        try:
            from kafka import KafkaConsumer
            
            consumer = KafkaConsumer(
                bootstrap_servers=[self.bootstrap_servers],
                consumer_timeout_ms=5000
            )
            
            metadata = {}
            for topic in self.topics:
                try:
                    partitions = consumer.partitions_for_topic(topic)
                    if partitions:
                        metadata[topic] = {
                            'partitions': len(partitions),
                            'partition_ids': list(partitions)
                        }
                    else:
                        metadata[topic] = {
                            'partitions': 0,
                            'partition_ids': [],
                            'error': 'Topic not found'
                        }
                except Exception as e:
                    metadata[topic] = {
                        'partitions': 0,
                        'partition_ids': [],
                        'error': str(e)
                    }
            
            consumer.close()
            return metadata
            
        except Exception as e:
            print(f"Error getting topic metadata: {e}")
            return {}
    
    def get_topic_offsets(self):
        """Get current offsets for all topic partitions."""
        try:
            from kafka import KafkaConsumer
            from kafka.structs import TopicPartition
            
            consumer = KafkaConsumer(
                bootstrap_servers=[self.bootstrap_servers],
                consumer_timeout_ms=5000
            )
            
            offsets = {}
            for topic in self.topics:
                try:
                    partitions = consumer.partitions_for_topic(topic)
                    if partitions:
                        topic_partitions = [TopicPartition(topic, p) for p in partitions]
                        
                        # Get latest offsets (high water mark)
                        end_offsets = consumer.end_offsets(topic_partitions)
                        
                        # Get earliest offsets
                        beginning_offsets = consumer.beginning_offsets(topic_partitions)
                        
                        offsets[topic] = {
                            'partitions': {}
                        }
                        
                        total_messages = 0
                        for tp in topic_partitions:
                            partition_id = tp.partition
                            latest = end_offsets.get(tp, 0)
                            earliest = beginning_offsets.get(tp, 0)
                            messages = latest - earliest
                            total_messages += messages
                            
                            offsets[topic]['partitions'][partition_id] = {
                                'earliest_offset': earliest,
                                'latest_offset': latest,
                                'messages': messages
                            }
                        
                        offsets[topic]['total_messages'] = total_messages
                    else:
                        offsets[topic] = {'error': 'Topic not found', 'total_messages': 0}
                        
                except Exception as e:
                    offsets[topic] = {'error': str(e), 'total_messages': 0}
            
            consumer.close()
            return offsets
            
        except Exception as e:
            print(f"Error getting topic offsets: {e}")
            return {}
    
    def calculate_message_rates(self, current_offsets: Dict):
        """Calculate message production rates."""
        timestamp = datetime.now()
        rates = {}
        
        for topic, data in current_offsets.items():
            if 'error' not in data:
                total_messages = data.get('total_messages', 0)
                
                # Store in history
                self.message_history[topic].append((timestamp, total_messages))
                
                # Keep only last 10 measurements
                if len(self.message_history[topic]) > 10:
                    self.message_history[topic].popleft()
                
                # Calculate rate (messages per minute)
                history = list(self.message_history[topic])
                if len(history) >= 2:
                    time_diff = (history[-1][0] - history[0][0]).total_seconds()
                    message_diff = history[-1][1] - history[0][1]
                    
                    if time_diff > 0:
                        rates[topic] = (message_diff / time_diff) * 60  # messages per minute
                    else:
                        rates[topic] = 0
                else:
                    rates[topic] = 0
            else:
                rates[topic] = 0
        
        return rates
    
    def sample_messages(self, topic: str, max_messages: int = 5):
        """Sample recent messages from a topic."""
        try:
            from kafka import KafkaConsumer
            
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[self.bootstrap_servers],
                consumer_timeout_ms=3000,
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
            )
            
            messages = []
            start_time = time.time()
            
            for message in consumer:
                if message.value:
                    messages.append({
                        'partition': message.partition,
                        'offset': message.offset,
                        'timestamp': datetime.fromtimestamp(message.timestamp / 1000) if message.timestamp else None,
                        'value': message.value
                    })
                
                # Stop after max_messages or 3 seconds
                if len(messages) >= max_messages or (time.time() - start_time) > 3:
                    break
            
            consumer.close()
            return messages
            
        except Exception as e:
            print(f"Error sampling messages from {topic}: {e}")
            return []
    
    def print_status(self, metadata: Dict, offsets: Dict, rates: Dict):
        """Print current Kafka status."""
        print(f"\n📊 Kafka Status - {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 80)
        
        # Topic overview
        print(f"{'Topic':<25} {'Partitions':<12} {'Messages':<12} {'Rate/min':<12} {'Status'}")
        print("-" * 80)
        
        for topic in self.topics:
            topic_meta = metadata.get(topic, {})
            topic_offsets = offsets.get(topic, {})
            topic_rate = rates.get(topic, 0)
            
            partitions = topic_meta.get('partitions', 0)
            messages = topic_offsets.get('total_messages', 0)
            
            if 'error' in topic_meta or 'error' in topic_offsets:
                status = "❌ Error"
            elif messages > 0:
                status = "✅ Active"
            else:
                status = "⚠️ Empty"
            
            print(f"{topic:<25} {partitions:<12} {messages:<12} {topic_rate:<12.1f} {status}")
        
        # Detailed partition info
        print(f"\n📋 Partition Details:")
        print("-" * 60)
        
        for topic in self.topics:
            topic_offsets = offsets.get(topic, {})
            if 'partitions' in topic_offsets:
                print(f"\n{topic}:")
                for partition_id, partition_data in topic_offsets['partitions'].items():
                    earliest = partition_data['earliest_offset']
                    latest = partition_data['latest_offset']
                    messages = partition_data['messages']
                    print(f"  Partition {partition_id}: {earliest} → {latest} ({messages} messages)")
    
    def monitor(self, interval: int = 30, duration: Optional[int] = None, 
               sample_messages: bool = False):
        """
        Monitor Kafka topics continuously.
        
        Parameters
        ----------
        interval : int
            Seconds between checks
        duration : int, optional
            Total monitoring duration in seconds
        sample_messages : bool
            Whether to sample and display recent messages
        """
        print("🔍 Starting Kafka Monitor")
        print(f"   Servers: {self.bootstrap_servers}")
        print(f"   Topics: {', '.join(self.topics)}")
        print(f"   Interval: {interval} seconds")
        if duration:
            print(f"   Duration: {duration} seconds")
        print("   Press Ctrl+C to stop")
        
        if not self.check_kafka_connection():
            print("❌ Cannot connect to Kafka. Exiting.")
            return
        
        start_time = time.time()
        end_time = start_time + duration if duration else None
        
        try:
            while True:
                # Collect metrics
                metadata = self.get_topic_metadata()
                offsets = self.get_topic_offsets()
                rates = self.calculate_message_rates(offsets)
                
                # Display status
                self.print_status(metadata, offsets, rates)
                
                # Sample messages if requested
                if sample_messages:
                    print(f"\n📨 Recent Messages:")
                    print("-" * 60)
                    
                    for topic in self.topics:
                        if offsets.get(topic, {}).get('total_messages', 0) > 0:
                            messages = self.sample_messages(topic, 2)
                            if messages:
                                print(f"\n{topic} (latest {len(messages)} messages):")
                                for msg in messages:
                                    timestamp_str = msg['timestamp'].strftime('%H:%M:%S') if msg['timestamp'] else 'N/A'
                                    print(f"  [{timestamp_str}] P{msg['partition']}:{msg['offset']} - {str(msg['value'])[:100]}...")
                
                # Check if we should stop
                if end_time and time.time() >= end_time:
                    break
                
                # Wait for next check
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n\n🛑 Monitoring stopped by user")
        
        total_time = time.time() - start_time
        print(f"\n📊 Monitoring Summary:")
        print(f"   Total time: {total_time:.1f} seconds")
        print(f"   Checks performed: {len(self.message_history.get(self.topics[0], []))}")


def main():
    """Main monitoring function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor Kafka topics for streaming pipeline")
    parser.add_argument("--kafka-servers", default="localhost:9092",
                       help="Kafka bootstrap servers")
    parser.add_argument("--interval", type=int, default=30,
                       help="Monitoring interval in seconds")
    parser.add_argument("--duration", type=int,
                       help="Total monitoring duration in seconds")
    parser.add_argument("--sample-messages", action="store_true",
                       help="Sample and display recent messages")
    parser.add_argument("--once", action="store_true",
                       help="Run once and exit")
    
    args = parser.parse_args()
    
    monitor = KafkaMonitor(args.kafka_servers)
    
    if args.once:
        # Single check
        if monitor.check_kafka_connection():
            metadata = monitor.get_topic_metadata()
            offsets = monitor.get_topic_offsets()
            rates = monitor.calculate_message_rates(offsets)
            monitor.print_status(metadata, offsets, rates)
        else:
            print("❌ Cannot connect to Kafka")
    else:
        # Continuous monitoring
        monitor.monitor(
            interval=args.interval,
            duration=args.duration,
            sample_messages=args.sample_messages
        )


if __name__ == "__main__":
    main()