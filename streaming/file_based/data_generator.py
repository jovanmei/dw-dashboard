"""
Real-time streaming data generator for e-commerce events.

This module generates continuous streams of e-commerce events (orders, customers, etc.)
that can be consumed by real-time data pipelines. Supports both Kafka and file-based streaming.
"""

from __future__ import annotations

import json
import random
import time
import os
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

# Conditional Kafka import - only needed for Kafka mode
try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    # Create dummy classes for type hints
    class KafkaProducer:
        pass
    class KafkaError(Exception):
        pass


class StreamingDataGenerator:
    """
    Generates real-time e-commerce events and streams them to Kafka or files.
    """
    
    def __init__(self, kafka_bootstrap_servers: Optional[str] = None, 
                 use_kafka: bool = False):
        """
        Initialize the streaming data generator.
        
        Parameters
        ----------
        kafka_bootstrap_servers : str, optional
            Kafka bootstrap servers (e.g., "localhost:9092")
        use_kafka : bool
            If True, stream to Kafka. If False, write to files for Spark file streaming.
        """
        self.use_kafka = use_kafka
        self.producer = None
        
        if use_kafka and kafka_bootstrap_servers:
            if not KAFKA_AVAILABLE:
                print("[WARN] kafka-python not installed. Cannot use Kafka mode.")
                print("   Install with: pip install kafka-python")
                print("   Falling back to file-based streaming")
                self.use_kafka = False
                return
            
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: str(k).encode('utf-8') if k else None
                )
                print(f"[OK] Connected to Kafka at {kafka_bootstrap_servers}")
            except Exception as e:
                print(f"[WARN] Could not connect to Kafka: {e}")
                print("   Falling back to file-based streaming")
                self.use_kafka = False
        
        # Base date for generating realistic timestamps
        self.base_date = datetime(2024, 1, 1)
        
        # Customer and product pools for realistic data
        self.customer_ids = list(range(101, 301))
        self.product_ids = list(range(201, 252))
        self.statuses = ["pending", "processing", "completed", "cancelled", "shipped"]
        
    def generate_order_event(self, order_id: int) -> dict:
        """Generate a single order event."""
        customer_id = random.choice(self.customer_ids)
        days_offset = random.randint(0, 365)
        order_date = (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat()
        
        # Generate realistic order amounts
        total_amount = round(random.uniform(10.99, 2999.99), 2)
        
        # Occasionally generate high-value or suspicious orders
        if random.random() < 0.05:  # 5% chance
            total_amount = round(random.uniform(3000, 10000), 2)
        
        return {
            "event_type": "order",
            "event_timestamp": datetime.now().isoformat(),
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date,
            "status": random.choice(self.statuses),
            "total_amount": total_amount,
            "source_system": random.choice(["web_portal", "mobile_app", "api", "legacy_db"])
        }
    
    def generate_order_item_event(self, order_id: int, item_id: int) -> dict:
        """Generate a single order item event."""
        product_id = random.choice(self.product_ids)
        quantity = random.randint(1, 10)
        price = round(random.uniform(5.99, 999.99), 2)
        
        # Occasionally generate suspicious quantities
        if random.random() < 0.03:  # 3% chance
            quantity = random.randint(20, 50)
        
        return {
            "event_type": "order_item",
            "event_timestamp": datetime.now().isoformat(),
            "item_id": item_id,
            "order_id": order_id,
            "product_id": product_id,
            "quantity": quantity,
            "price": price,
            "discount_percent": round(random.choice([0, 5, 10, 15, 20]), 2),
            "line_total": round(quantity * price * (1 - random.choice([0, 0.05, 0.1, 0.15, 0.2])), 2)
        }
    
    def generate_customer_event(self, customer_id: int, event_type: str = "update") -> dict:
        """Generate a customer event (new customer or update)."""
        first_names = ["Alice", "Bob", "Carol", "David", "Emma", "Frank", "Grace", "Henry"]
        last_names = ["Johnson", "Smith", "White", "Brown", "Davis", "Miller"]
        
        return {
            "event_type": f"customer_{event_type}",
            "event_timestamp": datetime.now().isoformat(),
            "customer_id": customer_id,
            "name": f"{random.choice(first_names)} {random.choice(last_names)}",
            "email": f"customer{customer_id}@example.com",
            "join_date": (datetime.now() - timedelta(days=random.randint(0, 1000))).isoformat(),
            "segment": random.choice(["Premium", "Regular", "VIP", "Standard"]),
            "phone": f"+1-555-{random.randint(100,999)}-{random.randint(1000,9999)}"
        }
    
    def stream_to_kafka(self, topic: str, event: dict, key: Optional[str] = None):
        """Send event to Kafka topic."""
        if not KAFKA_AVAILABLE:
            raise RuntimeError("Kafka not available. Install kafka-python to use Kafka mode.")
        if not self.producer:
            raise RuntimeError("Kafka producer not initialized")
        
        try:
            future = self.producer.send(topic, value=event, key=key)
            # Optionally wait for confirmation
            # future.get(timeout=10)
        except Exception as e:
            print(f"Error sending to Kafka: {e}")
    
    def stream_to_file(self, filepath: str, event: dict):
        """Append event to file (for Spark file streaming)."""
        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        try:
            with open(filepath, 'a', encoding='utf-8') as f:
                f.write(json.dumps(event) + '\n')
        except Exception as e:
            print(f"Error writing to file {filepath}: {e}")
    
    def start_order_stream(self, output_path: str = "data/streaming/orders", 
                          interval_seconds: float = 2.0,
                          kafka_topic: Optional[str] = None,
                          duration_seconds: Optional[int] = None):
        """
        Start streaming order events continuously.
        
        Parameters
        ----------
        output_path : str
            File path for file-based streaming (Spark will read from this directory)
        interval_seconds : float
            Time between events in seconds
        kafka_topic : str, optional
            Kafka topic name if using Kafka
        duration_seconds : int, optional
            How long to stream (None = infinite)
        """
        import os
        
        # Create directory if it doesn't exist
        if not self.use_kafka:
            os.makedirs(output_path, exist_ok=True)
        
        order_id = 1000  # Start from a high number to avoid conflicts
        start_time = time.time()
        
        print(f"[START] Starting order stream...")
        print(f"   Mode: {'Kafka' if self.use_kafka else 'File-based'}")
        print(f"   Interval: {interval_seconds}s")
        print(f"   Press Ctrl+C to stop")
        
        try:
            while True:
                # Check duration limit
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    print(f"\n[OK] Stream completed after {duration_seconds} seconds")
                    break
                
                # Generate order event
                order_event = self.generate_order_event(order_id)
                
                if self.use_kafka and kafka_topic:
                    self.stream_to_kafka(kafka_topic, order_event, key=str(order_id))
                else:
                    # Write to file with timestamp for ordering
                    filename = f"{output_path}/orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    self.stream_to_file(filename, order_event)
                
                # Generate 1-5 order items per order
                num_items = random.randint(1, 5)
                for item_idx in range(num_items):
                    item_event = self.generate_order_item_event(order_id, item_idx + 1)
                    
                    if self.use_kafka and kafka_topic:
                        self.stream_to_kafka(f"{kafka_topic}_items", item_event, key=str(order_id))
                    else:
                        items_filename = f"{output_path}/order_items_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{item_idx}.json"
                        self.stream_to_file(items_filename, item_event)
                
                order_id += 1
                
                # Add some randomness to interval
                sleep_time = interval_seconds * random.uniform(0.5, 1.5)
                time.sleep(sleep_time)
                
                # Print progress every 10 orders
                if order_id % 10 == 0:
                    print(f"   Generated {order_id - 1000} orders...")
                    
        except KeyboardInterrupt:
            print(f"\n[STOP] Stream stopped by user. Generated {order_id - 1000} orders total.")
        finally:
            if self.producer:
                self.producer.close()
    
    def start_customer_stream(self, output_path: str = "data/streaming/customers",
                             interval_seconds: float = 10.0,
                             kafka_topic: Optional[str] = None):
        """Start streaming customer events (less frequent than orders)."""
        import os
        
        if not self.use_kafka:
            os.makedirs(output_path, exist_ok=True)
        
        customer_id = 1000
        print(f"[START] Starting customer stream...")
        
        try:
            while True:
                customer_event = self.generate_customer_event(customer_id, "update")
                
                if self.use_kafka and kafka_topic:
                    self.stream_to_kafka(kafka_topic, customer_event, key=str(customer_id))
                else:
                    filename = f"{output_path}/customers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    self.stream_to_file(filename, customer_event)
                
                customer_id += 1
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print(f"\n[STOP] Customer stream stopped.")


def main():
    """Example usage of the streaming data generator."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate real-time e-commerce events")
    parser.add_argument("--mode", choices=["kafka", "file"], default="file",
                       help="Streaming mode: kafka or file")
    parser.add_argument("--kafka-servers", default="localhost:9092",
                       help="Kafka bootstrap servers")
    parser.add_argument("--interval", type=float, default=2.0,
                       help="Seconds between events")
    parser.add_argument("--duration", type=int, default=None,
                       help="Duration in seconds (None = infinite)")
    parser.add_argument("--topic", default="ecommerce_orders",
                       help="Kafka topic name")
    
    args = parser.parse_args()
    
    generator = StreamingDataGenerator(
        kafka_bootstrap_servers=args.kafka_servers if args.mode == "kafka" else None,
        use_kafka=(args.mode == "kafka")
    )
    
    generator.start_order_stream(
        output_path="data/streaming/orders",
        interval_seconds=args.interval,
        kafka_topic=args.topic if args.mode == "kafka" else None,
        duration_seconds=args.duration
    )


if __name__ == "__main__":
    main()


