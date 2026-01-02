#!/usr/bin/env python3
"""Test script to verify Simple Kafka imports."""

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print("Testing Simple Kafka imports...")

try:
    from src.streaming.simple.server import get_server, SimpleKafkaConsumer, start_simple_kafka_server
    print("✓ Direct import successful")
except ImportError as e:
    print(f"✗ Direct import failed: {e}")
    import traceback
    traceback.print_exc()

try:
    # Test importing just the module
    from src.streaming.simple import server
    print("✓ Module import successful")
except ImportError as e:
    print(f"✗ Module import failed: {e}")
    import traceback
    traceback.print_exc()

try:
    # Test the REST endpoint
    import requests
    response = requests.get("http://localhost:5051/topics", timeout=2)
    print(f"✓ REST API call successful: {response.status_code}")
    print(f"Topics: {response.json()}")
except Exception as e:
    print(f"✗ REST API call failed: {e}")
