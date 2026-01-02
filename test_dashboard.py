#!/usr/bin/env python3
"""Test script to run dashboard code with debugging."""

import sys
import os
import traceback

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print("Testing dashboard code...")

try:
    # Import the dashboard module
    from src.dashboards import broker_monitor
    print("✓ Dashboard module imported successfully")
    
    # Test the server status function
    print("Testing get_simple_kafka_status()...")
    status = broker_monitor.get_simple_kafka_status()
    print(f"✓ Server status: {status}")
    
    # Test the messages loading function
    print("Testing load_simple_kafka_messages()...")
    df = broker_monitor.load_simple_kafka_messages('ecommerce_orders', limit=10)
    if df is not None:
        print(f"✓ Loaded {len(df)} messages")
    else:
        print("✗ Failed to load messages")
        
    print("All tests completed successfully!")
    
except Exception as e:
    print(f"✗ Error: {e}")
    print(f"Traceback: {traceback.format_exc()}")
    sys.exit(1)
