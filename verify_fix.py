#!/usr/bin/env python3
"""Verification script for Simple Kafka dashboard fix."""

import requests
import json

SERVER_URL = "http://localhost:5051"

def test_server_connection():
    """Test connection to Simple Kafka server."""
    print("=== Testing Simple Kafka Server Connection ===")
    
    try:
        # Test topics endpoint
        response = requests.get(f"{SERVER_URL}/topics")
        if response.status_code == 200:
            topics = response.json()
            print(f"✅ Server available at {SERVER_URL}")
            print(f"✅ Topics: {topics}")
            return True
        else:
            print(f"❌ Server returned HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Could not connect to server: {e}")
        return False

def test_message_production():
    """Test producing a message to the server."""
    print("\n=== Testing Message Production ===")
    
    try:
        test_message = {
            "event_type": "test",
            "message": "Hello, Simple Kafka!",
            "timestamp": "2023-01-01T00:00:00Z"
        }
        
        response = requests.post(
            f"{SERVER_URL}/produce/test_topic",
            json={"value": test_message}
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Message produced successfully")
            print(f"   Offset: {result.get('offset')}")
            print(f"   Topic: {result.get('topic')}")
            print(f"   Partition: {result.get('partition')}")
            return True
        else:
            print(f"❌ Failed to produce message: HTTP {response.status_code}")
            print(f"   Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error producing message: {e}")
        return False

def test_message_consumption():
    """Test consuming messages from the server."""
    print("\n=== Testing Message Consumption ===")
    
    try:
        response = requests.get(
            f"{SERVER_URL}/consume/test_topic",
            params={"partition": 0, "limit": 5}
        )
        
        if response.status_code == 200:
            messages = response.json()
            if messages:
                print(f"✅ Messages consumed successfully")
                print(f"   Found {len(messages)} messages")
                for i, msg in enumerate(messages[-2:]):  # Show last 2 messages
                    print(f"   [{i+1}] Offset {msg['offset']}: {json.dumps(msg['value'], indent=2)}")
            else:
                print(f"⚠️  No messages found in test_topic")
            return True
        else:
            print(f"❌ Failed to consume messages: HTTP {response.status_code}")
            print(f"   Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error consuming messages: {e}")
        return False

def test_topic_info():
    """Test getting topic information."""
    print("\n=== Testing Topic Information ===")
    
    try:
        response = requests.get(f"{SERVER_URL}/topics/ecommerce_orders")
        
        if response.status_code == 200:
            topic_info = response.json()
            print(f"✅ Topic info retrieved successfully")
            print(f"   Topic: ecommerce_orders")
            print(f"   Partitions: {topic_info.get('partition_count')}")
            print(f"   Total messages: {topic_info.get('total_messages')}")
            return True
        else:
            print(f"❌ Failed to get topic info: HTTP {response.status_code}")
            print(f"   Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error getting topic info: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Verifying Simple Kafka Dashboard Fix")
    print(f"Testing server at: {SERVER_URL}")
    print("=" * 50)
    
    # Run all tests
    results = []
    results.append(test_server_connection())
    results.append(test_message_production())
    results.append(test_message_consumption())
    results.append(test_topic_info())
    
    print("\n" + "=" * 50)
    print("📊 Test Results")
    print(f"✅ Passed: {results.count(True)}")
    print(f"❌ Failed: {results.count(False)}")
    
    if all(results):
        print("🎉 All tests passed! The Simple Kafka server is working correctly.")
        print("\nThe dashboard should now be able to connect in live mode.")
    else:
        print("❌ Some tests failed. Please check the server configuration.")
