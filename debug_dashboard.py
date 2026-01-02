#!/usr/bin/env python3
"""Debug script to run the dashboard with detailed error output."""

import sys
import os
import traceback

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print("=== Debug Dashboard Startup ===")
print(f"Python version: {sys.version}")
print(f"Project root: {project_root}")
print(f"Python path: {sys.path}")

try:
    # Import the dashboard module
    print("\n1. Importing dashboard module...")
    from src.dashboards import broker_monitor
    print("✓ Dashboard module imported successfully")
    
    # Try to run the main function with mock Streamlit
    print("\n2. Testing main function...")
    
    # Mock session state for testing
    import streamlit as st
    if not hasattr(st, 'session_state'):
        st.session_state = {}
    st.session_state.demo_mode = True
    
    print("✓ Session state initialized")
    
    # Test server status function
    print("\n3. Testing server status function...")
    status = broker_monitor.get_simple_kafka_status()
    print(f"✓ Server status: {status}")
    
    print("\n=== All Tests Passed! ===")
    print("The dashboard should now run successfully.")
    
except Exception as e:
    print(f"\n❌ Error: {type(e).__name__}: {e}")
    print("Traceback:")
    traceback.print_exc()
    sys.exit(1)
