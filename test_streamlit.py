import streamlit as st
import requests

SERVER_URL = "http://localhost:5051"

st.title("Simple Kafka Test")

st.write("Testing Simple Kafka server connection...")

# Test connection
try:
    response = requests.get(f"{SERVER_URL}/topics", timeout=2)
    st.write(f"✅ Server response: {response.status_code}")
    st.write(f"✅ Topics: {response.json()}")
    
    # Test producing a message
    if st.button("Test Produce Message"):
        test_msg = {"test": "message"}
        produce_resp = requests.post(f"{SERVER_URL}/produce/test_topic", json={"value": test_msg})
        st.write(f"Produce response: {produce_resp.status_code}")
        st.write(f"Produce result: {produce_resp.json()}")
        
    # Test consuming messages    
    if st.button("Test Consume Messages"):
        consume_resp = requests.get(f"{SERVER_URL}/consume/test_topic", params={"partition": 0, "limit": 5})
        st.write(f"Consume response: {consume_resp.status_code}")
        st.write(f"Messages: {consume_resp.json()}")
        
except Exception as e:
    st.error(f"Error: {e}")
    import traceback
    st.code(traceback.format_exc())
