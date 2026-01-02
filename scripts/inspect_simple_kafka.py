import sqlite3
import pandas as pd
from pathlib import Path
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

DB_PATH = Path(project_root) / "simple_kafka_data" / "kafka_broker.db"

def get_connection():
    if not DB_PATH.exists():
        print(f"[ERROR] Database not found at {DB_PATH}")
        print("Make sure the Simple Kafka server has been started at least once.")
        sys.exit(1)
    return sqlite3.connect(DB_PATH)

def list_topics():
    print("\n--- TOPICS ---")
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM topics", conn)
    print(df if not df.empty else "No topics found.")
    conn.close()

def view_messages(topic=None, limit=10):
    print(f"\n--- LATEST {limit} MESSAGES ---")
    conn = get_connection()
    query = "SELECT * FROM messages"
    if topic:
        query += f" WHERE topic = '{topic}'"
    query += f" ORDER BY id DESC LIMIT {limit}"
    
    df = pd.read_sql_query(query, conn)
    if not df.empty:
        # Try to parse JSON values for better display
        import json
        def try_format_json(val):
            try:
                return json.dumps(json.loads(val), indent=2)
            except:
                return val
        
        # df['value'] = df['value'].apply(try_format_json)
        print(df)
    else:
        print("No messages found.")
    conn.close()

def view_offsets():
    print("\n--- CONSUMER OFFSETS (Group Progress) ---")
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM consumer_offsets", conn)
    print(df if not df.empty else "No offsets recorded.")
    conn.close()

def main():
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "topics":
            list_topics()
        elif cmd == "messages":
            topic = sys.argv[2] if len(sys.argv) > 2 else None
            view_messages(topic)
        elif cmd == "offsets":
            view_offsets()
        else:
            print("Usage: python scripts/inspect_simple_kafka.py [topics|messages|offsets]")
    else:
        # Default: show summary
        list_topics()
        view_offsets()
        view_messages(limit=5)

if __name__ == "__main__":
    main()
