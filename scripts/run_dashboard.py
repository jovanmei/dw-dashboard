"""
Main launcher for all Streamlit dashboards.

Usage:
    python scripts/run_dashboard.py [batch|spark|simple]
"""

import subprocess
import sys
import os
import argparse

def main():
    parser = argparse.ArgumentParser(description="Run Streamlit dashboards")
    parser.add_argument("type", choices=["batch", "spark", "simple"], default="batch", nargs="?",
                       help="Dashboard type: batch (default), spark (real-time), or simple (broker monitor)")
    parser.add_argument("--port", type=int, help="Override default port")
    
    args = parser.parse_args()
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    dashboards = {
        "batch": {
            "path": os.path.join(project_root, "src", "dashboards", "batch_view.py"),
            "port": 8501,
            "name": "Batch ETL Dashboard"
        },
        "spark": {
            "path": os.path.join(project_root, "src", "dashboards", "realtime_view.py"),
            "port": 8502,
            "name": "Spark Streaming Dashboard"
        },
        "simple": {
            "path": os.path.join(project_root, "src", "dashboards", "broker_monitor.py"),
            "port": 8503,
            "name": "Simple Kafka Monitor"
        }
    }
    
    config = dashboards[args.type]
    port = args.port or config["port"]
    
    print(f"🚀 Starting {config['name']}...")
    print(f"📊 Dashboard URL: http://localhost:{port}")
    print("=" * 60)
    
    try:
        # Run streamlit
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", config["path"],
            "--server.port", str(port),
            "--server.address", "localhost"
        ], check=True)
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running dashboard: {e}")
        print("💡 Make sure Streamlit is installed: pip install streamlit")

if __name__ == "__main__":
    main()
