"""
Convenience script to run the complete real-time data engineering pipeline.

This script orchestrates:
1. Starting the data generator (streaming events)
2. Starting the Spark Structured Streaming pipeline
3. Optionally starting the real-time dashboard

Usage:
    python run_realtime_pipeline.py [--with-dashboard]
"""

import subprocess
import sys
import time
import argparse
import os
from pathlib import Path

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


def check_dependencies():
    """Check if required dependencies are installed."""
    try:
        import pyspark
        print("[OK] PySpark installed")
    except ImportError:
        print("[ERROR] PySpark not found. Install with: pip install pyspark")
        return False
    
    try:
        import kafka
        print("[OK] kafka-python installed")
    except ImportError:
        print("[WARN] kafka-python not found (optional for file-based streaming)")
    
    try:
        import streamlit
        # Check Streamlit version for cache_resource support
        import streamlit as st_module
        if hasattr(st_module, "cache_resource"):
            print("[OK] Streamlit installed (supports cache_resource)")
        else:
            print("[WARN] Streamlit installed but older version (cache_resource not available)")
            print("   Dashboard will work but Spark session caching disabled")
        print(f"   Streamlit version: {streamlit.__version__}")
    except ImportError:
        print("[WARN] Streamlit not found (required for dashboard)")
    
    return True


def create_directories():
    """Create necessary directories for streaming."""
    directories = [
        "data/streaming/orders",
        "data/streaming/customers",
        "checkpoints/streaming",
        "lake/bronze/orders_streaming",
        "lake/silver/orders_enriched_streaming"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"[OK] Created directory: {directory}")


def start_data_generator(interval: float = 2.0, duration: int = None):
    """Start the streaming data generator in background."""
    print("\n[START] Starting data generator...")
    cmd = [
        sys.executable, "streaming_data_generator.py",
        "--mode", "file",
        "--interval", str(interval)
    ]
    
    if duration:
        cmd.extend(["--duration", str(duration)])
    
    # Use text mode with error handling for Windows encoding issues
    return subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        encoding='utf-8',
        errors='replace'  # Replace invalid bytes instead of crashing
    )


def start_streaming_pipeline(mode: str = "file"):
    """Start the Spark Structured Streaming pipeline."""
    print("\n[START] Starting streaming pipeline...")
    cmd = [
        sys.executable, "streaming_pipeline.py",
        "--mode", mode
    ]
    
    if mode == "file":
        cmd.extend(["--input-path", "data/streaming/orders"])
    
    # Use text mode with error handling for Windows encoding issues
    return subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        encoding='utf-8',
        errors='replace'  # Replace invalid bytes instead of crashing
    )


def start_dashboard():
    """Start the Streamlit real-time dashboard."""
    print("\n[START] Starting real-time dashboard...")
    cmd = [
        sys.executable, "-m", "streamlit", "run", "app_realtime.py",
        "--server.port", "8502",
        "--server.address", "localhost"
    ]
    
    return subprocess.Popen(cmd)


def main():
    parser = argparse.ArgumentParser(
        description="Run complete real-time data engineering pipeline"
    )
    parser.add_argument("--with-dashboard", action="store_true",
                       help="Also start the real-time dashboard")
    parser.add_argument("--generator-interval", type=float, default=2.0,
                       help="Seconds between events (default: 2.0)")
    parser.add_argument("--generator-duration", type=int, default=None,
                       help="Duration to run generator in seconds (None = infinite)")
    parser.add_argument("--mode", choices=["file", "kafka"], default="file",
                       help="Streaming mode: file or kafka")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("REAL-TIME DATA ENGINEERING PIPELINE")
    print("=" * 80)
    
    # Check dependencies
    if not check_dependencies():
        print("\n❌ Missing required dependencies. Please install them first.")
        sys.exit(1)
    
    # Create directories
    print("\n📁 Creating directories...")
    create_directories()
    
    # Start components
    processes = []
    
    try:
        # Start data generator
        gen_process = start_data_generator(
            interval=args.generator_interval,
            duration=args.generator_duration
        )
        processes.append(("Data Generator", gen_process))
        
        # Wait a bit for some data to be generated
        print("\n[WAIT] Waiting 5 seconds for initial data generation...")
        time.sleep(5)
        
        # Start streaming pipeline
        pipeline_process = start_streaming_pipeline(mode=args.mode)
        processes.append(("Streaming Pipeline", pipeline_process))
        
        # Start dashboard if requested
        if args.with_dashboard:
            dashboard_process = start_dashboard()
            processes.append(("Dashboard", dashboard_process))
            print("\n[OK] Dashboard available at: http://localhost:8502")
        
        print("\n" + "=" * 80)
        print("[OK] All components started!")
        print("=" * 80)
        print("\n[STATUS] Pipeline Status:")
        print("   - Data Generator: Running")
        print("   - Streaming Pipeline: Running")
        if args.with_dashboard:
            print("   - Dashboard: http://localhost:8502")
        print("\n[TIP] Press Ctrl+C to stop all components")
        print("=" * 80)
        
        # Wait for processes
        while True:
            time.sleep(1)
            # Check if any process has died
            for name, proc in processes:
                if proc.poll() is not None:
                    print(f"\n[WARN] {name} has stopped unexpectedly")
                    print(f"   Exit code: {proc.returncode}")
                    if proc.stdout:
                        try:
                            output = proc.stdout.read()
                            # Handle both string (when encoding specified) and bytes
                            if isinstance(output, bytes):
                                output = output.decode('utf-8', errors='replace')
                            if output.strip():
                                print(f"   Output: {output[:500]}")  # Limit output length
                        except Exception as e:
                            print(f"   Could not read stdout: {e}")
                    if proc.stderr:
                        try:
                            errors = proc.stderr.read()
                            # Handle both string (when encoding specified) and bytes
                            if isinstance(errors, bytes):
                                errors = errors.decode('utf-8', errors='replace')
                            if errors.strip():
                                print(f"   Errors: {errors[:500]}")  # Limit output length
                        except Exception as e:
                            print(f"   Could not read stderr: {e}")
    
    except KeyboardInterrupt:
        print("\n\n[STOP] Stopping all components...")
        
        for name, proc in processes:
            print(f"   Stopping {name}...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                print(f"   Force killed {name}")
        
        print("[OK] All components stopped")
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        for name, proc in processes:
            proc.terminate()
        sys.exit(1)


if __name__ == "__main__":
    main()

