#!/usr/bin/env python3
"""
Simple Kafka Pipeline Runner

This script starts the complete Simple Kafka pipeline:
1. Simple Kafka server (in-memory message broker)
2. Data generator (creates test data)
3. Streamlit dashboard (for visualization)
"""

import subprocess
import sys
import time
import signal
import os
from pathlib import Path


# Ensure output is flushed immediately
def print_flush(*args, **kwargs):
    print(*args, **kwargs)
    sys.stdout.flush()


def start_simple_kafka_server():
    """Start the Simple Kafka REST server."""
    print_flush("[START] Starting Simple Kafka REST Server...")
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Use -m to run as a module so imports work correctly
    cmd = [
        sys.executable, "-m", "src.streaming.simple.server", "serve", "--port", "5051"
    ]
    
    try:
        # Set PYTHONPATH to project root so -m works
        env = os.environ.copy()
        env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")
        
        # Redirect server logs to file for debugging
        server_log_file = open("simple_kafka_server.log", "w")
        
        process = subprocess.Popen(
            cmd,
            stdout=server_log_file,
            stderr=server_log_file,
            text=True,
            env=env,
            cwd=project_root
        )
        
        # Give it a moment to start
        time.sleep(3)
        if process.poll() is not None:
            # It died immediately, check why
            server_log_file.close()
            with open("simple_kafka_server.log", "r") as f:
                err = f.read()
            print_flush(f"[ERROR] Server failed to start: {err}")
            return None
            
        print_flush("[OK] Simple Kafka Server started successfully")
        print_flush("   Logs: simple_kafka_server.log")
        return process
    except Exception as e:
        print_flush(f"[ERROR] Failed to start Simple Kafka server: {e}")
        return None


def start_data_generator():
    """Start the data generator."""
    print_flush("[START] Starting data generator...")
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    generator_path = os.path.join(project_root, "src", "streaming", "simple", "generator.py")
    
    cmd = [
        sys.executable, generator_path,
        "--interval", "2.0",
        "--burst"
    ]
    
    try:
        # Set PYTHONPATH to project root
        env = os.environ.copy()
        env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")
        
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.DEVNULL, # Avoid pipe buffer issues
            stderr=subprocess.PIPE,    # Still capture errors
            text=True,
            env=env,
            cwd=project_root
        )
        
        # Give it a moment to see if it crashes immediately
        time.sleep(2)
        if process.poll() is not None:
            _, err = process.communicate()
            print_flush(f"[ERROR] Data generator failed to start: {err}")
            return None
        
        print_flush(f"[OK] Data generator started (PID: {process.pid})")
        return process
    except Exception as e:
        print_flush(f"[ERROR] Failed to start data generator: {e}")
        return None


def start_dashboard(port=8503):
    """Start the Streamlit dashboard."""
    print_flush(f"[START] Starting dashboard on port {port}...")
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dashboard_path = os.path.join(project_root, "src", "dashboards", "broker_monitor.py")
    
    cmd = [
        sys.executable, "-m", "streamlit", "run", 
        dashboard_path,
        "--server.port", str(port),
        "--server.headless", "true"
    ]
    
    try:
        # We'll use a log file for the dashboard to capture any errors
        log_file = open("dashboard.log", "w")
        
        process = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=log_file,
            text=True
        )
        # Give it a moment to see if it crashes immediately
        print_flush(f"[INFO] Dashboard Piped to dashboard.log, waiting 5s to verify...")
        try:
            time.sleep(5)
        except IOError:
            pass # Ignore sleep interruptions
            
        if process.poll() is not None:
            print_flush(f"[ERROR] Dashboard failed to start (Exit code: {process.returncode})")
            return None
            
        print_flush(f"[OK] Dashboard started (PID: {process.pid})")
        print_flush(f"   Access at: http://localhost:{port}")
        return process
    except Exception as e:
        print_flush(f"[ERROR] Failed to start dashboard: {e}")
        return None


def main():
    """Run the complete Simple Kafka pipeline."""
    print_flush("[START] Simple Kafka Pipeline Launcher")
    print_flush("=" * 50)
    
    processes = []
    
    def cleanup():
        print_flush("\n[STOP] Stopping all processes...")
        for process in processes:
            if process and process.poll() is None:
                try:
                    # On Windows, we need to be more aggressive with sub-processes
                    if os.name == 'nt':
                        subprocess.run(['taskkill', '/F', '/T', '/PID', str(process.pid)], capture_output=True)
                    else:
                        process.terminate()
                        process.wait(timeout=5)
                except Exception:
                    try:
                        process.kill()
                    except:
                        pass
    
    def signal_handler(signum, frame):
        cleanup()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 1. Start Simple Kafka server
        server = start_simple_kafka_server()
        if not server:
            print_flush("[ERROR] Cannot continue without Simple Kafka server")
            return
        processes.append(server)
        
        # 2. Start data generator
        generator_process = start_data_generator()
        if generator_process:
            processes.append(generator_process)
        
        # Wait a bit for data to be generated
        print_flush("[INFO] Waiting for initial data generation...")
        time.sleep(5)
        
        # 3. Start dashboard
        dashboard_process = start_dashboard(port=8585)
        if dashboard_process:
            processes.append(dashboard_process)
        
        print_flush("\n[SUCCESS] Pipeline started successfully!")
        print_flush("\nAccess Points:")
        print_flush("   Dashboard: http://localhost:8585")
        print_flush("\nComponents running:")
        print_flush("   - Simple Kafka Server (in-memory message broker)")
        print_flush("   - Data Generator (creating test e-commerce data)")
        print_flush("   - Streamlit Dashboard (real-time visualization)")
        print_flush("\nPress Ctrl+C to stop all components")
        
        # Keep running and monitor processes
        process_names = ["Server", "Generator", "Dashboard"]
        print_flush("\n[INFO] Monitoring processes (Press Ctrl+C to stop)...")
        
        while True:
            time.sleep(5)
            
            # Check if any process died
            active_processes = 0
            for i, process in enumerate(processes):
                if process:
                    poll = process.poll()
                    name = process_names[i] if i < len(process_names) else f"Process {i+1}"
                    
                    if poll is not None:
                        print_flush(f"[WARN] {name} stopped unexpectedly (Exit code: {poll})")
                        
                        # Try to get some error output without blocking
                        if process.stderr:
                            try:
                                # We can't use communicate() as it might block if the process is weird
                                # Just try to read whatever is in the pipe
                                import msvcrt
                                import os
                                fd = process.stderr.fileno()
                                if os.name == 'nt':
                                    # Non-blocking read on Windows is tricky, let's just use a simple read
                                    # Since poll() is not None, the process is dead, so read() should be safe
                                    err_output = process.stderr.read()
                                    if err_output:
                                        print_flush(f"   Error details: {err_output.strip()}")
                            except Exception as e:
                                print_flush(f"   Could not read error details: {e}")
                        
                        # If it's the server or generator, we must exit
                        if name in ["Server", "Generator"]:
                            print_flush(f"[ERROR] Critical component {name} failed. Shutting down...")
                            return
                    else:
                        active_processes += 1
            
            if active_processes == 0:
                print_flush("[INFO] All processes have stopped. Exiting.")
                break
        
    except KeyboardInterrupt:
        print_flush("\n[STOP] Pipeline stopped by user")
    except Exception as e:
        print_flush(f"\n[ERROR] Pipeline error: {e}")
    finally:
        cleanup()


if __name__ == "__main__":
    main()