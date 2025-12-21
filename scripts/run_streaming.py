#!/usr/bin/env python3
"""
Main launcher for real-time streaming pipeline.
"""

import sys
import os
import argparse

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    """Launch real-time streaming pipeline."""
    parser = argparse.ArgumentParser(description="Launch real-time streaming pipeline")
    parser.add_argument("--mode", choices=["file", "kafka", "simple"], default="file",
                       help="Streaming mode: file, kafka, or simple")
    parser.add_argument("--with-dashboard", action="store_true", default=True,
                       help="Start dashboard along with pipeline")
    
    args = parser.parse_args()
    
    print(f"Starting Real-Time Streaming Pipeline ({args.mode} mode)")
    print("=" * 60)
    
    try:
        if args.mode == "file":
            from streaming.file_based.run_pipeline import main as run_file_pipeline
            run_file_pipeline()
        elif args.mode == "kafka":
            from streaming.kafka.run_pipeline import main as run_kafka_pipeline
            run_kafka_pipeline()
        elif args.mode == "simple":
            from streaming.simple_kafka.run_pipeline import main as run_simple_pipeline
            run_simple_pipeline()
    except ImportError as e:
        print(f"Could not import pipeline module: {e}")
    except Exception as e:
        print(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()
