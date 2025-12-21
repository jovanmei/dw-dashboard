"""
Performance monitoring script for the streaming pipeline.

This script monitors the streaming pipeline performance and provides
insights into processing rates, file accumulation, and system health.
"""

import os
import time
import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, deque


class StreamingMonitor:
    """Monitor streaming pipeline performance."""
    
    def __init__(self):
        self.history = defaultdict(deque)
        self.start_time = datetime.now()
        
    def get_directory_stats(self, path):
        """Get statistics for a directory."""
        if not os.path.exists(path):
            return {'files': 0, 'size_mb': 0, 'latest_file': None}
        
        files = []
        total_size = 0
        
        try:
            for root, dirs, filenames in os.walk(path):
                for filename in filenames:
                    if filename.endswith(('.json', '.parquet')):
                        filepath = os.path.join(root, filename)
                        try:
                            stat = os.stat(filepath)
                            files.append({
                                'path': filepath,
                                'size': stat.st_size,
                                'mtime': stat.st_mtime
                            })
                            total_size += stat.st_size
                        except (OSError, FileNotFoundError):
                            pass
        except Exception:
            pass
        
        latest_file = max(files, key=lambda x: x['mtime']) if files else None
        
        return {
            'files': len(files),
            'size_mb': total_size / (1024 * 1024),
            'latest_file': datetime.fromtimestamp(latest_file['mtime']) if latest_file else None
        }
    
    def collect_metrics(self):
        """Collect current metrics."""
        timestamp = datetime.now()
        
        # Directory statistics
        dirs = {
            'input': 'data/streaming/orders',
            'bronze': 'lake/bronze/orders_streaming',
            'silver': 'lake/silver/orders_enriched_streaming',
            'silver_invalid': 'lake/silver/orders_enriched_streaming_invalid_dates'
        }
        
        metrics = {'timestamp': timestamp}
        
        for name, path in dirs.items():
            stats = self.get_directory_stats(path)
            metrics[name] = stats
            
            # Keep history for trend analysis
            self.history[f'{name}_files'].append((timestamp, stats['files']))
            self.history[f'{name}_size'].append((timestamp, stats['size_mb']))
            
            # Keep only last 100 measurements
            if len(self.history[f'{name}_files']) > 100:
                self.history[f'{name}_files'].popleft()
                self.history[f'{name}_size'].popleft()
        
        return metrics
    
    def calculate_rates(self, metrics):
        """Calculate processing rates."""
        rates = {}
        
        for metric_name in ['input_files', 'bronze_files', 'silver_files']:
            history = self.history[metric_name]
            
            if len(history) >= 2:
                # Calculate rate over last 5 measurements
                recent = list(history)[-5:]
                if len(recent) >= 2:
                    time_diff = (recent[-1][0] - recent[0][0]).total_seconds()
                    file_diff = recent[-1][1] - recent[0][1]
                    
                    if time_diff > 0:
                        rates[metric_name.replace('_files', '_rate')] = file_diff / time_diff * 60  # files per minute
                    else:
                        rates[metric_name.replace('_files', '_rate')] = 0
                else:
                    rates[metric_name.replace('_files', '_rate')] = 0
            else:
                rates[metric_name.replace('_files', '_rate')] = 0
        
        return rates
    
    def detect_issues(self, metrics, rates):
        """Detect potential issues."""
        issues = []
        
        # Check for file accumulation in input
        if metrics['input']['files'] > 1000:
            issues.append(f"⚠️ High input file count: {metrics['input']['files']} files")
        
        # Check for processing lag
        if metrics['input']['latest_file'] and metrics['bronze']['latest_file']:
            input_age = datetime.now() - metrics['input']['latest_file']
            bronze_age = datetime.now() - metrics['bronze']['latest_file']
            
            if input_age.total_seconds() < 60 and bronze_age.total_seconds() > 300:
                issues.append("⚠️ Processing lag detected (input newer than bronze by >5min)")
        
        # Check processing rates
        if rates.get('bronze_rate', 0) < rates.get('input_rate', 0) * 0.5:
            issues.append("⚠️ Bronze processing slower than input generation")
        
        # Check for Silver layer issues
        if metrics['bronze']['files'] > 0 and metrics['silver']['files'] == 0:
            issues.append("⚠️ No Silver layer data despite Bronze data existing")
        
        # Check disk space
        total_size = sum(m['size_mb'] for m in metrics.values() if isinstance(m, dict) and 'size_mb' in m)
        if total_size > 1000:  # > 1GB
            issues.append(f"⚠️ High disk usage: {total_size:.1f} MB")
        
        return issues
    
    def print_status(self, metrics, rates, issues):
        """Print current status."""
        print(f"\n📊 Streaming Pipeline Status - {metrics['timestamp'].strftime('%H:%M:%S')}")
        print("=" * 70)
        
        # File counts and sizes
        print(f"{'Directory':<20} {'Files':<8} {'Size (MB)':<12} {'Latest File':<20}")
        print("-" * 70)
        
        for name in ['input', 'bronze', 'silver', 'silver_invalid']:
            data = metrics[name]
            latest_str = data['latest_file'].strftime('%H:%M:%S') if data['latest_file'] else 'None'
            print(f"{name.title():<20} {data['files']:<8} {data['size_mb']:<12.2f} {latest_str:<20}")
        
        # Processing rates
        print(f"\n📈 Processing Rates (files/minute)")
        print("-" * 40)
        for rate_name, rate_value in rates.items():
            print(f"{rate_name.replace('_', ' ').title():<20} {rate_value:<8.1f}")
        
        # Issues
        if issues:
            print(f"\n⚠️ Issues Detected:")
            for issue in issues:
                print(f"  {issue}")
        else:
            print(f"\n✅ No issues detected")
        
        # Runtime
        runtime = datetime.now() - self.start_time
        print(f"\n⏱️ Monitoring runtime: {runtime}")
    
    def monitor(self, interval=30, duration=None):
        """
        Monitor the streaming pipeline.
        
        Parameters
        ----------
        interval : int
            Seconds between measurements
        duration : int, optional
            Total monitoring duration in seconds
        """
        print("🔍 Starting Streaming Pipeline Monitor")
        print(f"   Interval: {interval} seconds")
        if duration:
            print(f"   Duration: {duration} seconds")
        print("   Press Ctrl+C to stop")
        
        end_time = datetime.now() + timedelta(seconds=duration) if duration else None
        
        try:
            while True:
                # Collect metrics
                metrics = self.collect_metrics()
                rates = self.calculate_rates(metrics)
                issues = self.detect_issues(metrics, rates)
                
                # Display status
                self.print_status(metrics, rates, issues)
                
                # Check if we should stop
                if end_time and datetime.now() >= end_time:
                    break
                
                # Wait for next measurement
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n\n🛑 Monitoring stopped by user")
        
        print(f"\n📊 Final Summary:")
        print(f"   Total monitoring time: {datetime.now() - self.start_time}")
        print(f"   Measurements taken: {len(self.history.get('input_files', []))}")


def main():
    """Main monitoring function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor streaming pipeline performance")
    parser.add_argument("--interval", type=int, default=30, help="Monitoring interval in seconds")
    parser.add_argument("--duration", type=int, help="Total monitoring duration in seconds")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    
    args = parser.parse_args()
    
    monitor = StreamingMonitor()
    
    if args.once:
        # Single measurement
        metrics = monitor.collect_metrics()
        rates = monitor.calculate_rates(metrics)
        issues = monitor.detect_issues(metrics, rates)
        monitor.print_status(metrics, rates, issues)
    else:
        # Continuous monitoring
        monitor.monitor(interval=args.interval, duration=args.duration)


if __name__ == "__main__":
    main()