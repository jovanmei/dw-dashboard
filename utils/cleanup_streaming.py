"""
Cleanup script for streaming data directories.

This script helps manage streaming data by cleaning up old files
and resetting the pipeline for fresh starts.
"""

import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta


def get_directory_size(path):
    """Calculate total size of directory in MB."""
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                try:
                    total_size += os.path.getsize(filepath)
                except (OSError, FileNotFoundError):
                    pass
    except Exception:
        pass
    return total_size / (1024 * 1024)  # Convert to MB


def count_files(path, pattern="*"):
    """Count files matching pattern in directory."""
    try:
        path_obj = Path(path)
        if path_obj.exists():
            return len(list(path_obj.rglob(pattern)))
    except Exception:
        pass
    return 0


def cleanup_directory(path, keep_recent=False, hours=1):
    """
    Clean up a directory.
    
    Parameters
    ----------
    path : str
        Directory path to clean
    keep_recent : bool
        If True, keep files modified in last N hours
    hours : int
        Number of hours to keep if keep_recent=True
    """
    if not os.path.exists(path):
        print(f"  ⚠️ Directory doesn't exist: {path}")
        return 0
    
    deleted_count = 0
    cutoff_time = datetime.now() - timedelta(hours=hours) if keep_recent else None
    
    try:
        if keep_recent:
            # Delete old files only
            for root, dirs, files in os.walk(path):
                for file in files:
                    filepath = os.path.join(root, file)
                    try:
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
                        if file_mtime < cutoff_time:
                            os.remove(filepath)
                            deleted_count += 1
                    except (OSError, FileNotFoundError):
                        pass
            
            # Remove empty directories
            for root, dirs, files in os.walk(path, topdown=False):
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    try:
                        if not os.listdir(dir_path):
                            os.rmdir(dir_path)
                    except (OSError, FileNotFoundError):
                        pass
        else:
            # Delete entire directory
            shutil.rmtree(path)
            print(f"  ✅ Deleted: {path}")
            return -1  # Indicate full deletion
            
    except Exception as e:
        print(f"  ❌ Error cleaning {path}: {e}")
        return 0
    
    return deleted_count


def show_status():
    """Show current status of streaming directories."""
    print("📊 Current Streaming Data Status")
    print("=" * 60)
    
    directories = {
        'Input (Orders)': 'data/streaming/orders',
        'Input (Customers)': 'data/streaming/customers',
        'Bronze Layer': 'lake/bronze/orders_streaming',
        'Silver Layer': 'lake/silver/orders_enriched_streaming',
        'Silver Invalid': 'lake/silver/orders_enriched_streaming_invalid_dates',
        'Checkpoints': 'checkpoints/streaming'
    }
    
    total_size = 0
    total_files = 0
    
    for name, path in directories.items():
        if os.path.exists(path):
            size_mb = get_directory_size(path)
            file_count = count_files(path)
            total_size += size_mb
            total_files += file_count
            print(f"{name:20} | {file_count:6} files | {size_mb:8.2f} MB")
        else:
            print(f"{name:20} | Not found")
    
    print("=" * 60)
    print(f"{'TOTAL':20} | {total_files:6} files | {total_size:8.2f} MB")
    print()


def cleanup_old_input_files(hours=1):
    """Clean up old input files (keep recent ones)."""
    print(f"\n🧹 Cleaning up input files older than {hours} hour(s)...")
    
    orders_deleted = cleanup_directory('data/streaming/orders', keep_recent=True, hours=hours)
    customers_deleted = cleanup_directory('data/streaming/customers', keep_recent=True, hours=hours)
    
    if orders_deleted >= 0:
        print(f"  ✅ Deleted {orders_deleted} old order files")
    if customers_deleted >= 0:
        print(f"  ✅ Deleted {customers_deleted} old customer files")


def cleanup_processed_data():
    """Clean up Bronze and Silver layers."""
    print("\n🧹 Cleaning up processed data (Bronze/Silver)...")
    
    paths = [
        'lake/bronze/orders_streaming',
        'lake/silver/orders_enriched_streaming',
        'lake/silver/orders_enriched_streaming_invalid_dates'
    ]
    
    for path in paths:
        if os.path.exists(path):
            cleanup_directory(path, keep_recent=False)


def reset_checkpoints():
    """Reset streaming checkpoints (forces pipeline to reprocess all data)."""
    print("\n🧹 Resetting checkpoints...")
    
    checkpoint_path = 'checkpoints/streaming'
    if os.path.exists(checkpoint_path):
        cleanup_directory(checkpoint_path, keep_recent=False)
        print("  ✅ Checkpoints reset")
    else:
        print("  ⚠️ No checkpoints found")


def full_cleanup():
    """Perform full cleanup of all streaming data."""
    print("\n🧹 Performing FULL cleanup...")
    print("⚠️ This will delete ALL streaming data!")
    
    confirm = input("Are you sure? Type 'yes' to confirm: ")
    if confirm.lower() != 'yes':
        print("  ❌ Cleanup cancelled")
        return
    
    paths = [
        'data/streaming/orders',
        'data/streaming/customers',
        'lake/bronze/orders_streaming',
        'lake/silver/orders_enriched_streaming',
        'lake/silver/orders_enriched_streaming_invalid_dates',
        'checkpoints/streaming'
    ]
    
    for path in paths:
        if os.path.exists(path):
            cleanup_directory(path, keep_recent=False)
    
    print("  ✅ Full cleanup completed")


def main():
    """Main cleanup menu."""
    print("🧹 Streaming Data Cleanup Tool")
    print("=" * 60)
    
    # Show current status
    show_status()
    
    # Menu
    print("Cleanup Options:")
    print("  1. Clean old input files (keep last 1 hour)")
    print("  2. Clean old input files (keep last 6 hours)")
    print("  3. Clean processed data (Bronze/Silver)")
    print("  4. Reset checkpoints only")
    print("  5. FULL cleanup (delete everything)")
    print("  6. Show status only")
    print("  7. Exit")
    
    try:
        choice = input("\nEnter choice (1-7): ").strip()
        
        if choice == "1":
            cleanup_old_input_files(hours=1)
            print("\n📊 Updated Status:")
            show_status()
            
        elif choice == "2":
            cleanup_old_input_files(hours=6)
            print("\n📊 Updated Status:")
            show_status()
            
        elif choice == "3":
            cleanup_processed_data()
            print("\n📊 Updated Status:")
            show_status()
            
        elif choice == "4":
            reset_checkpoints()
            print("\n📊 Updated Status:")
            show_status()
            
        elif choice == "5":
            full_cleanup()
            print("\n📊 Updated Status:")
            show_status()
            
        elif choice == "6":
            print("Status already shown above")
            
        elif choice == "7":
            print("Exiting cleanup tool")
            
        else:
            print("Invalid choice")
    
    except KeyboardInterrupt:
        print("\n\nCleanup interrupted")
    
    print("\n💡 Recommendations:")
    print("  - Run cleanup regularly to prevent disk space issues")
    print("  - Keep input files for 1-6 hours depending on your needs")
    print("  - Reset checkpoints if you want to reprocess data")
    print("  - Stop streaming pipeline before full cleanup")


if __name__ == "__main__":
    main()
