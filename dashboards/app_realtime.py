"""
Real-time Streamlit Dashboard for E-Commerce Streaming Data Pipeline.

This dashboard reads directly from Bronze/Silver layers for reliable real-time visualization.
"""

from __future__ import annotations

import os
import time
import sys
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Handle Spark imports gracefully
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, sum as _sum, avg, max as _max, min as _min, window
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    st.error("PySpark not available. Please install with: pip install pyspark")


# Support both newer Streamlit (cache_resource) and older versions
if hasattr(st, "cache_resource"):
    _cache_resource_decorator = st.cache_resource
else:
    def _no_cache(func):
        return func
    _cache_resource_decorator = _no_cache


def safe_plotly_chart(fig, **kwargs):
    """Safely display plotly chart with compatibility for older Streamlit versions."""
    try:
        st.plotly_chart(fig, use_container_width=True, **kwargs)
    except TypeError:
        # Fallback for older Streamlit versions
        st.plotly_chart(fig, **kwargs)


def safe_dataframe(df, **kwargs):
    """Safely display dataframe with compatibility for older Streamlit versions."""
    try:
        st.dataframe(df, use_container_width=True, **kwargs)
    except TypeError:
        # Fallback for older Streamlit versions
        st.dataframe(df, **kwargs)


@_cache_resource_decorator
def get_spark_session():
    """Get or create Spark session for reading streaming data."""
    if not SPARK_AVAILABLE:
        return None
        
    try:
        spark = SparkSession.builder \
            .appName("StreamingDashboard") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        return spark
    except Exception as e:
        st.error(f"Could not create Spark session: {e}")
        return None


def get_pipeline_status() -> Dict[str, any]:
    """Get comprehensive pipeline status information."""
    status = {
        'generator_running': False,
        'pipeline_running': False,
        'input_files': 0,
        'bronze_files': 0,
        'silver_files': 0,
        'last_input_time': None,
        'last_bronze_time': None,
        'last_silver_time': None
    }
    
    def safe_get_mtime(file_path):
        """Safely get modification time, handling errors."""
        try:
            return file_path.stat().st_mtime
        except (OSError, FileNotFoundError, PermissionError):
            return None
    
    def filter_valid_files(files):
        """Filter out temporary and invalid files."""
        valid_files = []
        for f in files:
            # Skip temporary files and directories
            if ('_temporary' in str(f) or 
                '_SUCCESS' in f.name or 
                f.name.startswith('.') or
                f.name.startswith('_')):
                continue
            
            # Only include files that actually exist and are accessible
            if safe_get_mtime(f) is not None:
                valid_files.append(f)
        
        return valid_files
    
    # Check input directory
    input_dir = Path("data/streaming/orders")
    if input_dir.exists():
        try:
            input_files = list(input_dir.glob("*.json"))
            input_files = filter_valid_files(input_files)
            status['input_files'] = len(input_files)
            status['generator_running'] = len(input_files) > 0
            if input_files:
                mtimes = [safe_get_mtime(f) for f in input_files]
                mtimes = [t for t in mtimes if t is not None]
                if mtimes:
                    status['last_input_time'] = max(mtimes)
        except Exception:
            pass  # Directory access error, keep defaults
    
    # Check Bronze layer
    bronze_dir = Path("lake/bronze/orders_streaming")
    if bronze_dir.exists():
        try:
            bronze_files = list(bronze_dir.rglob("*.parquet"))
            bronze_files = filter_valid_files(bronze_files)
            status['bronze_files'] = len(bronze_files)
            status['pipeline_running'] = len(bronze_files) > 0
            if bronze_files:
                mtimes = [safe_get_mtime(f) for f in bronze_files]
                mtimes = [t for t in mtimes if t is not None]
                if mtimes:
                    status['last_bronze_time'] = max(mtimes)
        except Exception:
            pass  # Directory access error, keep defaults
    
    # Check Silver layer
    silver_dir = Path("lake/silver/orders_enriched_streaming")
    silver_invalid_dir = Path("lake/silver/orders_enriched_streaming_invalid_dates")
    
    silver_files = []
    
    # Check regular Silver directory
    if silver_dir.exists():
        try:
            files = list(silver_dir.rglob("*.parquet"))
            silver_files.extend(filter_valid_files(files))
        except Exception:
            pass
    
    # Check invalid dates Silver directory
    if silver_invalid_dir.exists():
        try:
            files = list(silver_invalid_dir.rglob("*.parquet"))
            silver_files.extend(filter_valid_files(files))
        except Exception:
            pass
    
    status['silver_files'] = len(silver_files)
    if silver_files:
        mtimes = [safe_get_mtime(f) for f in silver_files]
        mtimes = [t for t in mtimes if t is not None]
        if mtimes:
            status['last_silver_time'] = max(mtimes)
    
    return status


def load_bronze_orders(spark: SparkSession, limit: int = 1000) -> Optional[pd.DataFrame]:
    """Load recent orders from Bronze layer."""
    bronze_path = "lake/bronze/orders_streaming"
    if not os.path.exists(bronze_path):
        return None
        
    try:
        # Check if there are any actual parquet files (not just temporary ones)
        bronze_dir = Path(bronze_path)
        parquet_files = [f for f in bronze_dir.rglob("*.parquet") 
                        if '_temporary' not in str(f) and not f.name.startswith('_')]
        
        if not parquet_files:
            return None
        
        df = spark.read.parquet(bronze_path)
        # Sort by event_timestamp and limit
        if 'event_timestamp' in df.columns:
            df = df.orderBy(col("event_timestamp").desc()).limit(limit)
        else:
            df = df.limit(limit)
            
        pandas_df = df.toPandas()
        
        # Convert timestamp columns
        if 'event_timestamp' in pandas_df.columns:
            pandas_df['event_timestamp'] = pd.to_datetime(pandas_df['event_timestamp'], errors='coerce')
        if 'order_date' in pandas_df.columns:
            pandas_df['order_date'] = pd.to_datetime(pandas_df['order_date'], errors='coerce')
            
        return pandas_df if len(pandas_df) > 0 else None
    except Exception as e:
        # Log error but don't crash the dashboard
        print(f"Warning: Error loading Bronze data: {e}")
        return None


def load_silver_orders(spark: SparkSession, limit: int = 1000) -> Optional[pd.DataFrame]:
    """Load processed orders from Silver layer."""
    silver_path = "lake/silver/orders_enriched_streaming"
    silver_invalid_path = "lake/silver/orders_enriched_streaming_invalid_dates"
    
    dfs_to_combine = []
    
    # Try to load regular Silver data
    if os.path.exists(silver_path):
        try:
            silver_dir = Path(silver_path)
            parquet_files = [f for f in silver_dir.rglob("*.parquet") 
                            if '_temporary' not in str(f) and not f.name.startswith('_')]
            
            if parquet_files:
                df = spark.read.parquet(silver_path)
                dfs_to_combine.append(df)
        except Exception as e:
            print(f"Warning: Error loading regular Silver data: {e}")
    
    # Try to load invalid dates Silver data
    if os.path.exists(silver_invalid_path):
        try:
            silver_invalid_dir = Path(silver_invalid_path)
            parquet_files = [f for f in silver_invalid_dir.rglob("*.parquet") 
                            if '_temporary' not in str(f) and not f.name.startswith('_')]
            
            if parquet_files:
                df_invalid = spark.read.parquet(silver_invalid_path)
                dfs_to_combine.append(df_invalid)
        except Exception as e:
            print(f"Warning: Error loading invalid dates Silver data: {e}")
    
    if not dfs_to_combine:
        return None
    
    try:
        # Combine all Silver dataframes
        if len(dfs_to_combine) == 1:
            combined_df = dfs_to_combine[0]
        else:
            combined_df = dfs_to_combine[0]
            for df in dfs_to_combine[1:]:
                combined_df = combined_df.union(df)
        
        # Sort by processing_timestamp if available, otherwise event_timestamp
        if 'processing_timestamp' in combined_df.columns:
            combined_df = combined_df.orderBy(col("processing_timestamp").desc()).limit(limit)
        elif 'event_timestamp' in combined_df.columns:
            combined_df = combined_df.orderBy(col("event_timestamp").desc()).limit(limit)
        else:
            combined_df = combined_df.limit(limit)
            
        pandas_df = combined_df.toPandas()
        
        # Convert timestamp columns
        timestamp_cols = ['event_timestamp', 'order_timestamp', 'processing_timestamp', 'order_date_parsed']
        for col_name in timestamp_cols:
            if col_name in pandas_df.columns:
                pandas_df[col_name] = pd.to_datetime(pandas_df[col_name], errors='coerce')
                
        return pandas_df if len(pandas_df) > 0 else None
    except Exception as e:
        # Log error but don't crash the dashboard
        print(f"Warning: Error combining Silver data: {e}")
        return None


def calculate_realtime_metrics(df: pd.DataFrame) -> Dict[str, any]:
    """Calculate real-time metrics from orders data."""
    if df is None or len(df) == 0:
        return {}
    
    # Filter recent data (last hour)
    now = datetime.now()
    one_hour_ago = now - timedelta(hours=1)
    
    # Use event_timestamp if available, otherwise order_timestamp
    timestamp_col = 'event_timestamp' if 'event_timestamp' in df.columns else 'order_timestamp'
    if timestamp_col in df.columns:
        recent_df = df[df[timestamp_col] >= one_hour_ago]
    else:
        recent_df = df  # Use all data if no timestamp
    
    # Calculate metrics
    metrics = {
        'total_orders': len(df),
        'recent_orders': len(recent_df),
        'total_revenue': df['total_amount'].sum() if 'total_amount' in df.columns else 0,
        'recent_revenue': recent_df['total_amount'].sum() if 'total_amount' in recent_df.columns else 0,
        'avg_order_value': df['total_amount'].mean() if 'total_amount' in df.columns else 0,
        'unique_customers': df['customer_id'].nunique() if 'customer_id' in df.columns else 0,
        'completed_orders': len(df[df['status'] == 'completed']) if 'status' in df.columns else 0,
        'completion_rate': len(df[df['status'] == 'completed']) / len(df) * 100 if 'status' in df.columns and len(df) > 0 else 0
    }
    
    return metrics


def detect_fraud_from_data(df: pd.DataFrame) -> pd.DataFrame:
    """Detect potential fraud from orders data."""
    if df is None or len(df) == 0:
        return pd.DataFrame()
    
    # Create fraud scoring
    fraud_df = df.copy()
    
    # Amount-based scoring
    fraud_df['amount_score'] = 0
    if 'total_amount' in fraud_df.columns:
        fraud_df.loc[fraud_df['total_amount'] > 5000, 'amount_score'] = 4
        fraud_df.loc[(fraud_df['total_amount'] > 3000) & (fraud_df['total_amount'] <= 5000), 'amount_score'] = 3
        fraud_df.loc[(fraud_df['total_amount'] > 2000) & (fraud_df['total_amount'] <= 3000), 'amount_score'] = 2
    
    # Status-based scoring
    fraud_df['status_score'] = 0
    if 'status' in fraud_df.columns:
        fraud_df.loc[fraud_df['status'].isin(['cancelled', 'refunded']), 'status_score'] = 1
    
    # Calculate total fraud score
    fraud_df['fraud_score'] = fraud_df['amount_score'] + fraud_df['status_score']
    
    # Filter potential fraud (score >= 3)
    fraud_alerts = fraud_df[fraud_df['fraud_score'] >= 3].copy()
    
    return fraud_alerts


def create_pipeline_status_section():
    """Display pipeline status and progress."""
    st.header("🔄 Pipeline Status & Progress")
    
    try:
        status = get_pipeline_status()
    except Exception as e:
        st.error(f"Error getting pipeline status: {e}")
        st.info("This may happen if Spark is currently writing files. Please refresh in a few seconds.")
        return
    
    # Status indicators
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if status['generator_running']:
            st.success("✅ Data Generator")
            st.metric("Input Files", status['input_files'])
        else:
            st.error("❌ Data Generator")
            st.metric("Input Files", 0)
    
    with col2:
        if status['pipeline_running']:
            st.success("✅ Streaming Pipeline")
            st.metric("Bronze Files", status['bronze_files'])
        else:
            st.error("❌ Streaming Pipeline")
            st.metric("Bronze Files", 0)
    
    with col3:
        if status['silver_files'] > 0:
            st.success("✅ Data Processing")
            st.metric("Silver Files", status['silver_files'])
        else:
            st.warning("⏳ Data Processing")
            st.metric("Silver Files", 0)
    
    with col4:
        # Overall health
        if status['generator_running'] and status['pipeline_running']:
            st.success("✅ System Healthy")
        elif status['pipeline_running']:
            st.warning("⚠️ Generator Stopped")
        else:
            st.error("❌ System Down")
    
    # Timeline
    st.subheader("📊 Processing Timeline")
    
    timeline_data = []
    try:
        if status['last_input_time']:
            timeline_data.append({
                'Stage': 'Data Generation',
                'Last Activity': datetime.fromtimestamp(status['last_input_time']),
                'Status': 'Active' if status['generator_running'] else 'Stopped'
            })
        
        if status['last_bronze_time']:
            timeline_data.append({
                'Stage': 'Bronze Ingestion',
                'Last Activity': datetime.fromtimestamp(status['last_bronze_time']),
                'Status': 'Active' if status['pipeline_running'] else 'Stopped'
            })
        
        if status['last_silver_time']:
            timeline_data.append({
                'Stage': 'Silver Processing',
                'Last Activity': datetime.fromtimestamp(status['last_silver_time']),
                'Status': 'Active'
            })
        
        if timeline_data:
            timeline_df = pd.DataFrame(timeline_data)
            # Use use_container_width only if available (Streamlit >= 1.12.0)
            try:
                st.dataframe(timeline_df, use_container_width=True)
            except TypeError:
                # Fallback for older Streamlit versions
                st.dataframe(timeline_df)
        else:
            st.info("No pipeline activity detected. Start the streaming pipeline to see progress.")
    
    except Exception as e:
        st.warning(f"Could not generate timeline: {e}")
    
    # Quick start instructions
    if not status['generator_running'] or not status['pipeline_running']:
        st.subheader("🚀 Quick Start")
        st.code("""
# Option 1: All-in-one (Recommended)
python run_realtime_pipeline.py --with-dashboard

# Option 2: Manual setup
# Terminal 1: Start data generator
python streaming_data_generator.py --interval 2.0

# Terminal 2: Start streaming pipeline  
python streaming_pipeline.py --mode file
        """)
    
    # Debug information
    with st.expander("🔧 Debug Information"):
        st.json(status)


def create_realtime_metrics_section(spark: SparkSession):
    """Display comprehensive real-time metrics."""
    st.header("📊 Real-Time Metrics")
    
    # Load data from Silver layer (processed) or Bronze (raw) as fallback
    silver_df = load_silver_orders(spark, 1000)
    bronze_df = load_bronze_orders(spark, 1000) if silver_df is None else None
    
    data_df = silver_df if silver_df is not None else bronze_df
    data_source = "Silver (Processed)" if silver_df is not None else "Bronze (Raw)" if bronze_df is not None else None
    
    if data_df is not None:
        st.success(f"📈 Live data from {data_source} layer ({len(data_df):,} orders)")
        
        # Calculate metrics
        metrics = calculate_realtime_metrics(data_df)
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Orders", f"{metrics['total_orders']:,}")
        col2.metric("Total Revenue", f"${metrics['total_revenue']:,.2f}")
        col3.metric("Avg Order Value", f"${metrics['avg_order_value']:,.2f}")
        col4.metric("Completion Rate", f"{metrics['completion_rate']:.1f}%")
        
        # Recent activity (last hour)
        st.subheader("⚡ Recent Activity (Last Hour)")
        col1, col2, col3 = st.columns(3)
        col1.metric("Recent Orders", f"{metrics['recent_orders']:,}")
        col2.metric("Recent Revenue", f"${metrics['recent_revenue']:,.2f}")
        col3.metric("Unique Customers", f"{metrics['unique_customers']:,}")
        
        # Time-based analysis
        if 'event_timestamp' in data_df.columns or 'order_timestamp' in data_df.columns:
            timestamp_col = 'event_timestamp' if 'event_timestamp' in data_df.columns else 'order_timestamp'
            
            # Orders over time (last 2 hours, 5-minute buckets)
            st.subheader("📈 Orders Over Time")
            now = datetime.now()
            two_hours_ago = now - timedelta(hours=2)
            recent_data = data_df[data_df[timestamp_col] >= two_hours_ago].copy()
            
            if len(recent_data) > 0:
                # Create 5-minute buckets
                recent_data['time_bucket'] = recent_data[timestamp_col].dt.floor('5min')
                time_agg = recent_data.groupby('time_bucket').agg({
                    'order_id': 'count',
                    'total_amount': 'sum'
                }).reset_index()
                time_agg.columns = ['Time', 'Orders', 'Revenue']
                
                # Dual-axis chart
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                fig.add_trace(
                    go.Scatter(x=time_agg['Time'], y=time_agg['Orders'], 
                              name="Orders", line=dict(color='blue')),
                    secondary_y=False,
                )
                
                fig.add_trace(
                    go.Scatter(x=time_agg['Time'], y=time_agg['Revenue'], 
                              name="Revenue", line=dict(color='green')),
                    secondary_y=True,
                )
                
                fig.update_xaxes(title_text="Time")
                fig.update_yaxes(title_text="Order Count", secondary_y=False)
                fig.update_yaxes(title_text="Revenue ($)", secondary_y=True)
                fig.update_layout(title="Orders and Revenue Over Time (5-min buckets)")
                
                safe_plotly_chart(fig)
            else:
                st.info("No recent data in the last 2 hours.")
        
        # Status distribution
        if 'status' in data_df.columns:
            st.subheader("📊 Order Status Distribution")
            status_counts = data_df['status'].value_counts()
            
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(values=status_counts.values, names=status_counts.index,
                           title="Order Status Breakdown")
                safe_plotly_chart(fig)
            
            with col2:
                fig = px.bar(x=status_counts.index, y=status_counts.values,
                           title="Order Count by Status")
                fig.update_xaxes(title="Status")
                fig.update_yaxes(title="Count")
                safe_plotly_chart(fig)
        
        # Revenue distribution
        if 'total_amount' in data_df.columns:
            st.subheader("💰 Revenue Distribution")
            
            col1, col2 = st.columns(2)
            with col1:
                fig = px.histogram(data_df, x='total_amount', nbins=30,
                                 title="Order Value Distribution")
                fig.update_xaxes(title="Order Value ($)")
                fig.update_yaxes(title="Count")
                safe_plotly_chart(fig)
            
            with col2:
                # Box plot by status
                if 'status' in data_df.columns:
                    fig = px.box(data_df, x='status', y='total_amount',
                               title="Order Value by Status")
                    fig.update_xaxes(title="Status")
                    fig.update_yaxes(title="Order Value ($)")
                    safe_plotly_chart(fig)
    
    else:
        st.warning("⏳ No streaming data available yet")
        st.info("""
        **To see real-time data:**
        1. Start the data generator: `python streaming_data_generator.py`
        2. Start the streaming pipeline: `python streaming_pipeline.py`
        3. Wait 30-60 seconds for data processing
        4. Refresh this dashboard
        """)


def create_fraud_detection_section(spark: SparkSession):
    """Display fraud detection analysis."""
    st.header("🚨 Fraud Detection")
    
    # Load data
    silver_df = load_silver_orders(spark, 1000)
    bronze_df = load_bronze_orders(spark, 1000) if silver_df is None else None
    data_df = silver_df if silver_df is not None else bronze_df
    
    if data_df is not None:
        # Detect fraud
        fraud_df = detect_fraud_from_data(data_df)
        
        if len(fraud_df) > 0:
            st.error(f"🚨 {len(fraud_df)} potential fraud alerts detected!")
            
            # Fraud metrics
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Fraud Alerts", len(fraud_df))
            col2.metric("High Risk (Score ≥ 5)", len(fraud_df[fraud_df['fraud_score'] >= 5]))
            col3.metric("Fraud Amount", f"${fraud_df['total_amount'].sum():,.2f}")
            col4.metric("Avg Fraud Score", f"{fraud_df['fraud_score'].mean():.1f}")
            
            # Fraud score distribution
            st.subheader("📊 Fraud Score Distribution")
            fig = px.histogram(fraud_df, x='fraud_score', nbins=10,
                             title="Distribution of Fraud Scores")
            fig.update_xaxes(title="Fraud Score")
            fig.update_yaxes(title="Count")
            safe_plotly_chart(fig)
            
            # Fraud over time
            if 'event_timestamp' in fraud_df.columns:
                st.subheader("⏰ Fraud Alerts Timeline")
                fig = px.scatter(fraud_df, x='event_timestamp', y='fraud_score',
                               size='total_amount', color='fraud_score',
                               hover_data=['order_id', 'customer_id'],
                               title="Fraud Alerts Over Time")
                fig.update_xaxes(title="Time")
                fig.update_yaxes(title="Fraud Score")
                safe_plotly_chart(fig)
            
            # Top fraud cases
            st.subheader("🔍 Top Fraud Cases")
            fraud_display = fraud_df.sort_values('fraud_score', ascending=False).head(20)
            display_cols = ['order_id', 'customer_id', 'total_amount', 'fraud_score', 'status']
            if 'event_timestamp' in fraud_display.columns:
                display_cols.insert(2, 'event_timestamp')
            
            available_cols = [c for c in display_cols if c in fraud_display.columns]
            safe_dataframe(fraud_display[available_cols])
            
        else:
            st.success("✅ No fraud alerts detected")
            st.info("""
            **Fraud Detection Criteria:**
            - High-value orders (>$2,000): 2-4 points
            - Cancelled/refunded orders: +1 point
            - **Alert threshold**: Total score ≥ 3 points
            """)
    
    else:
        st.info("⏳ No data available for fraud analysis")


def create_recent_orders_section(spark: SparkSession):
    """Display recent orders stream."""
    st.header("📦 Recent Orders Stream")
    
    # Load recent orders
    silver_df = load_silver_orders(spark, 200)
    bronze_df = load_bronze_orders(spark, 200) if silver_df is None else None
    data_df = silver_df if silver_df is not None else bronze_df
    
    if data_df is not None:
        data_source = "Silver (Processed)" if silver_df is not None else "Bronze (Raw)"
        st.success(f"📊 Showing latest {len(data_df)} orders from {data_source} layer")
        
        # Filter controls
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'status' in data_df.columns:
                status_filter = st.selectbox("Filter by Status", 
                                           ['All'] + list(data_df['status'].unique()))
                if status_filter != 'All':
                    data_df = data_df[data_df['status'] == status_filter]
        
        with col2:
            if 'total_amount' in data_df.columns:
                min_amount = st.number_input("Min Order Amount", min_value=0.0, value=0.0)
                data_df = data_df[data_df['total_amount'] >= min_amount]
        
        with col3:
            show_count = st.selectbox("Show Orders", [50, 100, 200], index=1)
            data_df = data_df.head(show_count)
        
        # Summary metrics
        if len(data_df) > 0:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Filtered Orders", len(data_df))
            col2.metric("Total Value", f"${data_df['total_amount'].sum():,.2f}")
            col3.metric("Average Value", f"${data_df['total_amount'].mean():,.2f}")
            col4.metric("Unique Customers", data_df['customer_id'].nunique())
            
            # Orders table
            st.subheader("📋 Orders Table")
            display_cols = ['order_id', 'customer_id', 'order_date', 'status', 'total_amount']
            if 'event_timestamp' in data_df.columns:
                display_cols.append('event_timestamp')
            if 'processing_timestamp' in data_df.columns:
                display_cols.append('processing_timestamp')
            
            available_cols = [c for c in display_cols if c in data_df.columns]
            safe_dataframe(data_df[available_cols])
        else:
            st.info("No orders match the current filters.")
    
    else:
        st.info("⏳ No orders data available yet")


def main():
    """Main dashboard function."""
    st.set_page_config(
        page_title="Real-Time E-Commerce Streaming Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
        page_icon="⚡"
    )
    
    st.title("⚡ Real-Time E-Commerce Streaming Dashboard")
    st.markdown("""
    **Live Data Pipeline Dashboard** - Real-time analytics from Bronze/Silver layers
    
    This dashboard provides:
    - **Pipeline Status** - Monitor data flow and processing progress
    - **Real-Time Metrics** - Live revenue, orders, and customer analytics  
    - **Fraud Detection** - Suspicious pattern identification
    - **Order Stream** - Recent orders with filtering capabilities
    """)
    
    # Get Spark session
    spark = get_spark_session()
    
    if spark is None:
        st.error("Could not initialize Spark session. Please check your Spark installation.")
        return
    
    # Auto-refresh configuration in sidebar
    st.sidebar.header("⚙️ Dashboard Settings")
    auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 15)
    
    # Pipeline status in sidebar
    st.sidebar.markdown("---")
    st.sidebar.subheader("📡 Quick Status")
    
    try:
        status = get_pipeline_status()
        st.sidebar.write("✅ Data Generator" if status['generator_running'] else "❌ Data Generator")
        st.sidebar.write("✅ Streaming Pipeline" if status['pipeline_running'] else "❌ Streaming Pipeline")
        st.sidebar.write(f"📁 Input Files: {status['input_files']}")
        st.sidebar.write(f"📁 Bronze Files: {status['bronze_files']}")
        st.sidebar.write(f"📁 Silver Files: {status['silver_files']}")
    except Exception as e:
        st.sidebar.error("Status check failed")
        st.sidebar.write("(Files may be in use by Spark)")
    
    # Quick start in sidebar
    st.sidebar.markdown("---")
    st.sidebar.subheader("🚀 Quick Start")
    st.sidebar.code("python run_realtime_pipeline.py --with-dashboard")
    
    # Navigation tabs
    st.markdown("---")
    
    # Use tabs for better organization
    if hasattr(st, "tabs"):
        tab1, tab2, tab3, tab4 = st.tabs([
            "🔄 Pipeline Status", 
            "📊 Real-Time Metrics", 
            "🚨 Fraud Detection", 
            "📦 Recent Orders"
        ])
        
        with tab1:
            create_pipeline_status_section()
        
        with tab2:
            create_realtime_metrics_section(spark)
        
        with tab3:
            create_fraud_detection_section(spark)
        
        with tab4:
            create_recent_orders_section(spark)
    
    else:
        # Fallback for older Streamlit versions
        page = st.selectbox(
            "Choose Dashboard Section:",
            ["🔄 Pipeline Status", "📊 Real-Time Metrics", "🚨 Fraud Detection", "📦 Recent Orders"]
        )
        
        st.markdown("---")
        
        if page == "🔄 Pipeline Status":
            create_pipeline_status_section()
        elif page == "📊 Real-Time Metrics":
            create_realtime_metrics_section(spark)
        elif page == "🚨 Fraud Detection":
            create_fraud_detection_section(spark)
        elif page == "📦 Recent Orders":
            create_recent_orders_section(spark)
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(refresh_interval)
        try:
            if hasattr(st, "rerun"):
                st.rerun()
            elif hasattr(st, "experimental_rerun"):
                st.experimental_rerun()
            else:
                # Fallback: JavaScript refresh
                st.write(f'<meta http-equiv="refresh" content="{refresh_interval}">', 
                        unsafe_allow_html=True)
        except Exception:
            # If auto-refresh fails, continue without it
            pass


if __name__ == "__main__":
    main()

