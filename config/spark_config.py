"""
Spark session factory.

Keeping Spark session creation in a separate module makes it easier to
reuse this code across jobs and to unit test transformation logic
without having to duplicate Spark configuration.
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import SparkSession

from config.settings import SPARK_CONFIG


def create_spark_session() -> SparkSession:
    """
    Create and configure a SparkSession for the ETL pipeline.
    
    Optimized for streaming workloads with large numbers of small files.
    """
    
    # Reduce Spark logging noise (optional - comment out if you want full logs)
    # These warnings are harmless but can clutter output
    logging.getLogger("org.apache.spark.util.ProcfsMetricsGetter").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.SparkContext").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.sql.SparkSession").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.executor.Executor").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.rpc").setLevel(logging.ERROR)
    
    # Create Spark session with optimized settings for streaming
    spark = (
        SparkSession.builder.appName(SPARK_CONFIG.app_name)
        .config("spark.sql.shuffle.partitions", str(SPARK_CONFIG.shuffle_partitions))
        .config("spark.driver.host", "localhost")  # Helps with Windows connection warnings
        
        # Memory optimizations
        .config("spark.driver.memory", "2g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.executor.memory", "2g")
        
        # Streaming optimizations
        .config("spark.sql.streaming.checkpointLocation.deleteOnStop", "false")
        .config("spark.sql.streaming.minBatchesToRetain", "5")
        .config("spark.sql.streaming.fileSource.cleaner.enabled", "true")
        .config("spark.sql.streaming.fileSource.cleaner.numThreads", "2")
        
        # File handling optimizations for many small files
        .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
        .config("spark.sql.files.openCostInBytes", "4194304")      # 4MB
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
        
        # Heartbeat and timeout settings to reduce warnings
        .config("spark.executor.heartbeatInterval", "20s")
        .config("spark.network.timeout", "300s")
        .config("spark.rpc.askTimeout", "300s")
        .config("spark.rpc.lookupTimeout", "300s")
        
        # Serialization optimization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        .getOrCreate()
    )
    
    # Set log level programmatically (alternative to log4j.properties)
    spark.sparkContext.setLogLevel("ERROR")  # Reduce noise from heartbeat warnings
    
    return spark


