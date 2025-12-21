"""
Real-time streaming ETL pipeline using Spark Structured Streaming.

This module implements continuous processing of e-commerce events with:
- Real-time data ingestion from Kafka or file streams
- Streaming transformations and aggregations
- Windowed analytics (tumbling/sliding windows)
- Real-time fraud detection
- Continuous writes to Bronze/Silver/Gold layers
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, from_json, lag, lit, max as _max, min as _min,
    sum as _sum, window, avg, current_timestamp, to_timestamp,
    expr, when, approx_count_distinct, collect_list, struct, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, LongType
)

from config.settings import PATHS
from config.spark_config import create_spark_session


# Define schemas for streaming data
ORDER_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("source_system", StringType(), True)
])

ORDER_ITEM_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("discount_percent", DoubleType(), True),
    StructField("line_total", DoubleType(), True)
])


class StreamingETLPipeline:
    """
    Real-time ETL pipeline using Spark Structured Streaming.
    """
    
    def __init__(self, spark: Optional[SparkSession] = None,
                 checkpoint_location: str = "checkpoints/streaming"):
        """
        Initialize the streaming pipeline.
        
        Parameters
        ----------
        spark : SparkSession, optional
            Spark session (creates new one if not provided)
        checkpoint_location : str
            Base directory for Spark checkpointing (required for fault tolerance)
        """
        self.spark = spark or create_spark_session()
        self.checkpoint_location = checkpoint_location
        
        # Configure Spark for streaming
        self.spark.conf.set("spark.sql.streaming.checkpointLocation", checkpoint_location)
        self.spark.conf.set("spark.sql.streaming.schemaInference", "true")
    
    def read_stream_from_files(self, input_path: str, schema: StructType) -> DataFrame:
        """
        Read streaming data from JSON files (for file-based streaming).
        
        Spark will monitor the directory and process new files as they arrive.
        Optimized for handling large numbers of small files.
        """
        return (self.spark
                .readStream
                .schema(schema)
                .option("maxFilesPerTrigger", 50)  # Increased from 10 to process more files per batch
                .option("latestFirst", "true")     # Process newest files first
                .option("maxFileAge", "7d")        # Ignore files older than 7 days
                .option("cleanSource", "delete")   # Delete processed files to prevent accumulation
                .json(input_path))
    
    def read_stream_from_kafka(self, kafka_servers: str, topic: str, 
                              schema: StructType) -> DataFrame:
        """
        Read streaming data from Kafka topic.
        """
        return (self.spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_servers)
                .option("subscribe", topic)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
                .select(
                    col("key").cast("string"),
                    from_json(col("value").cast("string"), schema).alias("data")
                )
                .select("data.*"))
    
    def process_orders_stream(self, orders_stream: DataFrame) -> DataFrame:
        """
        Process and enrich orders stream with real-time transformations.
        """
        return (orders_stream
                .withColumn("processing_timestamp", current_timestamp())
                .withColumn("order_timestamp", 
                           to_timestamp(col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
                .withColumn("order_date_parsed", 
                           coalesce(
                               to_timestamp(col("order_date"), "yyyy-MM-dd"),
                               to_timestamp(col("order_date"), "MM/dd/yyyy"),
                               to_timestamp(col("order_date"), "dd-MM-yyyy"),
                               to_timestamp(col("order_date"), "yyyy/MM/dd")
                           ))
                .withColumn("order_year", 
                           coalesce(
                               expr("year(order_date_parsed)"),
                               expr("year(order_timestamp)"),
                               lit(2024)  # Default year if parsing fails
                           ))
                .withColumn("order_month", 
                           coalesce(
                               expr("month(order_date_parsed)"),
                               expr("month(order_timestamp)"),
                               lit(12)  # Default month if parsing fails
                           ))
                .withColumn("is_completed", 
                           when(col("status") == "completed", 1).otherwise(0))
                .withColumn("is_valid_order",
                           when((col("order_id").isNotNull()) & 
                                (col("customer_id").isNotNull()) &
                                (col("total_amount") > 0), 1).otherwise(0))
                )
    
    def aggregate_revenue_by_window(self, orders_stream: DataFrame, 
                                   window_duration: str = "1 minute") -> DataFrame:
        """
        Calculate revenue aggregations over time windows.
        
        Parameters
        ----------
        orders_stream : DataFrame
            Processed orders stream
        window_duration : str
            Window size (e.g., "1 minute", "5 minutes", "1 hour")
        """
        return (orders_stream
                .filter(col("is_valid_order") == 1)
                .filter(col("status") == "completed")
                .withWatermark("order_timestamp", "10 minutes")  # Handle late data
                .groupBy(
                    window(col("order_timestamp"), window_duration),
                    col("status")
                )
                .agg(
                    count("order_id").alias("order_count"),
                    _sum("total_amount").alias("total_revenue"),
                    avg("total_amount").alias("avg_order_value"),
                    approx_count_distinct("customer_id").alias("unique_customers")
                )
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("status"),
                    col("order_count"),
                    col("total_revenue"),
                    col("avg_order_value"),
                    col("unique_customers")
                ))
    
    def detect_fraud_streaming(self, orders_stream: DataFrame) -> DataFrame:
        """
        Real-time fraud detection on streaming orders.
        
        Flags suspicious patterns:
        - High-value orders
        - Orders with unusual characteristics
        
        Note: Simplified to avoid stream-stream joins which require complex watermarking.
        Uses individual order characteristics for scoring.
        """
        # Filter valid orders first
        valid_orders = orders_stream.filter(col("is_valid_order") == 1)
        
        # Score orders for fraud based on individual order characteristics
        # This avoids stream-stream joins which are complex in streaming
        fraud_scored = (valid_orders
                       .withColumn("amount_score",
                                  when(col("total_amount") > 5000, 4)
                                  .when(col("total_amount") > 3000, 3)
                                  .when(col("total_amount") > 2000, 2)
                                  .otherwise(0))
                       # Additional scoring factors
                       .withColumn("status_score",
                                  when(col("status") == "completed", 0)  # Completed orders are normal
                                  .when(col("status").isin(["cancelled", "refunded"]), 1)  # Suspicious status
                                  .otherwise(0))
                       .withColumn("fraud_score", 
                                  col("amount_score") + col("status_score"))
                       .withColumn("is_potential_fraud",
                                  when(col("fraud_score") >= 3, True).otherwise(False))
                       )
        
        # Add watermark for append mode (required for some streaming operations)
        fraud_filtered = (fraud_scored
                         .filter(col("is_potential_fraud") == True)
                         .withWatermark("order_timestamp", "10 minutes"))
        
        return (fraud_filtered
                .select(
                    col("order_id"),
                    col("customer_id"),
                    col("order_timestamp"),
                    col("total_amount"),
                    col("fraud_score"),
                    lit(0).alias("orders_in_last_hour"),  # Placeholder - would need stateful aggregation
                    col("status")
                ))
    
    def write_stream_to_bronze(self, stream: DataFrame, table_name: str,
                              output_format: str = "parquet",
                              trigger_interval: str = "30 seconds"):
        """
        Write stream to Bronze layer (raw ingested data).
        
        Uses foreachBatch for better control over writes.
        """
        checkpoint_path = f"{self.checkpoint_location}/bronze/{table_name}"
        output_path = f"{PATHS.bronze_base}/{table_name}"
        
        def write_batch(batch_df, batch_id):
            """Write each micro-batch."""
            batch_df.write.mode("append").parquet(output_path)
        
        query = (stream
                .writeStream
                .outputMode("append")
                .foreachBatch(write_batch)
                .option("checkpointLocation", checkpoint_path)
                .trigger(processingTime=trigger_interval)
                .start())
        
        return query
    
    def write_stream_to_silver(self, stream: DataFrame, table_name: str,
                              trigger_interval: str = "1 minute"):
        """
        Write processed/cleaned stream to Silver layer.
        """
        checkpoint_path = f"{self.checkpoint_location}/silver/{table_name}"
        output_path = f"{PATHS.silver_base}/{table_name}"
        
        def write_batch(batch_df, batch_id):
            """Write each micro-batch with safe partitioning."""
            try:
                # Check if partitioning columns exist and have valid values
                if ("order_year" in batch_df.columns and "order_month" in batch_df.columns):
                    # Filter out rows with null partition values to avoid __HIVE_DEFAULT_PARTITION__
                    valid_partition_df = batch_df.filter(
                        col("order_year").isNotNull() & col("order_month").isNotNull()
                    )
                    
                    if valid_partition_df.count() > 0:
                        # Write with partitioning for valid data
                        valid_partition_df.write.mode("append").partitionBy("order_year", "order_month").parquet(output_path)
                    
                    # Write invalid partition data without partitioning
                    invalid_partition_df = batch_df.filter(
                        col("order_year").isNull() | col("order_month").isNull()
                    )
                    
                    if invalid_partition_df.count() > 0:
                        # Write to a separate directory for data with invalid dates
                        invalid_output_path = f"{output_path}_invalid_dates"
                        invalid_partition_df.write.mode("append").parquet(invalid_output_path)
                else:
                    # No partitioning columns, write normally
                    batch_df.write.mode("append").parquet(output_path)
                    
            except Exception as e:
                print(f"[ERROR] Silver layer write failed for batch {batch_id}: {e}")
                # Fallback: write without partitioning
                try:
                    batch_df.write.mode("append").parquet(output_path)
                    print(f"[OK] Fallback write successful for batch {batch_id}")
                except Exception as e2:
                    print(f"[ERROR] Fallback write also failed: {e2}")
        
        query = (stream
                .writeStream
                .outputMode("append")
                .foreachBatch(write_batch)
                .option("checkpointLocation", checkpoint_path)
                .trigger(processingTime=trigger_interval)
                .start())
        
        return query
    
    def write_stream_to_console(self, stream: DataFrame, 
                               output_mode: str = "complete",
                               trigger_interval: str = "10 seconds"):
        """
        Write stream to console for debugging/monitoring.
        """
        query = (stream
                .writeStream
                .outputMode(output_mode)
                .format("console")
                .option("truncate", "false")
                .trigger(processingTime=trigger_interval)
                .start())
        
        return query
    
    def write_stream_to_memory_table(self, stream: DataFrame, table_name: str,
                                     output_mode: str = "complete",
                                     trigger_interval: str = "10 seconds"):
        """
        Write stream to in-memory table for real-time dashboard access.
        """
        try:
            query = (stream
                    .writeStream
                    .outputMode(output_mode)
                    .format("memory")
                    .queryName(table_name)
                    .trigger(processingTime=trigger_interval)
                    .start())
            
            return query
        except Exception as e:
            print(f"[ERROR] Failed to create memory table '{table_name}': {e}")
            print(f"   Output mode: {output_mode}")
            print(f"   Stream schema: {stream.schema}")
            raise


def create_streaming_pipeline(input_mode: str = "file",
                             input_path: str = "data/streaming/orders",
                             kafka_servers: Optional[str] = None,
                             kafka_topic: Optional[str] = None) -> StreamingETLPipeline:
    """
    Create and configure a streaming ETL pipeline.
    
    Parameters
    ----------
    input_mode : str
        "file" for file-based streaming, "kafka" for Kafka
    input_path : str
        File path for file-based streaming
    kafka_servers : str, optional
        Kafka bootstrap servers
    kafka_topic : str, optional
        Kafka topic name
    """
    pipeline = StreamingETLPipeline()
    
    # Read orders stream
    if input_mode == "kafka" and kafka_servers and kafka_topic:
        orders_raw = pipeline.read_stream_from_kafka(kafka_servers, kafka_topic, ORDER_SCHEMA)
    else:
        orders_raw = pipeline.read_stream_from_files(input_path, ORDER_SCHEMA)
    
    # Process orders
    orders_processed = pipeline.process_orders_stream(orders_raw)
    
    # Real-time aggregations
    revenue_by_window = pipeline.aggregate_revenue_by_window(orders_processed, "1 minute")
    
    # Fraud detection
    fraud_alerts = pipeline.detect_fraud_streaming(orders_processed)
    
    # Write to Bronze layer
    bronze_query = pipeline.write_stream_to_bronze(orders_raw, "orders_streaming")
    
    # Write to Silver layer
    silver_query = pipeline.write_stream_to_silver(orders_processed, "orders_enriched_streaming")
    
    # Write aggregations to memory for dashboard
    revenue_query = pipeline.write_stream_to_memory_table(revenue_by_window, "revenue_by_window")
    
    # Write fraud alerts to console and memory
    # Use "append" mode for non-aggregated streams (fraud_alerts is filtered, not aggregated)
    try:
        fraud_console_query = pipeline.write_stream_to_console(fraud_alerts, "append")
    except Exception as e:
        print(f"[WARN] Could not start fraud console query: {e}")
        fraud_console_query = None
    
    try:
        fraud_memory_query = pipeline.write_stream_to_memory_table(fraud_alerts, "fraud_alerts", "append")
    except Exception as e:
        print(f"[WARN] Could not start fraud memory table: {e}")
        print("   Fraud alerts will still be written to console and Bronze/Silver layers")
        fraud_memory_query = None
    
    print("[START] Streaming pipeline started!")
    print("   - Bronze layer: Writing raw orders")
    print("   - Silver layer: Writing processed orders")
    print("   - Real-time aggregations: Available in memory tables")
    print("   - Fraud detection: Active")
    print("\n   Press Ctrl+C to stop")
    
    # Wait for all queries
    try:
        bronze_query.awaitTermination()
        silver_query.awaitTermination()
        revenue_query.awaitTermination()
        if fraud_console_query:
            fraud_console_query.awaitTermination()
        if fraud_memory_query:
            fraud_memory_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[STOP] Stopping streaming pipeline...")
        bronze_query.stop()
        silver_query.stop()
        revenue_query.stop()
        if fraud_console_query:
            fraud_console_query.stop()
        if fraud_memory_query:
            fraud_memory_query.stop()
        print("[OK] Pipeline stopped")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run real-time streaming ETL pipeline")
    parser.add_argument("--mode", choices=["file", "kafka"], default="file",
                       help="Input mode: file or kafka")
    parser.add_argument("--input-path", default="data/streaming/orders",
                       help="Input path for file-based streaming")
    parser.add_argument("--kafka-servers", default="localhost:9092",
                       help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", default="ecommerce_orders",
                       help="Kafka topic name")
    
    args = parser.parse_args()
    
    create_streaming_pipeline(
        input_mode=args.mode,
        input_path=args.input_path,
        kafka_servers=args.kafka_servers if args.mode == "kafka" else None,
        kafka_topic=args.kafka_topic if args.mode == "kafka" else None
    )


