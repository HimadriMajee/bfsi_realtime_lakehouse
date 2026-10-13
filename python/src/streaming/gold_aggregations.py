"""
====================================================================================================
BFSI REAL-TIME DATA LAKEHOUSE: GOLD STREAMING RISK AGGREGATIONS PIPELINE
====================================================================================================
File Name   : python/src/streaming/gold_aggregations.py
Layer       : Gold Layer (Business Analytics & Real-Time Risk Layer)
Source      : S3 Silver Delta Table (s3a://.../silver/kafka_cleansed_transactions)
Sink        : S3 Gold Delta Table (s3a://.../gold/realtime_risk_metrics)

Pipeline Mechanics:
  - Dynamically configures local JDK 17, Hadoop binaries, and .env AWS credentials.
  - Reads cleansed streaming Delta data from the Silver S3 storage layer.
  - Implements event-time watermarking (15 mins) to handle late-arriving financial transactions.
  - Applies 10-minute tumbling event-time windows on transaction_timestamp.
  - Groups stream by time_window, transaction_type, and location.
  - Computes financial risk metrics: total_volume_usd, transaction_count, avg_val, max_single_val.
  - Flags high-risk financial anomalies (volume spikes > $10,000 USD per 10-minute window).
  - Writes continuously updated streaming aggregations in 'complete' mode to S3 Gold Delta storage.

====================================================================================================
PRODUCTION BOTTLENECK ANALYSIS & REAL-TIME OPERATIONAL ISSUES
====================================================================================================
1. BOTTLENECK: State Store Memory Growth (OOM in Streaming Aggregations)
   - ISSUE: Unbounded window aggregations store state in memory indefinitely for every active key,
     causing Spark driver/executor Java Heap Out-Of-Memory (OOM) crashes.
   - RESOLUTION: Always enforce event-time watermarking (`.withWatermark("timestamp", "15 minutes")`).
     This allows Spark's State Store (HDFSBackedStateStoreProvider or RocksDBStateStoreProvider)
     to safely purge expired window state from memory.

2. REAL-TIME ISSUE: Output Mode Compatibility ('complete' vs 'append' vs 'update')
   - ISSUE: In streaming aggregations without watermarks, 'append' mode fails because Spark cannot
     determine when a window is closed and complete.
   - RESOLUTION: Use `outputMode("complete")` when overwriting full aggregated table state, or use
     `outputMode("append")` paired with strict watermarks so rows are emitted only after the window closes.

3. REAL-TIME ISSUE: Shuffle Partition Over-Provisioning
   - ISSUE: The default `spark.sql.shuffle.partitions = 200` creates 200 tiny shuffle files for small
     streaming micro-batches, drastically increasing processing overhead and network latency.
   - RESOLUTION: Tune shuffle partitions for real-time low-latency streams:
     `spark.conf.set("spark.sql.shuffle.partitions", "8")` (or match executor core count).

====================================================================================================
SENIOR DATA ENGINEER INTERVIEW PREPARATION (BASIC TO ADVANCED Q&A)
====================================================================================================
Q1 (Basic): What is the role of the Gold layer in a Medallion Architecture?
    - Answer: The Gold layer transforms cleansed, atomic Silver data into business-level aggregates,
      KPIs, and analytics-ready datasets optimized for BI dashboards, ML feature stores, and reporting.

Q2 (Intermediate): What is a Tumbling Window versus a Sliding Window in Spark Structured Streaming?
    - Answer: 
      - Tumbling Window: Fixed-size, non-overlapping time intervals (e.g., 10:00-10:10, 10:10-10:20).
      - Sliding Window: Fixed-size, overlapping intervals defined by window duration and slide duration
        (e.g., 10-minute window sliding every 5 minutes: 10:00-10:10, 10:05-10:15).

Q3 (Advanced): How does Watermarking handle late-arriving data in stateful streaming?
    - Answer: Watermarking defines a trailing threshold (t_max - delay) relative to the highest 
      event time seen by engine. Any event with `event_time < (t_max - delay)` is dropped as too late.
      Events arriving within the watermark update current window state before the window finalizes.
====================================================================================================
"""

import os                                         # Operating system interface for environment variables and paths
import sys                                        # System parameters and stream management
import logging                                    # Python standard logging library
from dotenv import load_dotenv                    # Module to parse and load .env files into environment

# --------------------------------------------------------------------------------------------------
# STEP 1: MUTE VERBOSE LOGGERS
# --------------------------------------------------------------------------------------------------
# Reduce internal JVM and Py4J logging to prevent console clutter
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

# --------------------------------------------------------------------------------------------------
# STEP 2: DYNAMIC ENVIRONMENT & PATH RESOLUTION
# --------------------------------------------------------------------------------------------------
# Compute repository root path dynamically by navigating 3 directory levels up from this file
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")

# Force-load AWS IAM keys (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) from root .env file
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
    print(f"[*] Loaded AWS Credentials from: {ENV_PATH}")
else:
    load_dotenv()

# Configure local JDK 17 environment paths for Spark driver/executor processes on Windows
LOCAL_JDK_DIR = os.path.join(PROJECT_ROOT, "jdk-17.0.10+7")
if os.path.exists(LOCAL_JDK_DIR):
    os.environ["JAVA_HOME"] = LOCAL_JDK_DIR
    os.environ["PATH"] = os.path.join(LOCAL_JDK_DIR, "bin") + os.path.pathsep + os.environ.get("PATH", "")

# Configure local Hadoop winutils paths for local file system emulation
LOCAL_HADOOP_DIR = os.path.join(PROJECT_ROOT, "hadoop")
if os.path.exists(LOCAL_HADOOP_DIR):
    os.environ["HADOOP_HOME"] = LOCAL_HADOOP_DIR
    os.environ["PATH"] += os.path.pathsep + os.path.join(LOCAL_HADOOP_DIR, "bin")

# Import PySpark SQL dependencies after environment paths are fully bound
from pyspark.sql import SparkSession             # Entry point to Spark SQL and Structured Streaming functionality
from pyspark.sql.functions import (               # SQL functions for windowing, aggregates, and conditional logic
    col,
    window,
    sum as _sum,
    count as _count,
    avg as _avg,
    max as _max,
    when,
    lit,
    current_timestamp
)
from delta import configure_spark_with_delta_pip  # Helper to download and bind Delta Lake JAR packages


# --------------------------------------------------------------------------------------------------
# STEP 3: SPARK SESSION BUILDER
# --------------------------------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    """
    Initializes a SparkSession pre-configured with Delta Lake extensions, AWS S3A file system drivers,
    and optimized shuffle settings for real-time streaming aggregations.
    """
    print("[*] Initializing Spark Session for Gold Risk Aggregations Layer...")

    # Define Maven package dependencies required for AWS S3 and Delta Lake
    extra_packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",                 # AWS S3A file system connector
        "io.delta:delta-spark_2.12:3.1.0"                     # Delta Lake core engine
    ]

    builder = (
        SparkSession.builder
        .appName("BFSI_Gold_Risk_Aggregations_Pipeline")      # Set application identifier in Spark UI
        # Register Delta Lake SQL extensions and catalog handlers
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Configure Hadoop S3A FileSystem implementation
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")     # Enable high-throughput memory buffer for uploads
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.io.nativeio.enabled", "false")   # Disable Windows-native C++ IO libraries
        # Bind AWS authentication to environment variables
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
        )
        # Performance tuning: Optimize shuffle partitions for low-latency local streaming
        .config("spark.sql.shuffle.partitions", "8")
    )

    # Attach Maven packages and instantiate SparkSession instance
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=extra_packages
    ).getOrCreate()


# --------------------------------------------------------------------------------------------------
# STEP 4: MAIN STREAMING EXECUTION PIPELINE
# --------------------------------------------------------------------------------------------------
def main():
    # Initialize Spark Session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Mute noisy INFO-level execution logs

    # Target S3 Lakehouse Paths
    S3_BUCKET = "bp-bfsi-realtime-lakehouse-245987718167-ap-southeast-5-an"
    SILVER_S3_PATH = f"s3a://{S3_BUCKET}/silver/kafka_cleansed_transactions"
    GOLD_S3_PATH   = f"s3a://{S3_BUCKET}/gold/realtime_risk_metrics"
    CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/checkpoints/gold_aggregations"

    print(f"[*] Reading cleansed Delta stream from Silver S3 path: {SILVER_S3_PATH}")

    # Read Streaming Delta Table from Silver Storage Layer
    silver_stream = (
        spark.readStream
        .format("delta")                                       # Read directly from Delta storage format
        .load(SILVER_S3_PATH)                                 # Path to Silver cleansed S3 layer
    )

    # Apply Event-Time Watermarking and Calculate 10-Minute Tumbling Window Aggregations
    gold_risk_df = (
        silver_stream
        # Watermark: Allow late records up to 15 minutes past the highest observed timestamp
        .withWatermark("transaction_timestamp", "15 minutes")
        # Tumbling Window: Group records into fixed 10-minute time intervals
        .groupBy(
            window(col("transaction_timestamp"), "10 minutes").alias("time_window"),
            col("transaction_type"),                           # Dimension 1: Transaction classification type
            col("location")                                   # Dimension 2: Transaction geographical location
        )
        # Stateful Aggregations over window groups
        .agg(
            _count("transaction_id").alias("transaction_count"),      # Total transaction volume in window
            _sum("amount").alias("total_volume_usd"),                 # Cumulative financial amount in USD
            _avg("amount").alias("avg_transaction_val"),              # Mean transaction amount
            _max("amount").alias("max_single_transaction")             # Highest single transaction spike
        )
        # Select and format final business reporting attributes
        .select(
            col("time_window.start").alias("window_start"),           # Extract window start timestamp
            col("time_window.end").alias("window_end"),               # Extract window end timestamp
            col("transaction_type"),
            col("location"),
            col("transaction_count"),
            col("total_volume_usd"),
            col("avg_transaction_val"),
            col("max_single_transaction"),
            # Risk Anomaly Classification: Flag windows exceeding $10,000 total volume
            when(col("total_volume_usd") > 10000, lit("HIGH_RISK_SPIKE"))
            .otherwise(lit("NORMAL"))
            .alias("risk_classification"),
            current_timestamp().alias("gold_computed_timestamp")       # UTC calculation execution timestamp
        )
    )

    print(f"[*] Starting Gold Delta stream write to: {GOLD_S3_PATH}")
    print(f"[*] Checkpointing configured at: {CHECKPOINT_PATH}")

    # Write Micro-Batch Window Aggregations continuously into Gold Delta S3 Storage
    query = (
        gold_risk_df.writeStream
        .format("delta")                                       # Target storage engine (Delta Lake)
        .outputMode("complete")                                # Complete mode updates full aggregated state table
        .option("checkpointLocation", CHECKPOINT_PATH)         # Write-Ahead Log offset checkpointing directory
        .queryName("GoldRiskAggregationsStream")               # Assign explicit query identifier for UI tracking
        .start(GOLD_S3_PATH)                                   # Begin background streaming execution to S3 URI
    )

    print("[✔] Gold Risk Aggregations Stream active!")
    print("[*] Monitoring streaming metrics at http://localhost:4040 (Spark Web UI)")

    # Keep stream active and process continuous window aggregations
    query.awaitTermination()


if __name__ == "__main__":
    main()