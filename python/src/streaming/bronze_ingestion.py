"""
================================================================================
BFSI REAL-TIME DATA LAKEHOUSE: BRONZE STREAMING INGESTION (COMPLETE PRODUCTION FILE)
================================================================================
Architecture Layer : Bronze (Raw Landing Zone)
Source System      : Local Apache Kafka KRaft Broker (Multi-Topic Domain Stream)
Target Storage     : AWS S3 Delta Lake Table (s3a://.../bronze/kafka_raw_transactions)
Catalog Engine     : AWS Glue Data Catalog (bfsi_financial_risk_db)

Pipeline Mechanics :
  - Consumes raw event payloads across 3 domain topics simultaneously.
  - Registers 'pyspark-bronze-consumer-group' so active consumer metrics are visible in Kafka UI (http://localhost:8080).
  - Bypasses Windows C++ DLL permission errors using bytebuffer S3A staging.
  - Preserves immutable raw JSON strings without filtering or schema enforcement.
  - Captures message metadata (topic, partition, offset, timestamp) for auditability.
  - Uses S3 Delta Lake checkpointing for fault-tolerant at-least-once delivery.
  - Explicitly names the streaming query for visual inspection in Spark Web UI (http://localhost:4040).
================================================================================
"""

# Importing standard operating system module for path resolution and environment variable settings
import os
# Importing system parameters module
import sys
# Importing main PySpark SQL entry point class to configure distributed DataFrame sessions
from pyspark.sql import SparkSession
# Importing PySpark SQL functions for column transformations and capturing current ingestion timestamps
from pyspark.sql.functions import col, current_timestamp
# Importing Delta Lake configuration helper to inject Delta dependencies into PySpark
from delta import configure_spark_with_delta_pip

# ==============================================================================
# WINDOWS OS COMPATIBILITY FIX (HADOOP_HOME & WINUTILS)
# ==============================================================================
# Resolves the root project directory dynamically
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
# Automatically configures HADOOP_HOME to point to local ./hadoop folder on Windows machines
LOCAL_HADOOP_DIR = os.path.join(PROJECT_ROOT, "hadoop")
if os.path.exists(LOCAL_HADOOP_DIR):
    os.environ["HADOOP_HOME"] = LOCAL_HADOOP_DIR
    os.environ["PATH"] += os.path.pathsep + os.path.join(LOCAL_HADOOP_DIR, "bin")

# ==============================================================================
# CONFIGURATION CONSTANTS
# ==============================================================================
# Globally unique AWS S3 Bucket Name for storing Medallion Delta tables and checkpoints
S3_BUCKET = "bp-bfsi-realtime-lakehouse-245987718167-ap-southeast-5-an"

# External host listener address and port mapped in Docker Compose for Kafka broker traffic
KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"

# Multi-topic ingestion subscription list formatted as a comma-separated string
KAFKA_TOPICS = (
    "bfsi-retail-transactions,"
    "bfsi-highrisk-transactions,"
    "bfsi-travel-transactions"
)

# Target Delta Lake storage location in AWS S3 for raw Bronze records
BRONZE_S3_PATH = f"s3a://{S3_BUCKET}/bronze/kafka_raw_transactions"
# Target S3 path for PySpark Structured Streaming offset checkpoints
CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/checkpoints/bronze_ingestion"


def create_spark_session() -> SparkSession:
    """
    Initializes and returns a SparkSession configured with Delta Lake extensions,
    Kafka Structured Streaming connectors, and Apache Hadoop AWS S3 drivers.
    """
    # Print status message indicating SparkSession initialization step
    print("[*] Initializing Spark Session with Delta Lake & Kafka connectors...")
    
    # Define additional Maven package coordinates for Kafka streaming and Hadoop AWS S3 access
    extra_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ]
    
    # Start building the base SparkSession configuration
    builder = (
        # Obtain existing SparkSession or create a new builder instance
        SparkSession.builder
        # Set application name displayed in Spark UI and job logs
        .appName("BFSI_Bronze_Ingestion_Pipeline")
        # Enable Delta Lake SQL extensions inside PySpark
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # Set Delta Catalog as default catalog implementation for Spark catalog operations
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Register S3A FileSystem driver implementation class for s3a:// URI scheme resolution
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # USE IN-MEMORY FAST UPLOAD BUFFERING (Bypasses Windows disk allocator & NativeIO DLL errors)
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        # DISABLE WINDOWS C++ NATIVE DLL PERMISSION CALLS
        .config("spark.hadoop.io.nativeio.enabled", "false")
        # Instruct Hadoop AWS driver to retrieve credentials from active environment variables
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
        )
    )
    
    # Inject Delta Lake dependencies and extra packages, then build/return active SparkSession
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=extra_packages
    ).getOrCreate()


def main():
    """
    Main execution entry point for PySpark Bronze Streaming Ingestion Pipeline.
    """
    # Call function to initialize and retrieve configured SparkSession
    spark = create_spark_session()
    # Reduce console log verbosity to WARN to highlight critical operational logs
    spark.sparkContext.setLogLevel("WARN")

    # Print status message displaying target Kafka broker endpoint
    print(f"[*] Reading raw streams from Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    # Print status message displaying subscribed Kafka domain topics
    print(f"[*] Subscribed Topics: {KAFKA_TOPICS}")

    # Step 1: Initialize streaming DataFrame reader for Kafka multi-topic source
    kafka_stream = (
        # Access Structured Streaming read interface
        spark.readStream
        # Specify 'kafka' as the streaming source format driver
        .format("kafka")
        # Set Kafka bootstrap servers broker connection option
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        # Set subscription option to listen to multiple comma-separated topics simultaneously
        .option("subscribe", KAFKA_TOPICS)
        # Explicitly assign Consumer Group ID so Kafka UI (http://localhost:8080) can track consumer lag
        .option("kafka.group.id", "pyspark-bronze-consumer-group")
        # Configure stream to read from the earliest available offset on cold start
        .option("startingOffsets", "earliest")
        # Prevent streaming pipeline failure if Kafka topic log retention truncates offsets
        .option("failOnDataLoss", "false")
        # Load options and return a streaming DataFrame object
        .load()
    )

    # Step 2: Transform raw binary streams into structured string fields & preserve metadata
    bronze_df = kafka_stream.select(
        # Cast message key from BINARY to STRING and alias as kafka_key
        col("key").cast("STRING").alias("kafka_key"),
        # Cast message value from BINARY to STRING and alias as raw_payload (preserves unparsed JSON)
        col("value").cast("STRING").alias("raw_payload"),
        # Select original source Kafka topic name string
        col("topic").alias("kafka_topic"),
        # Select original source Kafka partition integer ID
        col("partition").alias("kafka_partition"),
        # Select original source Kafka record offset long integer value
        col("offset").alias("kafka_offset"),
        # Select Kafka message producer timestamp
        col("timestamp").alias("kafka_timestamp"),
        # Generate current system timestamp to log exact PySpark ingestion time
        current_timestamp().alias("ingestion_timestamp")
    )

    # Print target S3 Delta output storage destination
    print(f"[*] Starting Bronze Delta stream write to: {BRONZE_S3_PATH}")
    # Print S3 offset checkpoint location path
    print(f"[*] Checkpointing configured at: {CHECKPOINT_PATH}")

    # Step 3: Write streaming DataFrame to AWS S3 in Delta Lake format
    query = (
        # Access Structured Streaming write interface
        bronze_df.writeStream
        # Set destination table format to Delta Lake
        .format("delta")
        # Explicitly name the query for clear tracking in Spark Web UI (http://localhost:4040)
        .queryName("BronzeKafkaIngestionStream")
        # Set output mode to 'append' (standard for immutable append-only event streams)
        .outputMode("append")
        # Set checkpoint directory location in S3 for fault-tolerant state recovery
        .option("checkpointLocation", CHECKPOINT_PATH)
        # Start streaming execution and target S3 bucket location
        .start(BRONZE_S3_PATH)
    )

    # Print success message indicating active streaming query startup
    print("[✔] Bronze Ingestion Stream started successfully! Streaming active...")
    print("[*] Open http://localhost:4040 to view real-time query metrics in Spark Web UI.")
    print("[*] Open http://localhost:8080 to monitor 'pyspark-bronze-consumer-group' in Kafka UI.")
    # Keep streaming query thread alive and await manual or system termination signals
    query.awaitTermination()


# Python standard script execution entry point
if __name__ == "__main__":
    # Execute main function when script is invoked directly
    main()


# ================================================================================
# ENTERPRISE BOTTLENECK ANALYSIS, RESOLUTIONS & INTERVIEW QA REFERENCE
# ================================================================================
#
# --------------------------------------------------------------------------------
# PART 1: PRODUCER & CONSUMER STREAMING BOTTLENECK MATRIX
# --------------------------------------------------------------------------------
#
# 1. PRODUCER BOTTLENECKS & RESOLUTIONS:
# --------------------------------------------------------------------------------
# Issue / Symptom                  | Root Cause                        | Resolution Technique
# ---------------------------------|-----------------------------------|------------------------------------------------
# A. High Producer Network Latency | Sending messages synchronously    | Increase batch.size (e.g., 32KB/64KB) and set
#    & CPU Thrashing               | one-by-one (future.get() on       | linger.ms (10-50ms) to accumulate batches.
#                                  | every message).                   | Enable compression (snappy or lz4).
# ---------------------------------|-----------------------------------|------------------------------------------------
# B. Producer Backpressure /       | Kafka broker disk I/O saturation  | Configure buffer.memory (e.g., 32MB/64MB) and
#    Memory Buffer Exhaustion      | or consumer group falling behind. | set max.block.ms to fail fast or throttle rate.
#                                  |                                   | Scale out broker disk/partition count.
# ---------------------------------|-----------------------------------|------------------------------------------------
# C. Unbalanced Topic Partitioning | Default round-robin distribution  | Use custom Partitioner or salt partition keys
#    (Partition Skew at Ingress)   | or poor partition key choices.    | (account_id + "_" + random_int(1,5)) if single
#                                  |                                   | high-volume account spikes transaction volume.
#
# 2. CONSUMER (PYSPARK) BOTTLENECKS & RESOLUTIONS:
# --------------------------------------------------------------------------------
# Issue / Symptom                  | Root Cause                        | Resolution Technique
# ---------------------------------|-----------------------------------|------------------------------------------------
# A. Consumer Group Lag            | Micro-batch execution time >      | 1. Increase topic partitions (e.g., from 3 to 12).
#    (Unprocessed Records Piling Up| batch interval time; insufficient | 2. Scale PySpark worker executors to match partitions.
#    in Kafka Topics)              | parallel processing units.        | 3. Set maxOffsetsPerTrigger to cap records per
#                                  |                                   |    micro-batch (prevents Out-Of-Memory / OOM).
# ---------------------------------|-----------------------------------|------------------------------------------------
# B. Executor Data Skew            | Monolithic key distribution       | 1. Implement Two-Stage Aggregation (Salt key with
#    (One Spark executor takes 90% | (e.g., massive enterprise customer|    random integer before final group-by).
#    of processing time)           | generates millions of txns).      | 2. Enable Adaptive Query Execution (spark.sql.adaptive.enabled).
#                                  |                                   | 3. Adjust Spark dynamic allocation parameters.
# ---------------------------------|-----------------------------------|------------------------------------------------
# C. S3 "Small File Problem"       | Streaming engine writes hundreds  | 1. Increase trigger interval (trigger(processingTime='1 minute')).
#    & S3 API Rate Limit Throttling| of tiny Parquet/Delta files every | 2. Run periodic compaction jobs: OPTIMIZE table ZORDER BY (kafka_timestamp).
#                                  | few seconds to S3.                | 3. Enable Delta Auto-Optimize and Auto-Compact properties.
# ---------------------------------|-----------------------------------|------------------------------------------------
# D. Late-Arriving / Out-of-Order  | Network latency, edge devices,    | Apply Watermarking in PySpark Silver Layer:
#    Events                        | or offline mobile banking syncs.  | .withWatermark("kafka_timestamp", "15 minutes")
#                                  |                                   | to safely bound stateful memory windows.
#
# --------------------------------------------------------------------------------
# PART 2: TOP BFSI STREAMING DATA ENGINEERING INTERVIEW QUESTIONS
# --------------------------------------------------------------------------------
#
# Q1: Why do we store unparsed raw JSON in the Bronze Layer instead of parsing it immediately?
# A1: Storing raw payloads immutably in Bronze preserves schema lineage and auditability.
#     If downstream business logic, parsing rules, or schema evolution changes in Silver/Gold,
#     we can reprocess historical events directly from Bronze without needing to replay raw Kafka topics.
#
# Q2: How does PySpark ensure exact-once processing semantics when writing streams to S3 Delta Lake?
# A2: Exact-once state is maintained by combining Kafka offset tracking in Delta checkpoint directories
#     (checkpointLocation) with Delta Lake's ACID transaction log (_delta_log/).
#     Upon worker failure, Spark reads the last committed transaction ID from the Delta log and restarts
#     consumption strictly from the corresponding recorded Kafka partition offsets.
#
# Q3: What happens if a Kafka producer sends records with a missing or null partition key?
# A3: If no key is provided, Kafka uses sticky partitioning (or round-robin in older versions)
#     to distribute records evenly across available topic partitions.
#     However, for financial applications requiring strict per-account event ordering, a key like
#     account_id MUST be provided to guarantee that all transactions for that account map to the same partition.
#
# Q4: How do you handle S3 API rate limits and small file accumulation in PySpark Structured Streaming?
# A4: Micro-batch streaming to object storage produces many small files.
#     We resolve this by:
#     1. Tuning trigger(processingTime='1 minute') or trigger(availableNow=True) to control write frequencies.
#     2. Utilizing Delta Lake OPTIMIZE commands periodically to compact small files into 1GB Parquet files.
#     3. Setting spark.databricks.delta.optimizeWrite.enabled = true.
#
# Q5: Why disable KAFKA_AUTO_CREATE_TOPICS_ENABLE in production BFSI pipelines?
# A5: Auto-created topics default to single-partition topics with default retention settings.
#     Disabling auto-creation prevents typos from silently creating unmanaged topics, enforces strict
#     topic provisioning governance via IaC (Terraform), and guarantees proper partition distribution for scaling.
# ================================================================================
