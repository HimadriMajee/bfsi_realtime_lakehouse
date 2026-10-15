"""
====================================================================================================
BFSI REAL-TIME DATA LAKEHOUSE: BRONZE STREAMING INGESTION PIPELINE
====================================================================================================
File Name   : python/src/streaming/bronze_ingestion.py
Layer       : Bronze Layer (Raw Streaming Ingestion)
Source      : Kafka Broker (bfsi-retail-transactions, bfsi-highrisk-transactions, bfsi-travel-transactions)
Sink        : S3 Bronze Delta Table (s3a://.../bronze/kafka_raw_transactions)

Pipeline Mechanics:
  - Dynamically configures local JDK 17, Hadoop binaries, and .env AWS credentials.
  - Subscribes to 3 real-time transaction topics on Kafka.
  - Micro-batches raw JSON streams as-is without dropping or transforming attributes.
  - Appends raw records and Kafka metadata (topic, partition, offset, timestamp) to S3 Bronze.
  - Uses Delta ACID checkpoints for fault-tolerant, exactly-once processing recovery.

====================================================================================================
PRODUCTION BOTTLENECK ANALYSIS & REAL-TIME OPERATIONAL ISSUES
====================================================================================================
1. BOTTLENECK: Small File Problem (S3 Metadata & I/O Overhead)
   - ISSUE: High-frequency low-latency micro-batches create thousands of tiny 10-KB Parquet files 
     in S3, degrading Athena/Spark read query performance and driving up S3 GET/PUT API costs.
   - RESOLUTION: Configure Delta Auto-Optimize and Auto-Compact:
     spark.databricks.delta.optimizeWrite.enabled=true
     spark.databricks.delta.autoCompact.enabled=true
     Or schedule periodic OPTIMIZE / VACUUM jobs out-of-band.

2. REAL-TIME ISSUE: Kafka Consumer Rebalancing & Backpressure
   - ISSUE: Spikes in transaction volume can overwhelm Spark executors, causing GC pauses, 
     missed heartbeat signals, consumer group rebalances, and stream processing lag.
   - RESOLUTION: Enforce backpressure limits via Spark configuration:
     spark.streaming.kafka.maxRatePerPartition=5000
     This caps the max records read per batch partition to prevent Out-Of-Memory (OOM) errors.

3. REAL-TIME ISSUE: Schema Drift in Source Kafka Messages
   - ISSUE: Upstream producers modify JSON payloads (add/delete/rename fields), breaking rigid downstream schemas.
   - RESOLUTION: In the Bronze layer, store raw JSON as an unparsed STRING or standard Binary column. 
     Defer schema enforcement to the Silver layer (Schema-on-Read / Late Binding pattern).

4. REAL-TIME ISSUE: Poison Pill / Malformed Records
   - ISSUE: Non-JSON or corrupted byte arrays in Kafka cause JSON parsers to throw runtime exceptions.
   - RESOLUTION: Keep raw payloads untouched in Bronze. Cleanse and filter or quarantine malformed 
     records to a Dead Letter Queue (DLQ) during Silver transformation.

====================================================================================================
SENIOR DATA ENGINEER INTERVIEW PREPARATION (BASIC TO ADVANCED Q&A)
====================================================================================================
Q1 (Basic): What is the purpose of the Bronze layer in a Medallion Architecture?
    - Answer: To preserve immutable, raw source data in its native format alongside ingestion metadata
      (Kafka offset, partition, ingestion timestamp). It acts as an audit log and enables historical
      reprocessing without re-querying upstream transactional systems.

Q2 (Intermediate): How does Delta Lake guarantee exactly-once processing during streaming ingestion?
    - Answer: Delta Lake combines Spark Structured Streaming's Write-Ahead Logging (WAL) and 
      checkpointing with Delta's ACID transaction log (_delta_log). Each micro-batch commit is atomic; 
      if a driver crashes, Spark resumes from the exact offset logged in the S3 checkpoint.

Q3 (Intermediate): Why use s3a:// instead of s3:// or s3n:// in PySpark?
    - Answer: s3a:// uses the modern Hadoop S3A file system driver supporting high-performance 
      multipart parallel uploads, IAM role authentication, and high throughput for large file systems.

Q4 (Advanced): How do you address the "Small File Problem" caused by continuous micro-batch writes?
    - Answer: By running asynchronous background compaction (`OPTIMIZE` command) to merge small 
      files into 1GB files, and executing `VACUUM` to remove stale historical files past retention limits.
====================================================================================================
"""

import os                                         # Operating system interface for environment variables and paths
import sys                                        # System parameters and stream management
import json                                       # Parses the JSON offset strings reported by Spark's streaming progress events
import logging                                    # Python standard logging library
from dotenv import load_dotenv                    # Module to parse and load .env files into environment

# --------------------------------------------------------------------------------------------------
# STEP 1: MUTE VERBOSE LOGGERS
# --------------------------------------------------------------------------------------------------
# Reduce internal JVM and Py4J logging to prevent console spam
logging.getLogger("py4j").setLevel(logging.ERROR)      # Show only ERROR-level messages from the Py4J bridge
logging.getLogger("pyspark").setLevel(logging.ERROR)   # Show only ERROR-level messages from PySpark's Python logger

# --------------------------------------------------------------------------------------------------
# STEP 2: DYNAMIC ENVIRONMENT & PATH RESOLUTION
# --------------------------------------------------------------------------------------------------
# Compute repository root path dynamically by navigating 3 directory levels up from this file
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))  # Absolute path of repo root
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")                                          # Expected location of .env file

# Force-load AWS IAM keys (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION)
if os.path.exists(ENV_PATH):                                   # Check whether the project-level .env file exists
    load_dotenv(ENV_PATH)                                      # Load its key=value pairs into os.environ
    print(f"[*] Loaded AWS Credentials from: {ENV_PATH}")      # Confirm to the console which file was used
else:                                                          # Fallback when the project-level .env is missing
    load_dotenv()                                              # Search default locations for a .env file

# Configure local JDK 17 environment paths for Spark driver/executor processes on Windows
LOCAL_JDK_DIR = os.path.join(PROJECT_ROOT, "jdk-17.0.10+7")    # Expected path of the bundled JDK folder
if os.path.exists(LOCAL_JDK_DIR):                              # Apply the override only if the folder exists
    os.environ["JAVA_HOME"] = LOCAL_JDK_DIR                    # Point Spark at the bundled JDK
    os.environ["PATH"] = os.path.join(LOCAL_JDK_DIR, "bin") + os.path.pathsep + os.environ.get("PATH", "")  # Put java.exe first on PATH

# Configure local Hadoop winutils paths for local file system emulation
LOCAL_HADOOP_DIR = os.path.join(PROJECT_ROOT, "hadoop")        # Expected path of the bundled Hadoop folder
if os.path.exists(LOCAL_HADOOP_DIR):                           # Apply the override only if the folder exists
    os.environ["HADOOP_HOME"] = LOCAL_HADOOP_DIR               # Tell Spark where Hadoop binaries (winutils.exe) live
    os.environ["PATH"] += os.path.pathsep + os.path.join(LOCAL_HADOOP_DIR, "bin")  # Append Hadoop bin folder to PATH

# Import Spark dependencies after environment paths are fully bound
from pyspark.sql import SparkSession             # Entry point to Spark SQL and Structured Streaming functionality
from pyspark.sql.functions import col, current_timestamp  # Functions for column manipulation and system time
from pyspark.sql.streaming import StreamingQueryListener  # Base class for receiving streaming progress events
from delta import configure_spark_with_delta_pip  # Helper to download and bind Delta Lake JAR packages
from confluent_kafka import Consumer, TopicPartition  # Lightweight Kafka client used only to commit offsets for the UI


# --------------------------------------------------------------------------------------------------
# STEP 3: SPARK SESSION BUILDER
# --------------------------------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    """
    Initializes a SparkSession pre-configured with Delta Lake extensions, Kafka connectors,
    and Hadoop S3A file system drivers for AWS integration.
    """
    print("[*] Initializing Spark Session for Bronze Ingestion Layer...")  # Log startup of the session builder

    # Define Maven package dependencies required for Kafka, AWS S3, and Delta Lake
    extra_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",  # Spark-Kafka integration library
        "org.apache.hadoop:hadoop-aws:3.3.4",                 # AWS S3A file system connector
        "io.delta:delta-spark_2.12:3.1.0"                     # Delta Lake core engine
    ]

    builder = (
        SparkSession.builder                                   # Start the fluent session builder
        .appName("BFSI_Bronze_Kafka_Ingestion_Pipeline")      # Set application identifier in Spark UI
        # Register Delta Lake SQL extensions and catalog handlers
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")  # Enable Delta SQL commands
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")  # Delta-aware catalog
        # Configure Hadoop S3A FileSystem implementation
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  # Map s3a:// to the S3A driver
        .config("spark.hadoop.fs.s3a.fast.upload", "true")     # Enable high-throughput memory buffer for uploads
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")  # Buffer upload parts off-heap in memory
        .config("spark.hadoop.io.nativeio.enabled", "false")   # Disable Windows-native C++ IO libraries
        # Bind AWS authentication to environment variables
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",    # Setting that selects the credential source
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"  # Read keys from AWS_* environment variables
        )
    )

    # Attach Maven packages and instantiate SparkSession instance
    return configure_spark_with_delta_pip(
        builder,                                               # Builder configured above
        extra_packages=extra_packages                          # Extra JARs resolved from Maven at startup
    ).getOrCreate()                                            # Create the session, or reuse an existing one


# --------------------------------------------------------------------------------------------------
# STEP 4: KAFKA OFFSET COMMIT LISTENER (ADDITIVE: MAKES THE CONSUMER GROUP VISIBLE IN KAFKA UI)
# --------------------------------------------------------------------------------------------------
class KafkaOffsetCommitListener(StreamingQueryListener):
    """
    Listens to Structured Streaming progress events and, after each completed micro-batch, commits that
    batch's end offsets to Kafka under a monitoring consumer group.
    NOTE: Spark's checkpoint remains the source of truth for recovery. This commit exists only so
    Kafka UI can display consumer group offsets and lag. The Delta sink is not modified in any way.
    """

    def __init__(self, bootstrap_servers: str, monitor_group_id: str, query_name: str):
        super().__init__()                                     # Initialize the Spark listener base class
        self.query_name = query_name                           # Only react to events from this streaming query
        # Consumer is never subscribed to topics; it is used only to commit explicit offsets.
        self.committer = Consumer({
            "bootstrap.servers": bootstrap_servers,            # Same broker the stream reads from
            "group.id": monitor_group_id,                      # Group name that will appear in Kafka UI
            "enable.auto.commit": False,                       # Commit manually, only when Spark reports a finished batch
        })

    def onQueryStarted(self, event):                           # Called once when a streaming query starts
        pass                                                   # Nothing to do on start

    def onQueryProgress(self, event):                          # Called after every completed micro-batch
        progress = event.progress                              # Metrics object describing the finished batch
        if progress.name != self.query_name:                   # Ignore events from any other streaming query
            return                                             # Skip unrelated queries
        try:                                                   # A Kafka commit problem must never stop the stream
            offsets = []                                       # Collects one TopicPartition entry per partition
            for source in progress.sources:                    # Each source describes one input (here: the Kafka source)
                if not source.endOffset:                       # Skip sources that report no offsets
                    continue                                   # Move on to the next source
                end_offsets = json.loads(source.endOffset)     # Parse JSON such as {"topic": {"0": 120, "1": 98}}
                for topic, partitions in end_offsets.items():  # Loop over topics in the offset map
                    for partition, offset in partitions.items():  # Loop over partitions of that topic
                        # endOffset is already the NEXT offset to read, so it is committed as-is (no +1).
                        offsets.append(TopicPartition(topic, int(partition), int(offset)))
            if offsets:                                        # Skip the commit when there is nothing to report
                self.committer.commit(offsets=offsets, asynchronous=False)  # Synchronous commit to the monitoring group
        except Exception as exc:                               # Catch any Kafka or parsing error
            print(f"[!] Kafka offset commit for UI failed (stream continues): {exc}")  # Log it and keep streaming

    def onQueryIdle(self, event):                              # Called when a query is running but has no new data (Spark 3.5+)
        pass                                                   # Nothing to do while idle

    def onQueryTerminated(self, event):                        # Called when the query stops or fails
        pass                                                   # Nothing to do on termination


# --------------------------------------------------------------------------------------------------
# STEP 5: MAIN STREAMING EXECUTION PIPELINE
# --------------------------------------------------------------------------------------------------
def main():
    # Initialize Spark Session
    spark = create_spark_session()                             # Build the session with Delta, Kafka and S3A support
    spark.sparkContext.setLogLevel("WARN")  # Mute noisy INFO-level execution logs

    # Pipeline Constants
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"                 # Kafka broker address
    KAFKA_TOPICS = "bfsi-retail-transactions,bfsi-highrisk-transactions,bfsi-travel-transactions"  # Topics to ingest
    KAFKA_MONITOR_GROUP = "bfsi-bronze-monitor"                # Consumer group name shown in Kafka UI (separate from Spark's own group)
    QUERY_NAME = "BronzeKafkaIngestionQuery"                   # Streaming query name, reused by the listener to filter events

    S3_BUCKET = "bp-bfsi-realtime-lakehouse-245987718167-ap-southeast-5-an"        # Target S3 bucket
    BRONZE_S3_PATH = f"s3a://{S3_BUCKET}/bronze/kafka_raw_transactions"            # Bronze Delta table location
    CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/checkpoints/bronze_ingestion"            # Spark checkpoint location (source of truth)

    print(f"[*] Subscribing to Kafka Topics: {KAFKA_TOPICS}")  # Log the topic list
    print(f"[*] Target S3 Bronze Path: {BRONZE_S3_PATH}")      # Log the target table path

    # Read Streaming Stream from Kafka Topics
    kafka_stream = (
        spark.readStream
        .format("kafka")                                        # Specify Kafka source connector
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) # Kafka broker network addresses
        .option("subscribe", KAFKA_TOPICS)                     # Comma-separated topics to ingest
        .option("startingOffsets", "earliest")                 # Consume from beginning if checkpoint absent
        .option("failOnDataLoss", "false")                     # Prevent job failure if Kafka topic retention expires
        .load()                                                # Create the streaming DataFrame
    )

    # Cast Kafka Binary Value to String and Append Ingestion Audit Metadata (column names match the existing Bronze table)
    raw_bronze_df = (
        kafka_stream
        .select(
            col("key").cast("STRING").alias("kafka_key"),          # Kafka message key (existing Bronze table already has this column)
            col("value").cast("STRING").alias("raw_payload"),      # Convert binary message payload to raw JSON string
            col("topic").alias("kafka_topic"),                     # Extract source topic name
            col("partition").alias("kafka_partition"),             # Extract Kafka partition ID
            col("offset").alias("kafka_offset"),                   # Extract message offset position
            col("timestamp").alias("kafka_timestamp"),             # Extract event timestamp from Kafka header
            current_timestamp().alias("ingestion_timestamp")       # Stamp UTC ingestion time, named as in the existing table
        )
    )

    # Register the listener BEFORE starting the query so the first micro-batch is also reported to Kafka
    spark.streams.addListener(
        KafkaOffsetCommitListener(KAFKA_BOOTSTRAP_SERVERS, KAFKA_MONITOR_GROUP, QUERY_NAME)
    )

    print(f"[*] Starting Micro-Batch Streaming Write to S3 Delta Bronze...")  # Log start of the write phase

    # Write Micro-Batches to S3 Bronze Delta Table (original sink, unchanged)
    query = (
        raw_bronze_df.writeStream
        .format("delta")                                       # Target storage engine (Delta Lake)
        .outputMode("append")                                  # Append new micro-batches atomically
        .option("checkpointLocation", CHECKPOINT_PATH)         # Write-Ahead Log offset checkpointing directory
        .queryName(QUERY_NAME)                                 # Assign explicit query identifier for UI tracking
        .start(BRONZE_S3_PATH)                                 # Begin background streaming execution to S3 URI
    )

    print("[✔] Bronze Ingestion Pipeline active! Catching up on pending Kafka records...")  # Confirm the stream started
    print("[*] Monitoring streaming metrics at http://localhost:4040 (Spark Web UI)")       # Point to the Spark UI
    print(f"[*] Kafka UI consumer group for lag monitoring: {KAFKA_MONITOR_GROUP}")         # Tell the user which group to look for

    # Keep stream active and listen for incoming micro-batches
    query.awaitTermination()                                   # Block the main thread until the stream stops or fails


if __name__ == "__main__":                                     # Run only when executed directly, not when imported
    main()                                                     # Start the pipeline


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