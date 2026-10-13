"""
================================================================================
BFSI REAL-TIME DATA LAKEHOUSE: SILVER STREAMING TRANSFORMATION (COMPLETE PRODUCTION FILE)
================================================================================
Architecture Layer : Silver (Cleansed, Parsed, Anonymized, & Queryable Storage)
Source Storage     : Bronze S3 Delta Table (s3a://.../bronze/kafka_raw_transactions)
Target Storage     : Silver S3 Delta Table (s3a://.../silver/kafka_cleansed_transactions)
Catalog Engine     : AWS Glue Data Catalog (bfsi_financial_risk_db)

Pipeline Mechanics :
  - Configures JAVA_HOME and HADOOP_HOME paths automatically for Windows OS environments.
  - Reads raw Delta streams directly from the Bronze S3 table landing zone.
  - Enforces explicit JSON schema parsing on unparsed raw_payload strings.
  - Anonymizes sensitive PII (account_id) using salted SHA-256 cryptographic hashing.
  - Applies 15-minute event-time watermarking for bounded late-data processing.
  - Filters corrupt or invalid payloads (null transaction IDs or non-positive amounts).
  - Writes cleansed streaming micro-batches to the Silver Delta Lake layer.
================================================================================
"""

# Importing standard operating system module for path resolution and environment settings
import os
# Importing system parameters module for environment inspection
import sys
# Importing main PySpark SQL entry point class to configure distributed DataFrame sessions
from pyspark.sql import SparkSession
# Importing PySpark SQL functions for column transformations, JSON parsing, hashing, string manipulation, and temporal operations
from pyspark.sql.functions import (
    col,               # Reference DataFrame columns by name
    from_json,         # Parse JSON strings into structured PySpark structs using an explicit schema
    sha2,              # Perform cryptographic SHA-2 hashing for PII anonymization
    concat_ws,         # Concatenate string columns with a custom delimiter (safe against nulls)
    lit,               # Wrap literal values (like salt strings) as PySpark SQL columns
    to_timestamp,      # Cast formatted timestamp strings into native Spark TimestampType
    current_timestamp  # Generate current processing timestamp for audit tracking
)
# Importing PySpark SQL data types to construct explicit schema contracts
from pyspark.sql.types import (
    StructType,        # Defines the root schema structure
    StructField,       # Represents an individual field within a struct
    StringType,        # String data type representation
    DoubleType,        # Double-precision floating-point type for monetary amounts
    TimestampType      # Temporal timestamp type representation
)
# Importing Delta Lake configuration helper to inject Delta extensions into PySpark
from delta import configure_spark_with_delta_pip

# ==============================================================================
# WINDOWS OS COMPATIBILITY & JAVA_HOME / HADOOP_HOME RESOLUTION
# ==============================================================================
# Resolves the absolute root directory of the project dynamically from current script path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))

# 1. Automatically locate and configure JAVA_HOME if not set in environment
LOCAL_JDK_DIR = os.path.join(PROJECT_ROOT, "jdk-17.0.10+7")
if os.path.exists(LOCAL_JDK_DIR):
    os.environ["JAVA_HOME"] = LOCAL_JDK_DIR
    os.environ["PATH"] = os.path.join(LOCAL_JDK_DIR, "bin") + os.path.pathsep + os.environ.get("PATH", "")

# 2. Automatically locate and configure HADOOP_HOME for Windows winutils execution
LOCAL_HADOOP_DIR = os.path.join(PROJECT_ROOT, "hadoop")
if os.path.exists(LOCAL_HADOOP_DIR):
    os.environ["HADOOP_HOME"] = LOCAL_HADOOP_DIR
    os.environ["PATH"] += os.path.pathsep + os.path.join(LOCAL_HADOOP_DIR, "bin")

# ==============================================================================
# CONFIGURATION CONSTANTS
# ==============================================================================
# Globally unique AWS S3 Bucket Name holding the Medallion architecture storage layers
S3_BUCKET = "bp-bfsi-realtime-lakehouse-245987718167-ap-southeast-5-an"

# Source S3 storage path for raw Bronze Delta Lake table
BRONZE_S3_PATH = f"s3a://{S3_BUCKET}/bronze/kafka_raw_transactions"
# Target S3 storage path for cleansed Silver Delta Lake table
SILVER_S3_PATH = f"s3a://{S3_BUCKET}/silver/kafka_cleansed_transactions"
# Target S3 path for Silver streaming offset checkpointing
CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/checkpoints/silver_transformation"

# Cryptographic salt key used for PII masking (In production, retrieve dynamically from AWS Secrets Manager)
PII_SALT = "BFSI_SECURE_SALT_2026"

# ==============================================================================
# EXPLICIT SCHEMA CONTRACT FOR RAW JSON PAYLOAD
# ==============================================================================
# What: Constructs an explicit PySpark StructType schema matching the expected producer JSON structure.
# Why: Enforces compile-time schema contracts. Avoids inferSchema overhead which requires scanning data.
# Alternative Practice: Using AWS Glue Schema Registry or Apache Avro schema registries for automated ingress validation.
TRANSACTION_SCHEMA = StructType([
    # Unique transaction identifier GUID string
    StructField("transaction_id", StringType(), True),
    # Sensitive bank account identifier (Requires PII hashing downstream)
    StructField("account_id", StringType(), True),
    # Transaction monetary value in double precision
    StructField("amount", DoubleType(), True),
    # Standard ISO currency code (e.g., USD, MYR, EUR)
    StructField("currency", StringType(), True),
    # Merchant entity identifier
    StructField("merchant_id", StringType(), True),
    # Categorical classification of transaction (e.g., POS, ATM, ONLINE)
    StructField("transaction_type", StringType(), True),
    # Geographical location or store identifier
    StructField("location", StringType(), True),
    # Original event timestamp string generated by edge devices/producers
    StructField("timestamp", StringType(), True)
])


def create_spark_session() -> SparkSession:
    """
    Initializes and returns a SparkSession configured with Delta Lake extensions,
    and Apache Hadoop AWS S3 drivers.
    """
    # Print status message indicating SparkSession initialization step
    print("[*] Initializing Spark Session for Silver Transformation Layer...")
    
    # Define additional Maven package coordinates for Hadoop AWS S3 connectivity
    extra_packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ]
    
    # Start building the base SparkSession configuration
    builder = (
        # Obtain existing SparkSession or create a new builder instance
        SparkSession.builder
        # Set application name displayed in Spark UI (http://localhost:4040)
        .appName("BFSI_Silver_Transformation_Pipeline")
        # Enable Delta Lake SQL syntax extensions inside PySpark
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # Set Delta Catalog as default catalog implementation for Spark catalog management
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Register S3A FileSystem driver implementation class for s3a:// URI protocol
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # USE IN-MEMORY FAST UPLOAD BUFFERING (Bypasses Windows disk allocator & NativeIO C++ DLL permission errors)
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
    
    # Inject Delta Lake dependencies and extra packages, then build and return active SparkSession
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=extra_packages
    ).getOrCreate()


def main():
    """
    Main execution entry point for PySpark Silver Layer Streaming Transformation Pipeline.
    """
    # Initialize and retrieve configured SparkSession instance
    spark = create_spark_session()
    # Reduce console log verbosity to WARN to highlight critical operational outputs
    spark.sparkContext.setLogLevel("WARN")

    # Print status message indicating input source path
    print(f"[*] Reading streaming Delta table from Bronze S3 path: {BRONZE_S3_PATH}")

    # ==========================================================================
    # STEP 1: READ DELTA STREAM FROM BRONZE LAYER
    # ==========================================================================
    # What: Connects PySpark Structured Streaming to read from the Bronze Delta Lake table path on AWS S3.
    # Why: Delta Lake acts as a native streaming source. Reading directly from Bronze decouples processing from Kafka.
    # Alternative Practice: Auto Loader (cloudFiles) in Databricks for automatic file discovery and schema evolution.
    bronze_stream = (
        # Access Structured Streaming read interface
        spark.readStream
        # Specify format as 'delta' to leverage transactional Delta reader
        .format("delta")
        # Load Bronze Delta table path from S3
        .load(BRONZE_S3_PATH)
    )

    # ==========================================================================
    # STEP 2: PARSE RAW JSON PAYLOAD USING EXPLICIT SCHEMA
    # ==========================================================================
    # What: Applies from_json() on the raw_payload string column using the TRANSACTION_SCHEMA contract.
    # Why: Converts unstructured text strings into a structured PySpark struct column ('payload') for direct column-level operations.
    # Alternative Practice: Native functions avoid JVM/Python process serialization overhead inherent to UDFs.
    parsed_df = bronze_stream.withColumn(
        # Create a temporary nested struct column named 'payload'
        "payload",
        # Parse raw_payload JSON string into structured type
        from_json(col("raw_payload"), TRANSACTION_SCHEMA)
    )

    # ==========================================================================
    # STEP 3: EXTRACT FIELDS, APPLY PII MASKING, & FORMAT TIMESTAMPS
    # ==========================================================================
    # What: Selects structured fields, applies cryptographic SHA-256 hashing on account_id, and converts timestamps.
    # Why: 
    #   - PII Masking: Financial regulatory frameworks (GDPR, PCI-DSS, BCBS 239) mandate anonymizing account details before exposure in analytical layers.
    #   - Salting (concat_ws): Adding a secret salt string before hashing prevents Rainbow Table brute-force dictionary attacks.
    #   - Timestamp Parsing: Casting raw timestamp strings into native TimestampType enables event-time windowing and watermarking downstream.
    silver_df = (
        parsed_df.select(
            # Extract transaction identifier GUID from nested struct
            col("payload.transaction_id").alias("transaction_id"),
            # Cryptographic PII Anonymization: Computes SHA-256 digest of account_id concatenated with secret salt
            sha2(
                # Concatenate account_id string with PII_SALT string using double colon delimiter
                concat_ws("::", col("payload.account_id"), lit(PII_SALT)),
                # Specify 256-bit SHA-2 hashing algorithm
                256
            ).alias("masked_account_id"),
            # Extract transaction monetary value
            col("payload.amount").alias("amount"),
            # Extract currency code string
            col("payload.currency").alias("currency"),
            # Extract merchant identifier
            col("payload.merchant_id").alias("merchant_id"),
            # Extract transaction category classification
            col("payload.transaction_type").alias("transaction_type"),
            # Extract transaction geographical location
            col("payload.location").alias("location"),
            # Parse event-time ISO string into native PySpark TimestampType
            to_timestamp(col("payload.timestamp")).alias("transaction_timestamp"),
            # Preserve original Kafka topic name for domain isolation
            col("kafka_topic"),
            # Preserve original Kafka partition ID for lineage tracking
            col("kafka_partition"),
            # Preserve original Kafka record offset for auditing
            col("kafka_offset"),
            # Preserve Bronze ingestion timestamp for end-to-end SLA processing latency measurement
            col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
            # Generate current timestamp recording Silver transformation completion time
            current_timestamp().alias("silver_processed_timestamp")
        )
        # ======================================================================
        # STEP 4: DATA QUALITY CHECKS & CORRUPT PAYLOAD FILTERING
        # ======================================================================
        # What: Filters out rows missing primary keys or containing invalid monetary amounts.
        # Why: Prevents dirty data (malformed JSON, null keys, negative values) from contaminating downstream Gold aggregates.
        # Alternative Practice: Routing corrupt records to a Dead-Letter Queue (DLQ) Delta table using PySpark badRecordsPath.
        .filter(
            # Validate that transaction_id is not null
            col("transaction_id").isNotNull() &
            # Validate that anonymized account identifier exists
            col("masked_account_id").isNotNull() &
            # Ensure monetary transaction amount is positive
            (col("amount") > 0)
        )
        # ======================================================================
        # STEP 5: EVENT-TIME WATERMARKING
        # ======================================================================
        # What: Establishes a 15-minute watermark bound on the event timestamp column ('transaction_timestamp').
        # Why: Tracks event-time progression and tells PySpark how long to wait for late-arriving records. Allows state store cleanup.
        .withWatermark("transaction_timestamp", "15 minutes")
    )

    # Print output paths for user confirmation
    print(f"[*] Starting Silver Delta stream write to: {SILVER_S3_PATH}")
    print(f"[*] Checkpointing configured at: {CHECKPOINT_PATH}")

    # ==========================================================================
    # STEP 6: WRITE CLEANSED STREAM TO SILVER DELTA LAKE TABLE
    # ==========================================================================
    # What: Initiates streaming write to the Silver Delta Lake table on AWS S3 with checkpointing.
    # Why: Append mode ensures new cleansed micro-batches are appended committedly. Checkpointing enables fault-tolerant recovery.
    query = (
        # Access Structured Streaming write interface
        silver_df.writeStream
        # Specify target output format as Delta Lake
        .format("delta")
        # Explicitly name the query for clear identification in Spark Web UI (http://localhost:4040)
        .queryName("SilverTransformationStream")
        # Set output mode to 'append' (standard for immutable transformed streams)
        .outputMode("append")
        # Specify S3 checkpoint location for fault-tolerant offset and state management
        .option("checkpointLocation", CHECKPOINT_PATH)
        # Start execution targeting the Silver S3 bucket path
        .start(SILVER_S3_PATH)
    )

    # Print success message indicating active transformation loop
    print("[✔] Silver Transformation Stream active!")
    print("[*] Monitoring pipeline metrics at http://localhost:4040 (Spark Web UI)")
    
    # Keep execution thread active awaiting system signals or manual termination
    query.awaitTermination()


# Script entry point execution
if __name__ == "__main__":
    main()


# ================================================================================
# ENTERPRISE BOTTLENECK ANALYSIS, RESOLUTIONS & INTERVIEW QA REFERENCE
# ================================================================================
#
# --------------------------------------------------------------------------------
# PART 1: SILVER LAYER STREAMING BOTTLENECK & FAILURE MATRIX
# --------------------------------------------------------------------------------
#
# Issue / Symptom                  | Root Cause                        | Resolution Technique
# ---------------------------------|-----------------------------------|------------------------------------------------
# A. Schema Drift / Malformed JSON | Ingress producers change payload  | 1. Use explicit StructType schemas with nullable=True.
#    Crashes Streaming Pipeline    | keys without updating consumers.  | 2. Quarantine bad rows into a Dead-Letter Queue (DLQ)
#                                  |                                   |    table using PySpark badRecordsPath or filter logic.
# ---------------------------------|-----------------------------------|------------------------------------------------
# B. PII Hash Computation          | SHA-256 calculation executed on   | 1. Use native PySpark SQL functions (sha2) rather
#    Performance Bottlenecks       | millions of rows via Python UDF.  |    than Python UDFs to avoid JVM-Python serialization.
#                                  |                                   | 2. Optimize JVM heap memory settings.
# ---------------------------------|-----------------------------------|------------------------------------------------
# C. State Store Memory Leak /     | Stateful operations (joins or     | Apply explicit watermarking (.withWatermark()):
#    OutOfMemoryError (OOM)        | deduplication) lack watermarks,   | Allows engine to purge stale state data older than
#                                  | keeping state in memory forever.  | the configured threshold.
# ---------------------------------|-----------------------------------|------------------------------------------------
# D. S3 API Throttling (HTTP 503)  | High-frequency micro-batch writes | Tune streaming trigger interval:
#    & File Proliferation          | generate thousands of small files.| .trigger(processingTime='1 minute') or run periodic
#                                  |                                   | Delta Lake OPTIMIZE compaction jobs.
#
# --------------------------------------------------------------------------------
# PART 2: TOP BFSI SILVER LAYER DATA ENGINEERING INTERVIEW QUESTIONS
# --------------------------------------------------------------------------------
#
# Q1: What is the purpose of the Silver Layer in Medallion Architecture?
# A1: The Silver layer represents the "Cleansed and Conformed" data zone. It takes raw, unparsed data
#     from Bronze, enforces strict schemas, cleans malformed records, anonymizes sensitive PII fields,
#     and standardizes types/timestamps into queryable, enterprise-ready Delta Lake tables.
#
# Q2: Why use native PySpark functions (sha2, from_json) instead of Python UDFs for Silver transformations?
# A2: Native PySpark SQL functions execute directly inside the JVM using optimized Catalyst expressions and
#     Tungsten binary memory management. Python UDFs require serializing data back and forth between JVM and
#     Python worker processes, causing massive performance overhead and memory bottlenecks.
#
# Q3: How do you handle PII masking safely in a financial data lakehouse?
# A3: Sensitive fields like account numbers or national IDs should be anonymized using cryptographic hashing
#     with a secret salt (e.g., sha2(concat_ws("::", col("account_id"), lit(SALT)), 256)).
#     Salting prevents dictionary/rainbow table attacks. Secret salts must be stored securely in enterprise
#     vaults like AWS Secrets Manager.
#
# Q4: What is Watermarking in PySpark Structured Streaming, and why is it critical in Silver/Gold layers?
# A4: Watermarking defines a time bound (e.g., 15 minutes) on event time, specifying how long the streaming
#     engine waits for late-arriving events. It enables the engine to safely purge historical state memory
#     from executor disk/RAM, preventing OutOfMemory (OOM) errors during stateful stream transformations.
#
# Q5: How do you handle malformed or bad records in the Silver layer without dropping data silently?
# A5: In production enterprise pipelines, instead of simply discarding records with .filter(), records failing
#     validation checks are routed to a Dead-Letter Queue (DLQ) Delta table (e.g., silver_quarantine_transactions).
#     This allows data governance and operations teams to inspect, fix, and reprocess bad records without losing auditability.
# ================================================================================