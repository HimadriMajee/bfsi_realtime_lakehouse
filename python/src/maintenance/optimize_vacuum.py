"""
====================================================================================================
BFSI REAL-TIME DATA LAKEHOUSE: DELTA LAKE OPTIMIZE & VACUUM MAINTENANCE PIPELINE
====================================================================================================
File Name   : python/src/maintenance/optimize_vacuum.py
Layer       : Data Lake Operations & Storage Maintenance
Target      : S3 Bronze, Silver, and Gold Delta Tables

Purpose:
  1. OPTIMIZE (File Compaction): Merges micro-batch small files into target ~1GB files to solve
     the "Small File Problem" in cloud object storage, drastically boosting S3 query performance.
  2. Z-ORDER BY (Multi-Dimensional Indexing): Co-locates related data along specified columns
     (e.g., transaction_timestamp, location) to enable effective file-skipping.
  3. VACUUM (Storage Cleanup): Purges unreferenced historical Parquet files past the retention 
     threshold (default 168 hours / 7 days) to minimize S3 storage costs.

====================================================================================================
INDUSTRY PRACTICES & ARCHITECTURAL REASONING
====================================================================================================
1. WHY SEPARATE MAINTENANCE FROM STREAMING INGESTION?
   - INDUSTRY PRACTICE: Run maintenance pipelines out-of-band as scheduled batch jobs (e.g., daily 
     Airflow DAGs at off-peak hours) rather than executing `OPTIMIZE` inline inside streaming jobs.
   - REASONING: Running heavy compaction operations inline within low-latency PySpark streaming queries 
     causes write latencies to spike, risks concurrent file modification conflicts, and drains 
     streaming driver resources.

2. WHY Z-ORDERING INSTEAD OF OVER-PARTITIONING?
   - INDUSTRY PRACTICE: Avoid partitioning on high-cardinality keys like `customer_id` or `timestamp`. 
     Instead, partition only by coarse time grains (e.g., `year` or `date`) and use Z-Ordering for 
     high-cardinality filtering columns.
   - REASONING: Over-partitioning creates thousands of directory sub-folders containing 1-2 KB files, 
     leading to severe S3 metadata throttling. Z-Ordering uses Space-Filling Curves (Hilbert Curves) 
     to physically pack correlated values together within single Parquet files, allowing query 
     engines (Athena/Spark) to skip irrelevant files during queries.

3. WHY ENFORCE VACUUM RETENTION SAFETY THRESHOLDS?
   - INDUSTRY PRACTICE: Maintain the default 7-day (168-hour) retention window in production.
   - REASONING: Delta Lake uses Multi-Version Concurrency Control (MVCC). Aggressively running 
     `VACUUM retention 0 hours` while long-running analytics queries or streaming consumers are 
     reading historical snapshots causes runtime `FileNotFoundException` crashes.

====================================================================================================
SENIOR DATA ENGINEER INTERVIEW PREPARATION (Q&A)
====================================================================================================
Q1: How does Delta Lake's `OPTIMIZE` command resolve the "Small File Problem"?
    - Answer: Streaming ingestion constantly writes small Parquet files (e.g., 50 KB–2 MB) to S3 per 
      micro-batch. `OPTIMIZE` reads these tiny files, merges them into optimal ~1 GB Parquet files, 
      and writes back a new transaction log commit.

Q2: What happens under the hood when `VACUUM` is executed?
    - Answer: `VACUUM` scans the Delta transaction log (`_delta_log/`) to identify Parquet files that 
      are no longer referenced by the latest active table snapshot. If those files are older than 
      the defined retention period, `VACUUM` physically deletes them from S3 storage to save cost.
====================================================================================================
"""

import os                                         # Standard library for operating system operations and environment variables
import sys                                        # System-specific parameters and functions for stream management
import logging                                    # Python standard logging library to adjust log verbosity levels
from dotenv import load_dotenv                    # Helper module to parse and load key-value pairs from .env file

# --------------------------------------------------------------------------------------------------
# STEP 1: MUTE VERBOSE LOGGERS
# --------------------------------------------------------------------------------------------------
# INDUSTRY PRACTICE: Suppress Py4J JVM bridge and PySpark log noise to ensure clean CI/CD production logs
logging.getLogger("py4j").setLevel(logging.ERROR)     # Set Py4J log level to ERROR only
logging.getLogger("pyspark").setLevel(logging.ERROR)  # Set PySpark internal log level to ERROR only

# --------------------------------------------------------------------------------------------------
# STEP 2: DYNAMIC ENVIRONMENT & PATH RESOLUTION
# --------------------------------------------------------------------------------------------------
# INDUSTRY PRACTICE: Avoid hardcoding absolute local paths; compute paths dynamically relative to the script location
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))  # Calculate repo root path (3 levels up)
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")                                         # Build full path to root .env file

# Load AWS IAM Secrets dynamically into system environment variables
if os.path.exists(ENV_PATH):                          # Check if .env file exists on local disk
    load_dotenv(ENV_PATH)                             # Load .env key-value pairs into system os.environ
    print(f"[*] Loaded AWS Credentials from: {ENV_PATH}") # Print execution status feedback
else:
    load_dotenv()                                     # Fallback to default load_dotenv behavior if path differs

# Configure local JDK 17 binaries dynamically for local Spark execution
LOCAL_JDK_DIR = os.path.join(PROJECT_ROOT, "jdk-17.0.10+7")  # Locate local JDK directory
if os.path.exists(LOCAL_JDK_DIR):                           # If local JDK directory exists in repository
    os.environ["JAVA_HOME"] = LOCAL_JDK_DIR                 # Bind JAVA_HOME environment variable to local JDK path
    os.environ["PATH"] = os.path.join(LOCAL_JDK_DIR, "bin") + os.path.pathsep + os.environ.get("PATH", "")  # Prepend bin directory to PATH

# Configure local Hadoop winutils dynamically for local Windows filesystem integration
LOCAL_HADOOP_DIR = os.path.join(PROJECT_ROOT, "hadoop")     # Locate local Hadoop winutils directory
if os.path.exists(LOCAL_HADOOP_DIR):                        # If local Hadoop winutils directory exists
    os.environ["HADOOP_HOME"] = LOCAL_HADOOP_DIR            # Bind HADOOP_HOME environment variable
    os.environ["PATH"] += os.path.pathsep + os.path.join(LOCAL_HADOOP_DIR, "bin")  # Append Hadoop bin to PATH

# Import PySpark and Delta Lake libraries AFTER environment paths are fully initialized
from pyspark.sql import SparkSession             # Imports main entry point for Spark SQL functionality
from delta.tables import DeltaTable               # Imports DeltaTable class for programmatically running OPTIMIZE and VACUUM
from delta import configure_spark_with_delta_pip  # Helper function to auto-configure Delta Lake JAR dependencies


# --------------------------------------------------------------------------------------------------
# STEP 3: SPARK SESSION BUILDER
# --------------------------------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    """
    Initializes a SparkSession pre-configured with Delta Lake catalog handlers and S3A connector drivers.
    
    INDUSTRY PRACTICE: Pass explicit Hadoop-AWS and Delta Lake Maven coordinates matching the Spark binary version.
    """
    print("[*] Initializing Spark Session for Delta Lake Maintenance Operations...") # Console status update

    # Define Maven dependencies required for AWS S3 connectivity and Delta Lake execution
    extra_packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",     # AWS S3A FileSystem connector package
        "io.delta:delta-spark_2.12:3.1.0"         # Delta Lake core engine package matching PySpark 3.5.x
    ]

    # Build Spark configuration options
    builder = (
        SparkSession.builder                       # Obtain SparkSession builder instance
        .appName("BFSI_Delta_Lake_Maintenance_Pipeline") # Set application name for Spark Web UI tracking
        # Configure Delta Lake catalog and SQL extension overrides
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") # Inject Delta custom SQL parser extensions
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") # Override standard Spark catalog
        # Configure S3A file system implementation details
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") # Define standard S3A implementation class
        .config("spark.hadoop.fs.s3a.fast.upload", "true")     # Enable streaming buffer upload mechanism to AWS S3
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") # Allocate off-heap bytebuffers for upload efficiency
        .config("spark.hadoop.io.nativeio.enabled", "false")   # Disable Windows C++ native IO libraries to prevent winutils crashes
        # Configure AWS Credentials Provider
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",    # Define credential provider class
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider" # Read AWS keys directly from OS environment variables
        )
        # Enable parallel deletion handling during VACUUM operations
        .config("spark.databricks.delta.vacuum.parallelDelete.enabled", "true") # Use multithreaded S3 DELETE operations for VACUUM speed
    )

    # Attach Maven packages and build SparkSession
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate() # Instantiate configured session


# --------------------------------------------------------------------------------------------------
# STEP 4: LAYER-BY-LAYER MAINTENANCE EXECUTION FUNCTION
# --------------------------------------------------------------------------------------------------
def optimize_and_vacuum_layer(spark: SparkSession, table_path: str, zorder_cols: list = None, retention_hours: int = 168):
    """
    Executes Delta Lake file compaction (OPTIMIZE), optional Z-Ordering indexing, and storage cleanup (VACUUM).
    
    Parameters:
      - spark: Active SparkSession instance
      - table_path: Target S3 Delta table URI (s3a://...)
      - zorder_cols: Optional list of string column names to apply multi-dimensional clustering on
      - retention_hours: Retention threshold in hours before unreferenced files are deleted (default: 168 hours / 7 days)
    """
    print(f"\n" + "="*70)                                 # Separator formatting line
    print(f"[*] Starting Maintenance for Delta Table: {table_path}") # Print target table URI
    print("="*70)                                         # Separator formatting line

    try:
        # Programmatically bind to the Delta table instance at the specified S3 path
        delta_table = DeltaTable.forPath(spark, table_path) # Load Delta Lake table metadata from S3 path

        # 1. Execute OPTIMIZE Compaction (and optional Z-Ordering)
        if zorder_cols:                                   # If Z-Ordering column list was provided
            zorder_str = ", ".join(zorder_cols)           # Format list into comma-separated string for console logging
            print(f"[*] Executing OPTIMIZE with Z-ORDER BY ({zorder_str})...") # Print status update
            # INDUSTRY PRACTICE: Z-Order high-cardinality query predicate columns to accelerate data skipping
            delta_table.optimize().executeZOrderBy(zorder_cols) # Compact files and re-cluster data along Z-Order curve
        else:
            print("[*] Executing standard OPTIMIZE file compaction...") # Print status update
            # INDUSTRY PRACTICE: Standard compaction merges small micro-batch files into target ~1 GB files
            delta_table.optimize().executeCompaction()   # Merge small files without re-clustering
        
        print("[✔] OPTIMIZE Compaction complete.")         # Output success feedback

        # 2. Execute VACUUM Storage Cleanup
        print(f"[*] Executing VACUUM (Retention Threshold: {retention_hours} hours)...") # Print status update
        # INDUSTRY PRACTICE: Purge unreferenced historical files past the 7-day retention safety threshold
        delta_table.vacuum(retention_hours)               # Perform physical deletion of expired Parquet files in S3
        print("[✔] VACUUM Cleanup complete.")             # Output success feedback

    except Exception as e:
        print(f"[✘] Maintenance Error on {table_path}: {str(e)}") # Catch and output execution errors gracefully


# --------------------------------------------------------------------------------------------------
# STEP 5: MAIN WORKFLOW ENTRY POINT
# --------------------------------------------------------------------------------------------------
def main():
    # Initialize Spark Session for maintenance
    spark = create_spark_session()                        # Instantiate session via builder
    spark.sparkContext.setLogLevel("ERROR")               # Set root log level to ERROR

    # Define Target S3 Medallion Layer Paths
    S3_BUCKET = "bp-bfsi-realtime-lakehouse-245987718167-ap-southeast-5-an" # Target S3 bucket identifier
    BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze/kafka_raw_transactions"       # Path to Bronze raw Delta layer
    SILVER_PATH = f"s3a://{S3_BUCKET}/silver/kafka_cleansed_transactions"   # Path to Silver cleansed Delta layer
    GOLD_PATH   = f"s3a://{S3_BUCKET}/gold/realtime_risk_metrics"           # Path to Gold risk metrics Delta layer

    # ----------------------------------------------------------------------------------------------
    # EXECUTION STRATEGY ACCORDING TO INDUSTRY BEST PRACTICES:
    # ----------------------------------------------------------------------------------------------
    
    # 1. Bronze Layer Maintenance: File Compaction only
    # INDUSTRY PRACTICE: Do not Z-Order Bronze raw string payloads. Simply compact micro-batch files.
    optimize_and_vacuum_layer(spark, BRONZE_PATH)

    # 2. Silver Layer Maintenance: Compaction + Z-Ordering on Filter Columns
    # INDUSTRY PRACTICE: Z-Order Silver layer on time and location because downstreams query by timestamp and location.
    optimize_and_vacuum_layer(spark, SILVER_PATH, zorder_cols=["transaction_timestamp", "location"])

    # 3. Gold Layer Maintenance: Compaction + Z-Ordering on Business Dimensions
    # INDUSTRY PRACTICE: Z-Order Gold aggregations on reporting window start and risk alert level for BI dashboard performance.
    optimize_and_vacuum_layer(spark, GOLD_PATH, zorder_cols=["window_start", "risk_classification"])

    # Final Execution Summary Log
    print("\n" + "="*70)
    print("[✔] ALL MEDALLION LAYERS OPTIMIZED AND CLEANED SUCCESSFULLY!") # Print overall success message
    print("="*70 + "\n")


if __name__ == "__main__":
    main()                                                # Execute main entry point function