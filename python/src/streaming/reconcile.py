"""
====================================================================================================
BFSI REAL-TIME DATA LAKEHOUSE: UNIFIED END-TO-END RECONCILIATION PIPELINE
====================================================================================================
File Name   : python/src/streaming/reconcile.py
Layer       : System Audit & Data Quality Reconciliation
Purpose     : Verifies data completeness and zero data loss across all Medallion layers:
              1. Kafka Producer / Broker Topics (Ingestion)
              2. S3 Bronze Delta Table (Raw Storage)
              3. S3 Silver Delta Table (Cleansed Storage)
              4. S3 Gold Delta Table (Real-Time Risk Aggregations)

====================================================================================================
PRODUCTION BOTTLENECK ANALYSIS & RESOLUTION STRATEGIES
====================================================================================================
1. BOTTLENECK: Full S3 File Scans on df.count()
   - ISSUE: Executing .count() on massive Delta tables forces Spark to query S3 object metadata.
   - RESOLUTION: In ultra-large scale production, query Delta log commit stats directly via 
     DeltaTable.forPath(spark, path).history() or Delta transaction log JSON files.

2. BOTTLENECK: Kafka Retention Expiration Discrepancy
   - ISSUE: Kafka log retention (e.g., log.retention.hours=24) deletes old messages, causing 
     Kafka message count (High - Low Watermark) to decrease while Bronze S3 count grows indefinitely.
   - RESOLUTION: Audit Kafka offset logs against producer emission checkpoints rather than total
     broker watermarks for long-running streaming clusters.

3. BOTTLENECK: Gold Layer Row Count Divergence
   - ISSUE: Gold table stores aggregated tumbling windows (10-min windows), so raw row count 
     will naturally be significantly lower than Silver atomic transactions.
   - RESOLUTION: Validate Gold layer completeness by summing total_volume or transaction_count 
     aggregates rather than comparing raw row counts.

====================================================================================================
INDUSTRY BEST PRACTICES & INTERVIEW PREPARATION (Q&A)
====================================================================================================
Q1: What is the difference between Kafka High Watermark and Consumer Offset?
    - High Watermark (HW): Offset of the latest message replicated across all In-Sync Replicas (ISR).
    - Consumer Offset: Current offset position processed by a consumer group. Lag = HW - Consumer Offset.

Q2: Why does Delta Lake perform faster count queries than raw Parquet/JSON?
    - Delta maintains explicit commit logs (_delta_log/*.json) containing exact table metadata, 
      allowing Spark to return counts directly from commit stats without full data scans.

Q3: How do you handle late-arriving data in streaming reconciliation?
    - Using event-time watermarking (.withWatermark("timestamp", "15 minutes")). Records within 
      the window update Silver/Gold; records beyond the threshold are dropped from stateful memory
      but remain preserved in Bronze raw storage for historical audit backfills.
====================================================================================================
"""

import os
import sys
import logging

# Mute Python logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

from dotenv import load_dotenv

# --------------------------------------------------------------------------------------------------
# ENVIRONMENT & PATH SETUP
# --------------------------------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")

if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
else:
    load_dotenv()

LOCAL_JDK_DIR = os.path.join(PROJECT_ROOT, "jdk-17.0.10+7")
if os.path.exists(LOCAL_JDK_DIR):
    os.environ["JAVA_HOME"] = LOCAL_JDK_DIR
    os.environ["PATH"] = os.path.join(LOCAL_JDK_DIR, "bin") + os.path.pathsep + os.environ.get("PATH", "")

LOCAL_HADOOP_DIR = os.path.join(PROJECT_ROOT, "hadoop")
if os.path.exists(LOCAL_HADOOP_DIR):
    os.environ["HADOOP_HOME"] = LOCAL_HADOOP_DIR
    os.environ["PATH"] += os.path.pathsep + os.path.join(LOCAL_HADOOP_DIR, "bin")

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from confluent_kafka import Consumer, TopicPartition


# --------------------------------------------------------------------------------------------------
# KAFKA OFFSET RECONCILIATION FUNCTION
# --------------------------------------------------------------------------------------------------
def get_kafka_total_count(bootstrap_servers: str, topics: list) -> str:
    """Connects to Kafka broker via 127.0.0.1 and sums total partition watermark offsets."""
    try:
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'reconciliation_e2e_checker',
            'auto.offset.reset': 'earliest',
            'log_level': 0  # Silence librdkafka stdout error logs
        }
        consumer = Consumer(conf)
        total_kafka_count = 0

        for topic in topics:
            metadata = consumer.list_topics(topic, timeout=3)
            if topic not in metadata.topics:
                continue

            partitions = metadata.topics[topic].partitions
            for partition_id in partitions:
                tp = TopicPartition(topic, partition_id)
                low_offset, high_offset = consumer.get_watermark_offsets(tp, timeout=3)
                total_kafka_count += (high_offset - low_offset)

        consumer.close()
        return str(total_kafka_count)
    except Exception:
        return "N/A (Broker Unreachable)"


# --------------------------------------------------------------------------------------------------
# DELTA LAKE S3 STORAGE RECONCILIATION FUNCTION
# --------------------------------------------------------------------------------------------------
def get_delta_table_count(spark: SparkSession, s3_path: str) -> str:
    """Reads a Delta Lake table snapshot from S3 and returns total row count."""
    try:
        df = spark.read.format("delta").load(s3_path)
        return str(df.count())
    except Exception:
        return "N/A (Layer Not Initialized)"


# --------------------------------------------------------------------------------------------------
# MAIN WORKFLOW
# --------------------------------------------------------------------------------------------------
def main():
    # Force host loopback address to avoid 'kafka:9092' unresolved DNS errors on Windows
    KAFKA_BOOTSTRAP = "127.0.0.1:9092"
    KAFKA_TOPICS = [
        "bfsi-retail-transactions",
        "bfsi-highrisk-transactions",
        "bfsi-travel-transactions"
    ]
    
    S3_BUCKET = "bp-bfsi-realtime-lakehouse-245987718167-ap-southeast-5-an"
    BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze/kafka_raw_transactions"
    SILVER_PATH = f"s3a://{S3_BUCKET}/silver/kafka_cleansed_transactions"
    GOLD_PATH   = f"s3a://{S3_BUCKET}/gold/realtime_risk_metrics"

    # Fetch Kafka Message Count
    kafka_val = get_kafka_total_count(KAFKA_BOOTSTRAP, KAFKA_TOPICS)

    # Initialize PySpark Session
    builder = (
        SparkSession.builder
        .appName("BFSI_E2E_Reconciliation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.io.nativeio.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    )

    spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Fetch Delta Table Layer Counts
    bronze_val = get_delta_table_count(spark, BRONZE_PATH)
    silver_val = get_delta_table_count(spark, SILVER_PATH)
    gold_val   = get_delta_table_count(spark, GOLD_PATH)

    # Calculate Data Quality Dropped Record Difference
    b_to_s_diff = "N/A"
    if bronze_val.isdigit() and silver_val.isdigit():
        b_to_s_diff = str(int(bronze_val) - int(silver_val))

    # Print Clean Report
    print("\n" + "="*68)
    print("       BFSI REAL-TIME LAKEHOUSE: END-TO-END RECONCILIATION REPORT       ")
    print("="*68)
    print(f" 1. Kafka Producer / Broker Topics Total : {kafka_val}")
    print(f" 2. S3 Bronze Delta Table (Raw Ingest)  : {bronze_val}")
    print(f" 3. S3 Silver Delta Table (Cleansed)    : {silver_val}")
    print(f" 4. S3 Gold Delta Table (Risk Metrics)  : {gold_val}")
    print("-" * 68)
    print(f" Data Quality Filtered Records (Bronze -> Silver) : {b_to_s_diff}")
    print("="*68 + "\n")


if __name__ == "__main__":
    main()