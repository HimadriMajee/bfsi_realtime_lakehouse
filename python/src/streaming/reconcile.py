"""
================================================================================
BFSI REAL-TIME DATA LAKEHOUSE: END-TO-END RECONCILIATION REPORT
================================================================================
Compares total record counts across all three pipeline stages:
  1. Kafka Broker Topics (Producer Ingestion)
  2. S3 Bronze Delta Table (Raw Storage)
  3. S3 Silver Delta Table (Cleansed Storage)
================================================================================
"""

import os
import sys
from dotenv import load_dotenv
from confluent_kafka import Consumer, TopicPartition
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# 1. Force load .env from project root BEFORE initializing PySpark
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")

if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
    print(f"[*] Loaded AWS Credentials from: {ENV_PATH}")
else:
    load_dotenv()

# 2. Setup JDK and Hadoop paths for Windows execution
LOCAL_JDK_DIR = os.path.join(PROJECT_ROOT, "jdk-17.0.10+7")
if os.path.exists(LOCAL_JDK_DIR):
    os.environ["JAVA_HOME"] = LOCAL_JDK_DIR
    os.environ["PATH"] = os.path.join(LOCAL_JDK_DIR, "bin") + os.path.pathsep + os.environ.get("PATH", "")

LOCAL_HADOOP_DIR = os.path.join(PROJECT_ROOT, "hadoop")
if os.path.exists(LOCAL_HADOOP_DIR):
    os.environ["HADOOP_HOME"] = LOCAL_HADOOP_DIR
    os.environ["PATH"] += os.path.pathsep + os.path.join(LOCAL_HADOOP_DIR, "bin")


def get_kafka_total_message_count(bootstrap_servers: str, topics: list) -> int:
    """Connects to Kafka broker and calculates total messages across topic partitions."""
    print(f"[*] Fetching active message offsets from Kafka ({bootstrap_servers})...")
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'reconciliation_checker_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    total_kafka_count = 0

    try:
        for topic in topics:
            metadata = consumer.list_topics(topic, timeout=5)
            if topic not in metadata.topics:
                print(f"[!] Warning: Topic '{topic}' not found in Kafka broker.")
                continue

            partitions = metadata.topics[topic].partitions
            for partition_id in partitions:
                tp = TopicPartition(topic, partition_id)
                low_offset, high_offset = consumer.get_watermark_offsets(tp, timeout=5)
                partition_count = high_offset - low_offset
                total_kafka_count += partition_count

    except Exception as e:
        print(f"[!] Unable to connect to Kafka broker: {e}")
        return -1
    finally:
        consumer.close()

    return total_kafka_count


def main():
    # Define Target Topics & S3 Paths
    KAFKA_BOOTSTRAP = "localhost:9092"
    KAFKA_TOPICS = [
        "bfsi-retail-transactions",
        "bfsi-highrisk-transactions",
        "bfsi-travel-transactions"
    ]
    
    S3_BUCKET = "bp-bfsi-realtime-lakehouse-245987718167-ap-southeast-5-an"
    BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze/kafka_raw_transactions"
    SILVER_PATH = f"s3a://{S3_BUCKET}/silver/kafka_cleansed_transactions"

    # Step 1: Query Kafka Topic Offsets
    kafka_count = get_kafka_total_message_count(KAFKA_BOOTSTRAP, KAFKA_TOPICS)

    # Step 2: Initialize PySpark Session
    print("[*] Initializing PySpark to read S3 Delta Lake tables...")
    builder = (
        SparkSession.builder
        .appName("BFSI_Lakehouse_Reconciliation")
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

    # Step 3: Count Records in S3 Delta Tables
    print(f"[*] Reading Bronze Delta Table from: {BRONZE_PATH}")
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    bronze_count = bronze_df.count()

    print(f"[*] Reading Silver Delta Table from: {SILVER_PATH}")
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    silver_count = silver_df.count()

    # Step 4: Print Unified Reconciliation Report
    print("\n" + "="*65)
    print("          BFSI LAKEHOUSE END-TO-END RECONCILIATION REPORT          ")
    print("="*65)
    print(f" 1. Total Messages Produced to Kafka Topics : {kafka_count if kafka_count >= 0 else 'N/A'}")
    print(f" 2. Total Raw Records Ingested into S3 Bronze: {bronze_count}")
    print(f" 3. Total Cleansed Records Written to Silver : {silver_count}")
    print("-" * 65)
    print(f" Kafka -> Bronze Ingestion Lag/Unconsumed   : {kafka_count - bronze_count if kafka_count >= 0 else 'N/A'}")
    print(f" Bronze -> Silver Dropped/Filtered Records   : {bronze_count - silver_count}")
    print("="*65 + "\n")


if __name__ == "__main__":
    main()