"""
================================================================================
BFSI REAL-TIME DATA LAKEHOUSE: ENTERPRISE TRANSACTION PRODUCER
================================================================================
Architecture Layer : Event Generation & Ingress Producer
Target Streaming   : Local Apache Kafka KRaft Broker (Multi-Topic Routing)
Data Domain        : Banking, Financial Services, and Insurance (BFSI)

Key Responsibilities:
  - Generates realistic synthetic transaction payloads using Faker.
  - Implements dynamic domain-based topic routing (Retail vs. Travel vs. High-Risk).
  - Enforces strict event ordering per customer account by keying messages with `account_id`.
  - Configures enterprise delivery guarantees (`acks=all`, retry policies, batching).
================================================================================
"""

import json                                              # Standard library module to serialize Python dict payloads into JSON strings
import random                                            # Standard library module to generate pseudo-random category choices and transaction amounts
import time                                              # Standard library module used to control producer stream throughput via sleep intervals
from typing import Dict, Any, Tuple                       # Type hinting annotations for structured function return signatures
from faker import Faker                                  # External library to generate realistic synthetic enterprise metadata (UUIDs, company names, cities)
from kafka import KafkaProducer                          # Apache Kafka client producer class from kafka-python-ng library
from kafka.errors import KafkaError                      # Exception class to capture and handle Kafka network and delivery failures gracefully

fake = Faker()                                           # Instantiates the Faker generator object to produce mock business entities

# External host listener address mapped in docker-compose.yml for host-to-container communication
BOOTSTRAP_SERVERS = ['localhost:9094']

# Domain-driven topic constants matching pre-provisioned 3-partition Kafka topics
TOPIC_RETAIL = 'bfsi-retail-transactions'                # Target topic for low-risk standard merchant transactions (RETAIL, GROCERY, ELECTRONICS)
TOPIC_HIGHRISK = 'bfsi-highrisk-transactions'            # Target topic for high-risk/regulated merchant categories (CRYPTO, GAMBLING)
TOPIC_TRAVEL = 'bfsi-travel-transactions'                # Target topic for travel and cross-border payment activity (TRAVEL)


def create_kafka_producer() -> KafkaProducer:
    """
    Initializes and returns a configured KafkaProducer instance with enterprise delivery guarantees.
    """
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,             # Connects to local Kafka broker on external port 9094
        key_serializer=lambda k: str(k).encode('utf-8'), # Encodes string partition key (account_id) into UTF-8 byte stream
        value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Converts payload dictionary into JSON string and encodes to UTF-8 bytes
        
        # --- ENTERPRISE RESILIENCY & THROUGHPUT CONFIGURATIONS ---
        acks='all',                                      # Requires full leader and in-sync replica write acknowledgment for maximum data durability
        retries=3,                                       # Automatically retries failed record transmissions up to 3 times upon transient drops
        batch_size=16384,                                # Accumulates outgoing records into batches up to 16KB before sending to broker
        linger_ms=10,                                    # Introduces a 10ms micro-wait to allow record batch accumulation, balancing throughput and latency
        max_in_flight_requests_per_connection=1          # Guarantees strict in-flight message ordering per partition when retries are enabled
    )


def generate_transaction() -> Tuple[str, Dict[str, Any]]:
    """
    Generates a synthetic transaction payload and determines the target Kafka domain topic.
    """
    merchant_category = random.choice([                  # Randomly picks a merchant domain for synthetic payload generation
        "RETAIL", "TRAVEL", "GAMBLING", "CRYPTO", "GROCERY", "ELECTRONICS"
    ])
    amount = round(random.uniform(5.0, 15000.0), 2)      # Generates floating point transaction amount between $5.00 and $15,000.00
    
    # Business risk rule: Flag transactions over $10,000 USD or in high-risk merchant categories
    is_flagged = amount > 10000.0 or merchant_category in ["GAMBLING", "CRYPTO"]

    payload = {                                          # Constructs structured JSON schema payload representing a banking event
        "transaction_id": f"txn_{fake.uuid4()}",         # Generates a unique transaction record identifier
        "account_id": f"ACC-{fake.random_int(min=10000, max=99999)}", # Generates customer account ID used as partition key
        "amount": amount,                                # Numeric transaction monetary value
        "currency": "USD",                               # Transaction ISO currency code
        "merchant_category": merchant_category,          # Business sector classification
        "merchant_name": fake.company(),                 # Synthetic merchant company name
        "location": f"{fake.city()}, {fake.country()}",  # City and country location string
        "timestamp": fake.iso8601(),                     # Event timestamp formatted in ISO 8601 standard
        "is_flagged": is_flagged                         # Boolean risk indicator flag
    }

    # Domain-driven topic routing logic based on merchant category
    if merchant_category in ["CRYPTO", "GAMBLING"]:      # Route high-risk categories to highrisk topic
        target_topic = TOPIC_HIGHRISK
    elif merchant_category == "TRAVEL":                  # Route travel categories to travel topic
        target_topic = TOPIC_TRAVEL
    else:                                                # Route all remaining standard categories to retail topic
        target_topic = TOPIC_RETAIL

    return target_topic, payload                         # Returns target topic name along with payload dictionary


def main():
    producer = create_kafka_producer()                   # Instantiates the Kafka producer client connection
    print(f"[*] Connected to Kafka broker at {BOOTSTRAP_SERVERS}") # Log statement displaying target broker host address
    print("[*] Domain-based multi-topic routing active:")# Log statement summarizing active topic routing rules
    print(f"    - High Risk : {TOPIC_HIGHRISK}")
    print(f"    - Travel    : {TOPIC_TRAVEL}")
    print(f"    - Retail    : {TOPIC_RETAIL}\n")

    try:
        while True:                                      # Continuous event streaming loop
            target_topic, txn = generate_transaction()   # Obtains next synthetic transaction and corresponding target topic
            account_key = txn["account_id"]              # Extracts account_id to serve as the Kafka record partition key

            # Sends record to Kafka topic; keyed message routing guarantees strict event ordering per account_id
            future = producer.send(
                topic=target_topic,                      # Target domain topic name
                key=account_key,                         # Partition key (hashes account_id to determine partition index)
                value=txn                                # Transaction event dictionary payload
            )
            
            record_metadata = future.get(timeout=10)     # Blocks synchronously for up to 10 seconds waiting for broker acknowledgment metadata
            print(                                       # Logs sent transaction details including partition index and assigned topic offset
                f"[Sent ➔ {target_topic:<26}] TxnID: {txn['transaction_id'][:12]}... | "
                f"Account: {account_key} | "
                f"Cat: {txn['merchant_category']:<11} | "
                f"Partition: {record_metadata.partition} | "
                f"Offset: {record_metadata.offset}"
            )
            
            time.sleep(0.5)                              # Pauses execution for 500ms to throttle event rate to 2 events per second

    except KeyboardInterrupt:                             # Gracefully handles manual shutdown signal (Ctrl+C)
        print("\n[*] Stopping producer stream...")
    except KafkaError as e:                              # Catches network, authorization, or serialization exceptions from Kafka client
        print(f"[!] Kafka Error encountered: {e}")
    finally:
        producer.flush()                                 # Forces any buffered records in memory to be transmitted to the broker before exit
        producer.close()                                 # Closes network connections and cleans up background producer threads
        print("[*] Producer closed successfully.")


if __name__ == "__main__":
    main()


"""
================================================================================
ENTERPRISE PRODUCER BOTTLENECK ANALYSIS, RESOLUTIONS & INTERVIEW QA
================================================================================

--------------------------------------------------------------------------------
PART 1: KAFKA PRODUCER ARCHITECTURAL BOTTLENECKS & MITIGATIONS
--------------------------------------------------------------------------------

1. PRODUCER BOTTLENECK SCENARIOS & RESOLUTIONS:
--------------------------------------------------------------------------------
Issue / Symptom                   | Root Cause                         | Resolution Technique
----------------------------------|------------------------------------|------------------------------------------------
A. High Network Overhead &        | Sending messages synchronously     | 1. Enable batching via `batch.size` (e.g., 32KB/64KB).
   CPU Thrashing                  | one-by-one (`future.get()` on     | 2. Set `linger.ms` (e.g., 10-50ms) to allow batch accumulation.
                                  | every message).                    | 3. Use async callbacks (`producer.send(..., callback=on_send)`)
                                  |                                    |    instead of synchronous blocking `.get()`.
                                  |                                    | 4. Enable compression (`compression.type='snappy'`).
----------------------------------|------------------------------------|------------------------------------------------
B. Out-of-Order Delivery During   | `retries > 0` with multiple        | Set `max.in.flight.requests.per.connection=1`.
   Network Retries                | in-flight requests allows request  | This ensures request #2 is not processed before
                                  | #2 to succeed while #1 retries.    | request #1 finishes retrying, preserving order.
----------------------------------|------------------------------------|------------------------------------------------
C. Buffer Exhaustion &            | Broker disk I/O saturation or      | 1. Increase `buffer.memory` (e.g., from 32MB to 64MB/128MB).
   `TimeoutException`             | network partition causes producer  | 2. Tune `max.block.ms` to fail-fast or trigger circuit breakers.
                                  | buffer memory to fill up.          | 3. Scale out broker storage and increase topic partitions.
----------------------------------|------------------------------------|------------------------------------------------
D. Partition Skew at Ingress      | Monolithic partition key values    | 1. Implement key salting (`account_id + "_" + random(1,5)`)
   (Hot Partitions)               | (e.g., single corporate entity     |    for ultra-high volume accounts.
                                  | generating 80% of transactions).   | 2. Write a custom `Partitioner` class to dynamically
                                  |                                    |    balance hot key payloads across partitions.


--------------------------------------------------------------------------------
PART 2: TOP KAFKA PRODUCER INTERVIEW QUESTIONS (BFSI DOMAIN)
--------------------------------------------------------------------------------

Q1: How do you guarantee strict message ordering per customer account in Apache Kafka?
A1: Message ordering is guaranteed within a single partition. By passing `account_id` as the message key,
    Kafka's default partitioner hashes the key (`hash(account_id) % num_partitions`) to ensure all events
    for that account land on the exact same partition. Additionally, setting `max.in.flight.requests.per.connection=1`
    prevents out-of-order writes during network retry scenarios.

Q2: What is the difference between `acks=0`, `acks=1`, and `acks=all` (`acks=-1`)?
A2: - `acks=0`: Fire-and-forget. Producer doesn't wait for acknowledgment. Highest throughput, highest risk of data loss.
    - `acks=1`: Waits for leader broker acknowledgment only. Balanced throughput, but data loss can occur if leader crashes before replica sync.
    - `acks=all`: Waits for leader AND all In-Sync Replicas (ISR) to acknowledge receipt. Maximum durability, non-negotiable for financial transactions.

Q3: How do `batch.size` and `linger.ms` interact in a high-throughput enterprise producer?
A3: They control record batching mechanics:
    - `batch.size` sets the maximum byte threshold (e.g., 16KB) for a single batch sent to a partition.
    - `linger.ms` sets the maximum time delay (e.g., 10ms) to wait for more messages before sending an incomplete batch.
    The producer transmits whichever threshold is reached FIRST.

Q4: What happens if a Kafka producer cannot reach the broker cluster? How do you prevent memory leaks?
A4: Outgoing records accumulate in `buffer.memory` (default 32MB). If the broker remains unreachable beyond `max.block.ms`
    (default 60 seconds), `producer.send()` raises a `BufferExhaustedException` or `TimeoutException`.
    In production, we wrap sends in try-catch blocks, implement exponential backoff retries, route failed records
    to a local Dead Letter Queue (DLQ) file, and alert monitoring systems via CloudWatch/Datadog.

Q5: Why is domain-driven topic routing (e.g., splitting into Retail, Travel, and High-Risk topics) preferred over a single mega-topic?
A5: Multi-topic routing isolates business blast radiuses, allows independent retention policies (e.g., keep high-risk transactions
    longer for compliance auditing), enables custom SLA scaling for downstream consumers (e.g., dedicated real-time fraud engines on High-Risk),
    and reduces consumer-side filtering overhead.
================================================================================
"""