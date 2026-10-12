#!/bin/bash
# ==============================================================================
# BFSI Lakehouse - Kafka Domain Topic Provisioning Script
# Executes inside the 'kafka_kraft' container to initialize domain topics.
# ==============================================================================

# Internal broker address inside the container
BOOTSTRAP_SERVER="localhost:9092"

# Full executable path inside the official apache/kafka:3.8.0 container
KAFKA_BIN="//opt/kafka/bin/kafka-topics.sh"

# Array of target domain topics
TOPICS=(
  "bfsi-retail-transactions"
  "bfsi-highrisk-transactions"
  "bfsi-travel-transactions"
)

echo "=========================================================="
echo " Starting Kafka Domain Topic Initialization"
echo "=========================================================="

for TOPIC in "${TOPICS[@]}"; do
  echo "[*] Provisioning topic: '${TOPIC}'..."
  
  docker exec -it kafka_kraft $KAFKA_BIN \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --create \
    --if-not-exists \
    --topic $TOPIC \
    --partitions 3 \
    --replication-factor 1

  if [ $? -eq 0 ]; then
    echo "    ✔ Successfully created/verified '${TOPIC}' (3 Partitions)."
  else
    echo "    ✖ Failed to create topic '${TOPIC}'."
  fi
done

echo ""
echo "=========================================================="
echo " Active Topics Summary"
echo "=========================================================="
docker exec -it kafka_kraft $KAFKA_BIN --bootstrap-server $BOOTSTRAP_SERVER --list