#!/bin/bash

set -e

echo "========================================"
echo "Waiting for Kafka to be ready..."
echo "========================================"

# Wait for Kafka to be fully ready
until kafka-broker-api-versions --bootstrap-server kafka:29092 > /dev/null 2>&1; do
  echo "Kafka not ready yet. Retrying in 5 seconds..."
  sleep 5
done

echo "Kafka is ready. Creating topics..."

# Create topics with proper configuration
kafka-topics --create \
  --bootstrap-server kafka:29092 \
  --topic insurance.raw_events \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config segment.ms=86400000 \
  --config compression.type=lz4 \
  --if-not-exists

kafka-topics --create \
  --bootstrap-server kafka:29092 \
  --topic insurance.processed_events \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000 \
  --config segment.ms=86400000 \
  --config compression.type=lz4 \
  --if-not-exists

kafka-topics --create \
  --bootstrap-server kafka:29092 \
  --topic insurance.mysql.users \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config segment.ms=86400000 \
  --config compression.type=lz4 \
  --config cleanup.policy=delete \
  --if-not-exists

kafka-topics --create \
  --bootstrap-server kafka:29092 \
  --topic _connect-configs \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --if-not-exists

kafka-topics --create \
  --bootstrap-server kafka:29092 \
  --topic _connect-offsets \
  --partitions 25 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --if-not-exists

kafka-topics --create \
  --bootstrap-server kafka:29092 \
  --topic _connect-status \
  --partitions 5 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --if-not-exists

echo "========================================"
echo "All Kafka topics created successfully!"
echo "========================================"

# List all topics
kafka-topics --list --bootstrap-server kafka:29092
