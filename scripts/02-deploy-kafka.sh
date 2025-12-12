#!/bin/bash

set -e

echo "========================================"
echo "Deploying Kafka, Zookeeper & Kafka UI"
echo "========================================"

# Start Zookeeper and Kafka
echo "Starting Zookeeper and Kafka..."
docker compose up -d zookeeper kafka

echo "Waiting for Kafka to be healthy..."
sleep 30

# Check if Kafka is healthy
if ! docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo "Kafka failed to start properly"
    exit 1
fi

echo "✓ Kafka is healthy and ready"

# Create Kafka topics
echo "Creating Kafka topics..."
docker compose exec kafka bash /tmp/create-kafka-topics.sh

echo "✓ Kafka topics created successfully"

# Start Kafka UI
echo "Starting Kafka UI..."
docker compose up -d kafka-ui

echo "Waiting for Kafka UI to be ready..."
sleep 15

# Check if Kafka UI is healthy
if ! curl --output /dev/null --silent --head --fail http://localhost:8080 > /dev/null 2>&1; then
    echo "Kafka UI failed to start properly"
    exit 1
fi

echo "✓ Kafka UI is ready"

echo ""
echo "Kafka Stack Details:"
echo "  Bootstrap Server: localhost:9092"
echo "  Kafka UI:          http://localhost:8080"
echo "  Zookeeper:         localhost:2181"