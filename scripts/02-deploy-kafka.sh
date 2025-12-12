#!/bin/bash

set -e

echo "========================================"
echo "Deploying Kafka & Zookeeper"
echo "========================================"

# Start Zookeeper and Kafka
echo "Starting Zookeeper and Kafka..."
docker compose up -d zookeeper kafka

echo "Waiting for services to be healthy..."
sleep 30

# Check if services are healthy
if ! docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo "Kafka failed to start properly"
    exit 1
fi

echo "✓ Kafka is healthy and ready"

# Create Kafka topics
echo "Creating Kafka topics..."
docker compose exec kafka bash /tmp/create-kafka-topics.sh

echo "✓ Kafka topics created successfully"
echo ""
echo "Kafka Details:"
echo "  Bootstrap Server: localhost:9092"
echo "  UI: http://localhost:8080 (when started)"