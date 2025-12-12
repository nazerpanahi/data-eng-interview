#!/bin/bash

set -e

echo "========================================"
echo "Deploying Monitoring Tools"
echo "========================================"

# Start Kafka UI and ClickHouse
echo "Starting monitoring tools..."
docker compose up -d kafka-ui clickhouse

echo "Waiting for monitoring tools to be ready..."
sleep 15

# Check Kafka UI
if ! curl --output /dev/null --silent --head --fail http://localhost:8080 > /dev/null 2>&1; then
    echo "Kafka UI failed to start properly"
    exit 1
fi

# Check ClickHouse
if ! docker compose exec clickhouse clickhouse-client --query "SELECT 1" > /dev/null 2>&1; then
    echo "ClickHouse failed to start properly"
    exit 1
fi

echo "✓ Monitoring tools are ready"
echo ""
echo "Monitoring URLs:"
echo "  Kafka UI:        http://localhost:8080"
echo "  Spark Master UI: http://localhost:8081"
echo "  Spark Worker UI: http://localhost:8082"
echo "  Kafka Connect:   http://localhost:8083"
echo "  ClickHouse HTTP: http://localhost:8123"