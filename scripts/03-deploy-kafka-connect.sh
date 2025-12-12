#!/bin/bash

set -e

echo "========================================"
echo "Deploying Kafka Connect with MySQL Support"
echo "========================================"

# Build and start Kafka Connect
echo "Building and starting Kafka Connect..."
docker compose up -d --build kafka-connect

echo "Waiting for Kafka Connect to be ready..."
MAX_RETRIES=60
RETRY_COUNT=0

until $(curl --output /dev/null --silent --head --fail http://localhost:8083/connectors);
do
  if [ ${RETRY_COUNT} -eq ${MAX_RETRIES} ];then
    echo "Kafka Connect did not become ready in time. Exiting."
    exit 1
  fi
  printf '.'
  RETRY_COUNT=$((RETRY_COUNT+1))
  sleep 5
done

echo "\n✓ Kafka Connect is healthy and ready"

# Deploy MySQL JDBC Source connector
echo "Deploying MySQL JDBC Source connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-source-connector",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
      "tasks.max": "1",
      "connection.url": "jdbc:mysql://mysql:3306/insurance_db",
      "connection.user": "insurance_user",
      "connection.password": "insurance_pass",
      "table.whitelist": "users",
      "mode": "incrementing",
      "incrementing.column.name": "user_id",
      "topic.prefix": "insurance.mysql.",
      "poll.interval.ms": 5000,
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "key.converter.schemas.enable": "false",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
    }
  }'

echo "✓ MySQL CDC connector deployed"
echo ""
echo "Kafka Connect Details:"
echo "  REST API: http://localhost:8083"
echo "  Available connectors: JDBC, ClickHouse, etc."