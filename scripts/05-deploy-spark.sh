#!/bin/bash

set -e

echo "========================================"
echo "Deploying Spark Cluster"
echo "========================================"

# Start Spark Master and Worker
echo "Starting Spark Master and Worker..."
docker compose up -d spark-master spark-worker

echo "Waiting for Spark services to be ready..."
sleep 20

# Check Spark Master
if ! curl --output /dev/null --silent --head --fail http://localhost:8081 > /dev/null 2>&1; then
    echo "Spark Master failed to start properly"
    exit 1
fi

echo "✓ Spark Master is ready"

# # Copy ETL files to Spark container
# echo "Copying ETL files to Spark container..."
# docker cp etl/. insurance-spark-master:/opt/spark-apps/

echo "✓ Spark cluster deployed successfully"
echo ""
echo "Spark Details:"
echo "  Master UI: http://localhost:8081"
echo "  Worker UI: http://localhost:8082"
echo "  Master Port: 7077"