#!/bin/bash

set -e

echo "========================================"
echo "Deploying ClickHouse Analytics Database"
echo "========================================"

# Start ClickHouse
echo "Starting ClickHouse database..."
docker compose up -d clickhouse

echo "Waiting for ClickHouse to be ready..."
sleep 20

# Check if ClickHouse is healthy
if ! docker compose exec clickhouse clickhouse-client --query "SELECT 1" > /dev/null 2>&1; then
    echo "ClickHouse failed to start properly"
    exit 1
fi

echo "✓ ClickHouse is healthy and ready"

# Initialize ClickHouse schema
echo "Initializing ClickHouse database schema..."
docker compose exec -T clickhouse clickhouse-client --queries-file /dev/stdin < init-scripts/clickhouse-init.sql

if [ $? -eq 0 ]; then
    echo "✓ ClickHouse schema initialized successfully"
else
    echo "✗ Failed to initialize ClickHouse schema"
    exit 1
fi

# Verify tables were created
echo "Verifying ClickHouse tables..."
docker compose exec clickhouse clickhouse-client --query "
SELECT 
    table,
    engine,
    total_rows,
    formatReadableSize(total_bytes) as size
FROM system.tables 
WHERE database = 'insurance_analytics'
ORDER BY table"

# Check database size and health
echo ""
echo "ClickHouse Database Health Check:"
docker compose exec clickhouse clickhouse-client --query "
SELECT 
    'Database Health' as metric,
    count() as table_count,
    formatReadableSize(sum(total_bytes)) as total_size,
    countIf(engine = 'ReplacingMergeTree') as rm_tables,
    countIf(engine = 'SummingMergeTree') as sm_tables,
    countIf(engine = 'Kafka') as kafka_tables
FROM system.tables 
WHERE database = 'insurance_analytics'"

echo ""
echo "✓ ClickHouse deployment completed successfully"
echo ""
echo "ClickHouse Details:"
echo "  Native Client:   docker compose exec clickhouse clickhouse-client"
echo "  HTTP Interface:  http://localhost:8123"
echo "  Database:        insurance_analytics"
echo "  Port:            9000 (native), 8123 (HTTP)"
echo ""
echo "Sample Queries:"
echo "  docker compose exec clickhouse clickhouse-client --query 'SELECT count(*) FROM insurance_analytics.processed_events_storage'"
echo "  curl http://localhost:8123 --data 'SELECT count(*) FROM insurance_analytics.processed_events_storage'"