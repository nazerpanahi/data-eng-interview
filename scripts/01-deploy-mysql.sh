#!/bin/bash

set -e

echo "========================================"
echo "Deploying MySQL Database"
echo "========================================"

# Start MySQL
echo "Starting MySQL..."
docker compose up -d mysql

echo "Waiting for MySQL to be healthy..."
sleep 15

# Check if MySQL is healthy
if ! docker compose exec mysql mysqladmin ping -h localhost --silent; then
    echo "MySQL failed to start properly"
    exit 1
fi

echo "✓ MySQL is healthy and ready"
echo ""
echo "MySQL Details:"
echo "  Host: localhost:3306"
echo "  Database: insurance_db"
echo "  User: insurance_user"
echo "  Port: 3306"