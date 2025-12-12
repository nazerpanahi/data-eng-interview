#!/bin/bash

set -e

echo "========================================"
echo "Initializing Data (MySQL & Kafka)"
echo "========================================"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_step() {
    echo -e "${GREEN}=== $1 ===${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Step 1: Create users table in MySQL
print_step "Step 1/3: Creating users table in MySQL"
docker compose exec -T mysql mysql -u insurance_user -pinsurance_pass insurance_db -e "
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    signup_date DATE NOT NULL,
    city VARCHAR(50) NOT NULL,
    device_type ENUM('Desktop', 'Mobile', 'Tablet') NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
" && print_success "Users table created successfully" || print_error "Failed to create users table"

# Step 2: Run combined data manager
print_step "Step 2/3: Running combined data manager"

# Activate virtual environment
if [ ! -d "venv" ]; then
    print_error "Virtual environment not found. Run: bash scripts/00-setup-venv.sh"
    exit 1
fi

source venv/bin/activate

# Run the data manager
if python scripts/init_data.py; then
    print_success "Data manager completed successfully"
else
    print_error "Data manager failed"
    exit 1
fi

# Step 3: Verify results
print_step "Step 3/3: Verifying results"

# Verify MySQL data
MYSQL_COUNT=$(docker compose exec -T mysql mysql -u insurance_user -pinsurance_pass insurance_db -e "SELECT COUNT(*)" -s -N)
print_success "MySQL users loaded: $MYSQL_COUNT"

# Verify Kafka events
sleep 2  # Give Kafka a moment to process
KAFKA_SAMPLES=$(docker compose exec -T kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic insurance.raw_events \
    --from-beginning \
    --max-messages 1 \
    --property print.key=false \
    --property print.timestamp=false 2>/dev/null | wc -l)

if [ "$KAFKA_SAMPLES" -gt 0 ]; then
    print_success "Kafka events loaded successfully"
    echo "Sample message:"
    docker compose exec -T kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic insurance.raw_events \
        --from-beginning \
        --max-messages 1 \
        --property print.key=false \
        --property print.timestamp=false 2>/dev/null || true
else
    print_error "No events found in Kafka"
fi

echo ""
echo "========================================"
echo "✓ Data initialization completed successfully!"
echo "========================================"
