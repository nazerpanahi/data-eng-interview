#!/bin/bash

set -e

echo "========================================"
echo "Setting up Python Virtual Environment"
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

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Step 1: Create virtual environment
print_step "Step 1/3: Creating virtual environment"
if [ -d "venv" ]; then
    print_info "Virtual environment already exists"
else
    echo "Creating Python virtual environment..."
    python3 -m venv venv
    print_success "Virtual environment created"
fi

# Step 2: Activate virtual environment
print_step "Step 2/3: Activating virtual environment"
source venv/bin/activate
print_success "Virtual environment activated"

# Step 3: Install requirements
print_step "Step 3/3: Installing Python packages"

if [ -f "requirements.txt" ]; then
    echo "Installing packages from requirements.txt..."
    pip install -r requirements.txt
    print_success "All packages installed successfully"
else
    print_error "requirements.txt not found"
    exit 1
fi

# Verify installation
echo ""
echo "Verifying package installation..."
python -c "
try:
    import pandas
    import sqlalchemy
    import pymysql
    import kafka
    import numpy
    print('✓ All required packages are available')
except ImportError as e:
    print(f'✗ Missing package: {e}')
    exit(1)
"

print_success "Package verification completed"

echo ""
echo "========================================"
echo "✓ Virtual environment setup completed!"
echo "========================================"

echo ""
print_info "Virtual environment is ready for use!"
print_info "To activate manually: source venv/bin/activate"
print_info "To deactivate: deactivate"