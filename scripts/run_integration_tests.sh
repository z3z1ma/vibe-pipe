#!/bin/bash
# Script to run integration tests with Docker Compose

set -e

echo "Starting integration test environment..."

# Start Docker containers
docker-compose -f docker-compose.test.yml up -d

# Wait for databases to be ready
echo "Waiting for databases to be ready..."
sleep 10

# Set environment variables for tests
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5433
export POSTGRES_DATABASE=testdb
export POSTGRES_USER=testuser
export POSTGRES_PASSWORD=testpass

export MYSQL_HOST=localhost
export MYSQL_PORT=3307
export MYSQL_DATABASE=testdb
export MYSQL_USER=testuser
export MYSQL_PASSWORD=testpass

# Run integration tests
echo "Running integration tests..."
uv run pytest tests/connectors/test_postgres_integration.py -v -m integration
uv run pytest tests/connectors/test_mysql_integration.py -v -m integration

# Cleanup
echo "Stopping test environment..."
docker-compose -f docker-compose.test.yml down

echo "Integration tests complete!"
