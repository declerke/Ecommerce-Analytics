#!/bin/bash
set -e

export SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

echo "Running db upgrade..."
superset db upgrade

echo "Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password admin 2>/dev/null || true

echo "Initialising roles and permissions..."
superset init

echo "Starting Superset on :8088..."
exec superset run -p 8088 --host 0.0.0.0 --with-threads
