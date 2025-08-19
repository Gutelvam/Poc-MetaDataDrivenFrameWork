#!/bin/bash

echo "🎯 Setting Airflow Variables (NOT Environment Variables)"
echo "======================================================="

echo ""
echo "ℹ️  These are stored in Airflow's database, not as environment variables"
echo ""

# Check if airflow webserver is running
# if ! docker compose ps airflow-webserver | grep -q "running"; then
#     echo "❌ Airflow webserver is not running. Starting it first..."
#     docker compose up -d airflow-webserver
#     echo "⏳ Waiting for webserver to be ready..."
#     sleep 30
# fi

echo "🔧 Setting required Airflow Variables..."

# Set the 3 essential variables that are causing errors
docker compose exec airflow-apiserver airflow variables set \
    prometheus_pushgateway_url "http://prometheus:9090"

docker compose exec airflow-apiserver airflow variables set \
    grafana_api_url "http://grafana:3000"

docker compose exec airflow-apiserver airflow variables set \
    grafana_api_key "dummy_key_for_testing"

echo ""
echo "✅ Variables set successfully!"
echo ""
echo "📋 Verifying variables are stored in Airflow:"
docker compose exec airflow-webserver airflow variables list

echo ""
echo "🎉 Done! These variables are now stored in Airflow's database."
echo ""
echo "💡 Alternative: You can also set these via the Web UI:"
echo "   1. Go to http://localhost:8080"
echo "   2. Login with admin/admin"
echo "   3. Navigate to Admin → Variables"
echo "   4. Add the variables there"
echo ""
echo "🔍 To verify the errors are gone:"
echo "   docker compose logs airflow-scheduler --tail=20 | grep Variable"