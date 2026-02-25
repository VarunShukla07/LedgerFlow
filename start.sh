#!/bin/bash
set -e

echo "==============================================="
echo "🚀 Starting Fintech Analytics Pipeline"
echo "==============================================="

docker compose up -d

echo ""
echo "⏳ Waiting 10 seconds for services..."
# sleep 10 -> Instead of that, will wait
echo "⏳ Waiting for Airflow to become healthy..."
until curl -s http://localhost:8080/health | grep -q "healthy"; do
    sleep 5
    echo "  still waiting..."
done

echo "✅ Airflow is healthy"

echo ""
docker compose ps

echo ""
echo "🌐 Access:"
echo "  Airflow: http://localhost:8080 (admin/admin)"
echo ""
echo "📌 Next manual steps:"
echo "  1. Start Producer:"
echo "     cd data-generator && python3 producer.py --total 100"
echo ""
echo "  2. Start Streaming:"
echo "     cd spark-streaming && python3 stream_processor.py"
echo ""
echo "  3. Trigger Airflow DAG from UI"