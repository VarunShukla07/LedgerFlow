#!/bin/bash

echo "==============================================="
echo "🧪 Testing Pipeline Health"
echo "==============================================="

errors=0

# Check containers running
if docker compose ps | grep -q "Up"; then
  echo "✅ Containers running"
else
  echo "❌ Containers not running"
  ((errors++))
fi

# Test Postgres
if docker exec postgres pg_isready -U airflow &> /dev/null; then
  echo "✅ PostgreSQL reachable"
else
  echo "❌ PostgreSQL not reachable"
  ((errors++))
fi

# Test Kafka
if docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 &> /dev/null; then
  echo "✅ Kafka reachable"
else
  echo "❌ Kafka not reachable"
  ((errors++))
fi

# Test Airflow
if curl -s http://localhost:8080/health | grep -q '"status": "healthy"'; then
  echo "✅ Airflow healthy"
else
  echo "❌ Airflow not responding"
  ((errors++))
fi

# Check data presence (optional)
count=$(docker exec postgres psql -U airflow -d airflow -t -c "SELECT COUNT(*) FROM raw.raw_transactions;" 2>/dev/null | tr -d ' ')
if [ "$count" -gt 0 ] 2>/dev/null; then
  echo "✅ Data present in Postgres ($count rows)"
else
  echo "⚠️ No data in Postgres yet"
fi

echo ""
if [ $errors -eq 0 ]; then
  echo "✅ ALL CORE TESTS PASSED"
else
  echo "❌ $errors critical issue(s) detected"
fi