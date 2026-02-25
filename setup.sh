#!/bin/bash
set -e

echo "==============================================="
echo "🚀 Fintech Analytics - Setup"
echo "==============================================="

# Check Docker
if ! command -v docker &> /dev/null; then
  echo "❌ Docker not installed"
  exit 1
fi

# Check Docker Compose (v2)
if ! docker compose version &> /dev/null; then
  echo "❌ Docker Compose v2 not installed"
  exit 1
fi

echo "✅ Docker & Compose detected"

echo "🔨 Building custom images..."
docker compose build --no-cache

echo "📦 Pulling required images..."
docker compose pull

echo "✅ Setup complete."
echo ""
echo "Next:"
echo "  ./start.sh"