#!/bin/bash

echo "==============================================="
echo "🛑 Stopping Fintech Analytics"
echo "==============================================="

docker compose stop

echo ""
docker compose ps

echo ""
read -p "Remove containers? (y/N): " choice
if [[ "$choice" =~ ^[Yy]$ ]]; then
    docker compose down
    echo "✅ Containers removed"
else
    echo "✅ Containers stopped (data preserved)"
fi