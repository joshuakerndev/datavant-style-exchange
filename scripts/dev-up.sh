#!/usr/bin/env bash
set -euo pipefail
docker compose -f infra/docker-compose.yml up -d
echo "✅ Infra up"
echo "Postgres:   localhost:5432 (exchange/exchange)"
echo "MinIO:      http://localhost:9001 (minioadmin/minioadmin)"
echo "Redpanda:   localhost:9092"
echo "Prometheus: http://localhost:9090"
echo "Grafana:    http://localhost:3000"
