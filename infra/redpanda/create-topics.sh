#!/usr/bin/env bash
set -euo pipefail

# Create topics (ignore if exists)
rpk topic create record.ingested.v1 -p 1 -r 1 || true
rpk topic create record.ingested.v2 -p 1 -r 1 || true
rpk topic create record.dlq.v1 -p 1 -r 1 || true

echo "✅ Topics ready"
