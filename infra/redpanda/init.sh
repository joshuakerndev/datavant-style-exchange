#!/usr/bin/env sh
set -e

echo "redpanda-init starting"
rpk topic create record.ingested.v1 -p 1 -r 1 --brokers redpanda:9092 || true
rpk topic create record.ingested.v2 -p 1 -r 1 --brokers redpanda:9092 || true
rpk topic create record.ingested.v1.dlq -p 1 -r 1 --brokers redpanda:9092 || true
rpk topic create record.ingested.v2.dlq -p 1 -r 1 --brokers redpanda:9092 || true
echo "topics ready"
