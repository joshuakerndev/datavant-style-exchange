#!/usr/bin/env sh
set -e

echo "minio-init starting"
mc alias set local http://minio:9000 minioadmin minioadmin || true
mc mb -p local/raw-objects || true
echo "minio bucket ready"
exit 0
