import argparse
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import psycopg
from confluent_kafka import Producer
from minio import Minio

DEFAULT_TOPIC = "record.ingested.v1"
MISSING_LIMIT = 20
STREAM_CHUNK_BYTES = 32 * 1024


def get_env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def get_bucket_env() -> str:
    return get_env("RAW_BUCKET", os.getenv("MINIO_BUCKET"))


def build_minio_client(endpoint: str, access_key: str, secret_key: str) -> Minio:
    parsed = urlparse(endpoint)
    return Minio(
        parsed.netloc or parsed.path,
        access_key=access_key,
        secret_key=secret_key,
        secure=parsed.scheme == "https",
    )


def list_object_keys(client: Minio, bucket: str, prefix: str) -> List[str]:
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    keys = [obj.object_name for obj in objects]
    keys.sort()
    return keys


def parse_record_id(key: str, source: str) -> Optional[str]:
    expected_prefix = f"{source}/"
    if not key.startswith(expected_prefix):
        return None
    suffix = key[len(expected_prefix) :]
    if not suffix.endswith(".json"):
        return None
    record_id = suffix[:-5]
    try:
        uuid.UUID(record_id)
    except ValueError:
        return None
    return record_id


def sha256_and_size(client: Minio, bucket: str, key: str) -> Tuple[str, int]:
    stat = client.stat_object(bucket, key)
    response = client.get_object(bucket, key)
    hasher = hashlib.sha256()
    try:
        for chunk in response.stream(STREAM_CHUNK_BYTES):
            if chunk:
                hasher.update(chunk)
    finally:
        response.close()
        response.release_conn()
    return hasher.hexdigest(), int(stat.size)


def correlation_id_for(record_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, record_id))


def build_event(
    record_id: str,
    source: str,
    bucket: str,
    key: str,
    sha256_hex: str,
    size_bytes: int,
) -> dict:
    return {
        "event_version": "1",
        "event_type": "record.ingested",
        "event_id": str(uuid.uuid4()),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "correlation_id": correlation_id_for(record_id),
        "source": source,
        "record_id": record_id,
        "raw_object": {
            "bucket": bucket,
            "key": key,
            "sha256": sha256_hex,
            "size_bytes": size_bytes,
        },
    }


def send_event(producer: Producer, topic: str, key: str, event: dict) -> None:
    delivered = False
    delivery_error: Optional[Exception] = None

    def _on_delivery(err, _msg) -> None:
        nonlocal delivered, delivery_error
        delivered = True
        if err is not None:
            delivery_error = RuntimeError(str(err))

    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        on_delivery=_on_delivery,
    )
    producer.flush(10)
    if not delivered:
        raise RuntimeError("kafka delivery timeout")
    if delivery_error is not None:
        raise delivery_error


def rate_limit(next_emit_time: Optional[float]) -> Optional[float]:
    if next_emit_time is None:
        return None
    now = time.monotonic()
    sleep_for = next_emit_time - now
    if sleep_for > 0:
        time.sleep(sleep_for)
    return time.monotonic()


def prepare_keys(keys: Sequence[str], source: str, limit: Optional[int], logger: logging.Logger) -> List[Tuple[str, str]]:
    prepared: List[Tuple[str, str]] = []
    skipped = 0
    for key in keys:
        record_id = parse_record_id(key, source)
        if not record_id:
            skipped += 1
            continue
        prepared.append((record_id, key))
        if limit is not None and len(prepared) >= limit:
            break
    if skipped:
        logger.info("skipped_keys count=%s", skipped)
    return prepared


def replay(args: argparse.Namespace) -> int:
    logger = logging.getLogger("replayer")

    minio_endpoint = get_env("MINIO_ENDPOINT")
    minio_access_key = get_env("MINIO_ACCESS_KEY")
    minio_secret_key = get_env("MINIO_SECRET_KEY")
    bucket = get_bucket_env()
    kafka_brokers = get_env("KAFKA_BROKERS")
    topic = os.getenv("TOPIC_RECORD_INGESTED_V1", DEFAULT_TOPIC)

    client = build_minio_client(minio_endpoint, minio_access_key, minio_secret_key)
    producer = Producer({"bootstrap.servers": kafka_brokers})

    prefix = args.prefix or f"{args.source}/"
    keys = list_object_keys(client, bucket, prefix)
    to_replay = prepare_keys(keys, args.source, args.limit, logger)

    if not to_replay:
        logger.info("no_records_found prefix=%s", prefix)
        return 0

    rate_interval: Optional[float] = None
    if args.rate_per_sec is not None:
        if args.rate_per_sec <= 0:
            raise ValueError("rate-per-sec must be > 0")
        rate_interval = 1.0 / float(args.rate_per_sec)

    next_emit_time: Optional[float] = None
    emitted = 0

    for record_id, key in to_replay:
        if rate_interval is not None:
            if next_emit_time is None:
                next_emit_time = time.monotonic()
            next_emit_time = rate_limit(next_emit_time)

        sha256_hex, size_bytes = sha256_and_size(client, bucket, key)
        event = build_event(record_id, args.source, bucket, key, sha256_hex, size_bytes)

        if args.dry_run:
            logger.info(
                "dry_run_emit record_id=%s key=%s size_bytes=%s",
                record_id,
                key,
                size_bytes,
            )
        else:
            send_event(producer, topic, record_id, event)
            logger.info("emitted record_id=%s", record_id)

        emitted += 1
        if rate_interval is not None:
            next_emit_time = (next_emit_time or time.monotonic()) + rate_interval

    logger.info("replay_complete total=%s emitted=%s", len(to_replay), emitted)
    return 0


def fetch_canonical_record_ids(conn: psycopg.Connection, source: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT record_id::text FROM canonical_records WHERE source = %s",
            (source,),
        )
        rows = cur.fetchall()
    return [row[0] for row in rows]


def verify(args: argparse.Namespace) -> int:
    logger = logging.getLogger("replayer")

    minio_endpoint = get_env("MINIO_ENDPOINT")
    minio_access_key = get_env("MINIO_ACCESS_KEY")
    minio_secret_key = get_env("MINIO_SECRET_KEY")
    bucket = get_bucket_env()
    postgres_dsn = get_env("POSTGRES_DSN")

    client = build_minio_client(minio_endpoint, minio_access_key, minio_secret_key)
    prefix = args.prefix or f"{args.source}/"
    keys = list_object_keys(client, bucket, prefix)
    raw_pairs = prepare_keys(keys, args.source, None, logger)
    raw_record_ids = [record_id for record_id, _ in raw_pairs]

    with psycopg.connect(postgres_dsn) as conn:
        canonical_ids = fetch_canonical_record_ids(conn, args.source)

    raw_set = set(raw_record_ids)
    canonical_set = set(canonical_ids)
    missing = sorted(raw_set - canonical_set)

    raw_count = len(raw_record_ids)
    canonical_count = len(canonical_ids)
    duplicates_raw = raw_count - len(raw_set)
    duplicates_canonical = canonical_count - len(canonical_set)

    print(f"raw_count={raw_count}")
    print(f"canonical_count={canonical_count}")
    print(f"missing_count={len(missing)}")
    if duplicates_raw:
        print(f"raw_duplicates={duplicates_raw}")
    if duplicates_canonical:
        print(f"canonical_duplicates={duplicates_canonical}")

    if missing:
        print("missing_record_ids=")
        for record_id in missing[:MISSING_LIMIT]:
            print(record_id)
        if len(missing) > MISSING_LIMIT:
            print(f"... ({len(missing) - MISSING_LIMIT} more)")

    return 0


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay raw objects into record.ingested.v1")
    parser.add_argument("--source", required=True, help="Source name, e.g. demo-source")
    parser.add_argument("--prefix", help="MinIO prefix (defaults to <source>/)")
    parser.add_argument("--limit", type=int, help="Max number of records to replay")
    parser.add_argument("--dry-run", action="store_true", help="Do not emit Kafka events")
    parser.add_argument("--rate-per-sec", type=float, help="Emit rate limit (events/sec)")
    parser.add_argument("--verify", action="store_true", help="Verify canonical completeness")
    return parser.parse_args(argv)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s level=%(levelname)s msg=%(message)s",
    )

    args = parse_args(sys.argv[1:])

    if args.verify:
        return verify(args)
    return replay(args)


if __name__ == "__main__":
    sys.exit(main())
