import argparse
import json
import logging
import os
import sys
import uuid
from typing import Dict, List, Optional, Sequence, Set
from urllib.parse import urlparse

import psycopg
from minio import Minio

SAMPLE_LIMIT = 10


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


def prepare_record_ids(keys: Sequence[str], source: str, limit: Optional[int], logger: logging.Logger) -> List[str]:
    prepared: List[str] = []
    skipped = 0
    for key in keys:
        record_id = parse_record_id(key, source)
        if not record_id:
            skipped += 1
            continue
        prepared.append(record_id)
        if limit is not None and len(prepared) >= limit:
            break
    if skipped:
        logger.info("skipped_keys count=%s", skipped)
    return prepared


def fetch_manifest_state(conn: psycopg.Connection, record_id: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT state FROM raw_object_manifest WHERE record_id = %s",
            (record_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def canonical_exists(conn: psycopg.Connection, record_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM canonical_records WHERE record_id::text = %s",
            (record_id,),
        )
        return cur.fetchone() is not None


def fetch_canonical_missing_manifest(conn: psycopg.Connection, source: str) -> Set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.record_id::text
            FROM canonical_records c
            LEFT JOIN raw_object_manifest m ON m.record_id = c.record_id::text
            WHERE m.record_id IS NULL AND c.source = %s
            """,
            (source,),
        )
        rows = cur.fetchall()
    return {row[0] for row in rows}


def reconcile(args: argparse.Namespace) -> int:
    logger = logging.getLogger("reconciler")

    minio_endpoint = get_env("MINIO_ENDPOINT")
    minio_access_key = get_env("MINIO_ACCESS_KEY")
    minio_secret_key = get_env("MINIO_SECRET_KEY")
    bucket = get_bucket_env()
    postgres_dsn = get_env("POSTGRES_DSN")

    client = build_minio_client(minio_endpoint, minio_access_key, minio_secret_key)
    prefix = f"{args.source}/"
    keys = list_object_keys(client, bucket, prefix)
    raw_record_ids = prepare_record_ids(keys, args.source, args.limit, logger)

    categories: Dict[str, Set[str]] = {
        "orphan_raw": set(),
        "stuck_enqueued": set(),
        "written_no_enqueue": set(),
        "unexpected_state_no_canonical": set(),
        "canonical_missing_manifest": set(),
        "canonical_state_mismatch": set(),
        "ok": set(),
    }

    with psycopg.connect(postgres_dsn) as conn:
        for record_id in raw_record_ids:
            manifest_state = fetch_manifest_state(conn, record_id)
            has_manifest = manifest_state is not None
            has_canonical = canonical_exists(conn, record_id)

            if not has_manifest and has_canonical:
                categories["canonical_missing_manifest"].add(record_id)
            elif not has_manifest and not has_canonical:
                categories["orphan_raw"].add(record_id)
            else:
                if has_canonical:
                    if manifest_state == "canonicalized":
                        categories["ok"].add(record_id)
                    else:
                        categories["canonical_state_mismatch"].add(record_id)
                else:
                    if manifest_state == "written":
                        categories["written_no_enqueue"].add(record_id)
                    elif manifest_state == "enqueued":
                        categories["stuck_enqueued"].add(record_id)
                    else:
                        categories["unexpected_state_no_canonical"].add(record_id)

        canonical_missing_manifest = fetch_canonical_missing_manifest(conn, args.source)

    raw_set = set(raw_record_ids)
    for record_id in canonical_missing_manifest:
        if record_id not in raw_set and record_id not in categories["canonical_missing_manifest"]:
            categories["canonical_missing_manifest"].add(record_id)

    counts = {name: len(values) for name, values in categories.items()}
    samples = {
        name: sorted(values)[:SAMPLE_LIMIT]
        for name, values in categories.items()
        if values
    }

    if args.output == "json":
        payload = {
            "source": args.source,
            "raw_count": len(raw_record_ids),
            "counts": counts,
            "samples": samples,
        }
        print(json.dumps(payload, indent=2))
        return 0

    print(f"raw_count={len(raw_record_ids)}")
    print(f"ok={counts['ok']}")
    print(f"orphan_raw={counts['orphan_raw']}")
    print(f"stuck_enqueued={counts['stuck_enqueued']}")
    print(f"written_no_enqueue={counts['written_no_enqueue']}")
    print(f"unexpected_state_no_canonical={counts['unexpected_state_no_canonical']}")
    print(f"canonical_missing_manifest={counts['canonical_missing_manifest']}")
    print(f"canonical_state_mismatch={counts['canonical_state_mismatch']}")

    for name in (
        "orphan_raw",
        "stuck_enqueued",
        "written_no_enqueue",
        "unexpected_state_no_canonical",
        "canonical_missing_manifest",
        "canonical_state_mismatch",
        "ok",
    ):
        sample = samples.get(name)
        if not sample:
            continue
        print(f"{name}_sample=")
        for record_id in sample:
            print(record_id)

    return 0


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile raw objects, manifest, and canonical records")
    parser.add_argument("--source", required=True, help="Source name, e.g. demo-source")
    parser.add_argument("--limit", type=int, help="Max number of raw records to scan")
    parser.add_argument("--output", choices=("json", "text"), default="text", help="Output format")
    return parser.parse_args(argv)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s level=%(levelname)s msg=%(message)s",
    )

    args = parse_args(sys.argv[1:])
    return reconcile(args)


if __name__ == "__main__":
    sys.exit(main())
