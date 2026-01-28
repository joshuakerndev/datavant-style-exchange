import hashlib
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from threading import Event
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import psycopg
from confluent_kafka import Consumer, KafkaException, Producer
from minio import Minio
from psycopg.types.json import Json
from urllib3 import PoolManager, Timeout


class TokenizerNon200Error(RuntimeError):
    pass


class MissingPatientIdentifiersError(ValueError):
    pass


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def fetch_raw_object(client: Minio, bucket: str, key: str) -> bytes:
    """Fetch raw object bytes from storage."""
    response = client.get_object(bucket, key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def normalize(
    raw: bytes,
    http: PoolManager,
    tokenizer_addr: str,
    auth_header: Optional[str],
) -> Dict[str, Any]:
    """Normalize raw payload to canonical JSON."""
    raw_json = json.loads(raw.decode("utf-8"))
    patient = raw_json.get("patient") or {}
    given_name = patient.get("first_name")
    family_name = patient.get("last_name")
    dob = patient.get("dob")
    ssn = patient.get("ssn")

    if not given_name or not family_name or not dob:
        raise MissingPatientIdentifiersError("missing patient identifiers")

    payload: Dict[str, Any] = {
        "given_name": given_name,
        "family_name": family_name,
        "dob": dob,
    }
    if ssn:
        payload["ssn"] = ssn

    patient_token = call_tokenizer(http, tokenizer_addr, payload, auth_header)
    return {
        "patient_token": patient_token,
        "meta": {"normalized_at": datetime.now(timezone.utc).isoformat()},
    }


def call_tokenizer(
    http: PoolManager,
    tokenizer_addr: str,
    payload: Dict[str, Any],
    auth_header: Optional[str],
) -> str:
    headers = {"Content-Type": "application/json"}
    if auth_header:
        headers["Authorization"] = auth_header

    response = http.request(
        "POST",
        f"{tokenizer_addr.rstrip('/')}/tokenize",
        body=json.dumps(payload).encode("utf-8"),
        headers=headers,
    )
    if response.status != 200:
        raise TokenizerNon200Error("tokenizer non-200")
    body = json.loads(response.data.decode("utf-8"))
    token = body.get("patient_token")
    if not token:
        raise ValueError("missing patient_token in tokenizer response")
    return token


def write_canonical(conn: psycopg.Connection, record: Dict[str, Any]) -> str:
    """Write canonical record; return status: ok|dup."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO canonical_records (
                record_id,
                source,
                raw_object_key,
                raw_sha256,
                normalized,
                ingested_at,
                correlation_id
            )
            VALUES (
                %(record_id)s,
                %(source)s,
                %(raw_object_key)s,
                %(raw_sha256)s,
                %(normalized)s,
                %(ingested_at)s,
                %(correlation_id)s
            )
            ON CONFLICT (record_id) DO NOTHING
            """,
            {
                "record_id": record["record_id"],
                "source": record["source"],
                "raw_object_key": record["raw_object_key"],
                "raw_sha256": record["raw_sha256"],
                "normalized": Json(record["normalized"]),
                "ingested_at": record["ingested_at"],
                "correlation_id": record["correlation_id"],
            },
        )
        conn.commit()
        return "ok" if cur.rowcount == 1 else "dup"


def publish_dlq(producer: Producer, topic: str, payload: Dict[str, Any]) -> None:
    """Publish DLQ envelope."""
    delivered = Event()
    delivery_error: Optional[Exception] = None

    def _on_delivery(err, _msg) -> None:
        nonlocal delivery_error
        if err is not None:
            delivery_error = RuntimeError(str(err))
        delivered.set()

    producer.produce(
        topic,
        value=json.dumps(payload).encode("utf-8"),
        on_delivery=_on_delivery,
    )
    producer.flush(10)
    if not delivered.is_set():
        raise RuntimeError("dlq delivery timeout")
    if delivery_error is not None:
        raise delivery_error


def sha256_hex(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def safe_error_message(exc: Exception) -> str:
    if isinstance(exc, MissingPatientIdentifiersError):
        return "missing patient identifiers"
    if isinstance(exc, TokenizerNon200Error):
        return "tokenizer non-200"
    return "processing error"


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s level=%(levelname)s msg=%(message)s",
    )
    logger = logging.getLogger("normalizer-worker")

    kafka_brokers = get_env("KAFKA_BROKERS")
    kafka_topic = get_env("KAFKA_TOPIC")
    kafka_dlq_topic = get_env("KAFKA_DLQ_TOPIC")
    kafka_group_id = get_env("KAFKA_GROUP_ID")
    minio_endpoint = get_env("MINIO_ENDPOINT")
    minio_access_key = get_env("MINIO_ACCESS_KEY")
    minio_secret_key = get_env("MINIO_SECRET_KEY")
    minio_bucket = get_env("MINIO_BUCKET")
    postgres_dsn = get_env("POSTGRES_DSN")
    env = os.getenv("ENV", "local")
    tokenizer_addr = os.getenv("TOKENIZER_ADDR", "http://tokenizer:8081")
    tokenizer_auth_token = os.getenv("TOKENIZER_AUTH_TOKEN")

    max_attempts = int(get_env("MAX_ATTEMPTS", "5"))
    backoff_base_ms = int(get_env("BACKOFF_BASE_MS", "250"))

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_brokers,
            "group.id": kafka_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    producer = Producer({"bootstrap.servers": kafka_brokers})

    db_conn = psycopg.connect(postgres_dsn)

    parsed_endpoint = urlparse(minio_endpoint)
    http_client = PoolManager(retries=0)
    tokenizer_http = PoolManager(retries=0, timeout=Timeout(connect=2.0, read=5.0))
    minio_client = Minio(
        parsed_endpoint.netloc or parsed_endpoint.path,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=parsed_endpoint.scheme == "https",
        http_client=http_client,
    )
    if env == "local":
        tokenizer_auth = "Bearer dev"
    else:
        if not tokenizer_auth_token:
            raise RuntimeError("Missing required env var: TOKENIZER_AUTH_TOKEN")
        tokenizer_auth = f"Bearer {tokenizer_auth_token}"

    running = True

    def _handle_signal(_signum: int, _frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    consumer.subscribe([kafka_topic])

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            attempt = 0
            while True:
                attempt += 1
                event: Dict[str, Any] = {}
                record_id = None
                correlation_id = None
                stage = "parse_event"
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    record_id = event.get("record_id")
                    source = event.get("source")
                    correlation_id = event.get("correlation_id")
                    ingested_at = event.get("occurred_at")
                    raw_object = event.get("raw_object") or {}
                    raw_object_key = raw_object.get("key")
                    raw_sha256 = raw_object.get("sha256")
                    raw_bucket = raw_object.get("bucket")
                    raw_size_bytes = raw_object.get("size_bytes")

                    if (
                        event.get("event_version") != "1"
                        or event.get("event_type") != "record.ingested"
                        or not event.get("event_id")
                        or not ingested_at
                        or not correlation_id
                        or not source
                        or not record_id
                        or not raw_object_key
                        or not raw_sha256
                        or not raw_bucket
                        or raw_size_bytes is None
                    ):
                        raise ValueError("missing required event fields")

                    if not isinstance(raw_size_bytes, int) or raw_size_bytes < 1:
                        raise ValueError("invalid raw_object.size_bytes")

                    if raw_bucket != minio_bucket:
                        logger.warning(
                            "unexpected_bucket record_id=%s correlation_id=%s bucket=%s",
                            record_id,
                            correlation_id,
                            raw_bucket,
                        )
                    stage = "fetch_raw_object"
                    raw = fetch_raw_object(minio_client, raw_bucket, raw_object_key)
                    stage = "normalize"
                    normalized = normalize(raw, tokenizer_http, tokenizer_addr, tokenizer_auth)

                    stage = "write_canonical"
                    try:
                        status = write_canonical(
                            db_conn,
                            {
                                "record_id": record_id,
                                "source": source,
                                "raw_object_key": raw_object_key,
                                "raw_sha256": raw_sha256,
                                "normalized": normalized,
                                "ingested_at": ingested_at,
                                "correlation_id": correlation_id,
                            },
                        )
                    except psycopg.OperationalError:
                        db_conn = psycopg.connect(postgres_dsn)
                        status = write_canonical(
                            db_conn,
                            {
                                "record_id": record_id,
                                "source": source,
                                "raw_object_key": raw_object_key,
                                "raw_sha256": raw_sha256,
                                "normalized": normalized,
                                "ingested_at": ingested_at,
                                "correlation_id": correlation_id,
                            },
                        )

                    logger.info(
                        "processed record_id=%s correlation_id=%s status=%s stage=write_canonical",
                        record_id,
                        correlation_id,
                        status,
                    )
                    consumer.commit(message=msg)
                    break
                except Exception as exc:
                    try:
                        db_conn.rollback()
                    except Exception:
                        pass
                    err_type = type(exc).__name__
                    logger.error(
                        "error record_id=%s correlation_id=%s stage=%s error_type=%s",
                        record_id,
                        correlation_id,
                        stage,
                        err_type,
                    )
                    if attempt >= max_attempts:
                        try:
                            publish_dlq(
                                producer,
                                kafka_dlq_topic,
                                {
                                    "original_event": event
                                    or {
                                        "unparseable_event": True,
                                        "event_sha256": sha256_hex(msg.value() or b""),
                                        "size_bytes": len(msg.value() or b""),
                                    },
                                    "error": {
                                        "type": err_type,
                                        "message": safe_error_message(exc),
                                        "stage": stage,
                                    },
                                    "attempts": attempt,
                                    "failed_at": datetime.now(timezone.utc).isoformat(),
                                },
                            )
                            consumer.commit(message=msg)
                            break
                        except Exception as dlq_exc:
                            logger.error(
                                "dlq_failed record_id=%s correlation_id=%s stage=dlq_publish error_type=%s",
                                record_id,
                                correlation_id,
                                type(dlq_exc).__name__,
                            )
                            sleep_ms = backoff_base_ms * (2 ** (attempt - 1))
                            time.sleep(sleep_ms / 1000.0)
                        continue

                    sleep_ms = backoff_base_ms * (2 ** (attempt - 1))
                    time.sleep(sleep_ms / 1000.0)
    finally:
        consumer.close()
        db_conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
