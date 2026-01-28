# Runbook — Datavant-Style Exchange (Local)

## Verify Outbox Pattern (Kafka Down Test)

The ingest API does not publish to Kafka inline. Instead, it writes events to Postgres `outbox_events`,
and a background publisher reads pending rows and publishes them once Kafka is available. This runbook
verifies ingest success while Kafka is down, durable queuing in Postgres, and eventual publish after
Kafka is restored.

1) Confirm services are running
   - `docker compose ps`
   - Ensure `ingest-api-go` and `postgres` are up

2) Stop Kafka (Redpanda)
   - `docker compose stop redpanda`

3) Send an ingest request
   - Use a new `Idempotency-Key`
   - Minimal valid payload (record_type = encounter, patient, payload present)
   ```bash
   curl -X POST http://localhost:8080/v1/ingest \
     -H "Authorization: Bearer dev" \
     -H "Idempotency-Key: demo-$(date +%s)" \
     -H "Content-Type: application/json" \
     -d '{
       "source": "demo-source",
       "record_type": "encounter",
       "patient": { "first_name": "Sam", "last_name": "Lee", "dob": "1990-01-01" },
       "payload": { "example": true }
     }'
   ```

4) Verify outbox row exists and is pending
   - `docker compose exec postgres psql -U exchange -d exchange`
   - Query the outbox table:
   ```sql
   SELECT id, topic, key, created_at, published_at
   FROM outbox_events
   ORDER BY id DESC
   LIMIT 5;
   ```
   - `published_at` should be `NULL` while Kafka is down

5) Restart Kafka (Redpanda)
   - `docker compose start redpanda`

6) Verify outbox drains
   - Re-run the same SQL query
   - `published_at` should now be populated for the new row

## Common Failure Modes

- Kafka down → outbox rows accumulate with `published_at = NULL`
- Kafka topic misconfiguration → publisher logs errors but keeps retrying
- Incorrect broker address → publisher cannot connect (check `KAFKA_BROKERS`)
- Writer/topic mismatch (kafka-go) → topic must be set either on the writer or per message, not both

## Notes on Guarantees and Limitations

- Outbox makes Kafka publishing durable and retryable, not exactly-once.
- Idempotency is serialized per `Idempotency-Key` via Postgres advisory locks.
- MinIO writes and DB transactions are not atomic; a crash after MinIO write but before DB commit can orphan a raw object.
- This tradeoff is intentional and documented.

## Milestone 3 — Normalizer Worker

This runbook validates the normalizer worker happy path, DLQ behavior, and idempotency.

1) Start the stack
   - `docker compose up -d`
   - Confirm `normalizer-worker-py` is running with `docker compose ps`

2) Ingest a record (capture `record_id`)
   - `curl -X POST http://localhost:8080/v1/ingest \
     -H "Authorization: Bearer dev" \
     -H "Idempotency-Key: normalizer-demo-$(date +%s)" \
     -H "Content-Type: application/json" \
     -d '{
       "source": "demo-source",
       "record_type": "encounter",
       "patient": { "first_name": "Sam", "last_name": "Lee", "dob": "1990-01-01" },
       "payload": { "example": true }
     }'`
   - Save the `record_id` from the response

3) Verify canonical row exists
   - `docker compose exec postgres psql -U exchange -d exchange`
   - Query:
   ```sql
   SELECT record_id, source, raw_object_key, raw_sha256, ingested_at, normalized_at, correlation_id
   FROM canonical_records
   WHERE record_id = '<record_id>';
   ```
   - `ingested_at` comes from the event `occurred_at` field

### Important: How to correctly trigger DLQ in this system

In this architecture, the ingest API writes the raw payload to MinIO before publishing a Kafka event.
If the raw object cannot be persisted, ingest fails and no Kafka event is emitted.

- Stopping MinIO before calling `/v1/ingest` will cause ingest to fail with `failed to persist raw object`,
  and no DLQ message will be produced.
- This is expected behavior and an intentional tradeoff, not an error.

To exercise the normalizer DLQ path, the failure must occur after the Kafka event already exists.

4) Simulate failure and verify DLQ
   - Stop the normalizer worker:
     - `docker compose stop normalizer-worker-py`
   - Ensure MinIO is running:
     - `docker compose start minio`
   - Ingest a new record (use a new Idempotency-Key)
   - Stop MinIO:
     - `docker compose stop minio`
   - Restart the normalizer worker without dependencies:
     - `docker compose up -d --no-deps normalizer-worker-py`
   - Wait for retries to exhaust, then verify DLQ messages:
     - `docker compose exec redpanda rpk topic consume record.ingested.v1.dlq -n 1 --brokers redpanda:9092`
   - Confirm the DLQ envelope includes `original_event`, `error` (type, message, stage), `attempts`, and `failed_at`

5) Idempotency test (no duplicate rows)
   - Reprocess the same `record_id` (replay the same Kafka message or re-run the worker against the same event)
   - Verify row count remains 1:
   ```sql
   SELECT COUNT(*) FROM canonical_records WHERE record_id = '<record_id>';
   ```

## Milestone 4 — Tokenizer Boundary

This runbook validates tokenizer determinism and confirms canonical records contain tokens only.

1) Start the stack
   - `docker compose up -d`
   - Confirm `tokenizer` and `normalizer-worker-py` are running with `docker compose ps`

2) Verify tokenizer determinism
   - Call `/tokenize` twice with the same identifiers:
   ```bash
   curl -X POST http://localhost:8081/tokenize \
     -H "Authorization: Bearer dev" \
     -H "Content-Type: application/json" \
     -d '{"given_name":"Sam","family_name":"Lee","dob":"1990-01-01","ssn":"123-45-6789"}'
   ```
   - Repeat the same request; confirm `patient_token` is identical.
   - Change a single field (e.g., `dob`) and confirm `patient_token` changes.

3) Confirm canonical records contain tokens, not PII
   - Ingest a record as in Milestone 3 and capture the `record_id`
   - Query the canonical row:
   ```sql
   SELECT record_id, normalized
   FROM canonical_records
   WHERE record_id = '<record_id>';
   ```
   - Confirm `normalized` includes `patient_token` and `meta`, and does **not** include `patient` fields or raw payloads.
   - If the tokenizer is down, the normalizer will retry and then DLQ with a PII-safe envelope.
   - DLQ `error.message` is conservative (e.g., "tokenizer non-200") and never includes patient identifiers.

## Milestone 5 — Reprocessing / Backfill

This runbook validates deterministic replay from raw storage, canonical rebuild, and completeness verification.

1) Start the stack
   - `docker compose up -d`
   - Confirm `ingest-api-go`, `normalizer-worker-py`, `minio`, `redpanda`, and `postgres` are healthy

2) Ingest a few records (same source)
   - Use new `Idempotency-Key` values
   ```bash
   curl -X POST http://localhost:8080/v1/ingest \
     -H "Authorization: Bearer dev" \
     -H "Idempotency-Key: replay-demo-$(date +%s)" \
     -H "Content-Type: application/json" \
     -d '{
       "source": "demo-source",
       "record_type": "encounter",
       "patient": { "first_name": "Sam", "last_name": "Lee", "dob": "1990-01-01" },
       "payload": { "example": true }
     }'
   ```

3) Drop canonical data (simulate rebuild)
   - Stop normalizer worker:
     - `docker compose stop normalizer-worker-py`
   - Truncate canonical table:
     - `docker compose exec postgres psql -U exchange -d exchange -c "TRUNCATE canonical_records;"`

4) Re-emit events from raw storage
   - `docker compose --profile tools run --rm replayer --source demo-source`

5) Restart normalizer worker
   - `docker compose start normalizer-worker-py`

6) Confirm canonical rebuild
   - `docker compose exec postgres psql -U exchange -d exchange -c "SELECT count(*) FROM canonical_records WHERE source='demo-source';"`

7) Verify completeness (raw vs canonical)
   - `docker compose --profile tools run --rm replayer --source demo-source --verify`
   - `missing_count` should be `0`
