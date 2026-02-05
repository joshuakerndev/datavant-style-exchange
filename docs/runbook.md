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

## Investigating missing canonical records

Goal: determine whether the gap is raw storage, publish/consume, or normalization.

1) Verify raw object exists
   - MinIO console or `mc` (if configured)
   - Expected key: `<source>/<record_id>.json`

2) Verify outbox publish status
   - `docker compose exec postgres psql -U exchange -d exchange`
   - Query:
   ```sql
   SELECT id, topic, key, created_at, published_at
   FROM outbox_events
   WHERE key = '<record_id>'
   ORDER BY id DESC
   LIMIT 5;
   ```

3) Check Kafka consumption + DLQ
   - `docker compose exec redpanda rpk topic consume record.ingested.v1 -n 1 --brokers redpanda:9092`
   - `docker compose exec redpanda rpk topic consume record.ingested.v1.dlq -n 1 --brokers redpanda:9092`
   - Repeat for `v2` if applicable

4) Verify canonical row
   - `docker compose exec postgres psql -U exchange -d exchange`
   - `SELECT record_id, normalized_at FROM canonical_records WHERE record_id = '<record_id>';`

5) Use replay/verify if raw exists but canonical is missing
   - `docker compose --profile tools run --rm replayer --source <source> --verify`
   - If the missing record is in raw storage, re-emit with replay:
     - `docker compose --profile tools run --rm replayer --source <source> --limit 1`

## Outbox backlog or publish failures

Symptoms: `outbox_events.published_at` stays NULL, ingest succeeds but no downstream processing.

1) Confirm backlog
   ```sql
   SELECT count(*) FROM outbox_events WHERE published_at IS NULL;
   ```
2) Validate Kafka connectivity
   - Check `KAFKA_BROKERS` env on `ingest-api-go`
   - Restart Redpanda if needed: `docker compose restart redpanda`
3) Observe publisher logs
   - `docker compose logs -f ingest-api-go`
   - Look for `outbox publish failed` or `failed to publish outbox event`

## Normalizer failures & DLQ behavior

The normalizer retries with exponential backoff (`MAX_ATTEMPTS`, `BACKOFF_BASE_MS`), and only commits offsets after:
1) canonical write succeeds (or is a duplicate), or
2) the DLQ publish succeeds.

Operator checks:
- Consume DLQ messages:
  - `record.ingested.v1.dlq` and `record.ingested.v2.dlq`
- Inspect DLQ envelope fields: `original_event`, `error.type`, `error.message`, `error.stage`, `attempts`, `failed_at`
- Check `normalizer-worker-py` logs for stage and error type

## Processing Ledger (Milestone 2)

The ledger stores attempt history and current state. `error_message` is intentionally sanitized.

Counts by status:
```sql
SELECT status, COUNT(*) AS count
FROM processing_stage_state
GROUP BY status
ORDER BY status;
```

Latest retryable failures:
```sql
SELECT record_id, pipeline, last_error_type, last_error_stage, last_attempt_at, next_retry_at
FROM processing_stage_state
WHERE status = 'failed_retryable'
ORDER BY last_attempt_at DESC
LIMIT 25;
```

Attempt history for a record_id:
```sql
SELECT pipeline, attempt_no, status, started_at, ended_at, result, error_type, error_stage, transient
FROM processing_attempts
WHERE record_id = '<record_id>'
ORDER BY attempt_no DESC;
```

DLQ'd recently:
```sql
SELECT record_id, pipeline, last_error_type, last_error_stage, updated_at
FROM processing_stage_state
WHERE status = 'dlq'
ORDER BY updated_at DESC
LIMIT 25;
```

## Replay vs reconciliation: when to use which

- **Replay (implemented):** deterministic re-emit of raw objects; use for backfills or to rebuild canonical when raw objects exist.
- **Reconciliation (implemented, report-only):** explicit raw↔manifest↔canonical comparison for the non-atomic boundary.
  - Run: `docker compose --profile tools run --rm reconciler --source <source>`
  - Repair mode remains future work (this milestone is report-only).

## Known failure modes and how to reason about them

- **Kafka down:** ingest succeeds; outbox grows; publish resumes when Kafka returns.
- **MinIO down:** ingest fails before DB writes; no outbox entry or idempotency row is created.
- **Postgres down:** ingest fails; no raw->DB handshake beyond raw object write; normalizer cannot write canonical.
- **Tokenizer down:** normalizer retries then DLQ; no PII in DLQ envelopes.
- **Non-atomic raw/DB boundary:** raw objects can exist without corresponding DB rows; use replay now, reconciliation later.

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

8) Reconcile raw, manifest, canonical (report-only)
   - `docker compose --profile tools run --rm reconciler --source demo-source`

## Milestone 6 — Dual-Version Compatibility (Local)

This runbook validates v2 ingest, dual-version normalization, and versioned DLQ routing.
It is a local schema-evolution exercise, not end-to-end production hardening.
Topic creation source of truth is `infra/redpanda/init.sh`; keep helper scripts in sync.

1) Fresh start (optional)
   - `docker compose down -v`
   - `docker compose up --build -d`

2) Verify topics exist
   - `docker compose exec redpanda rpk topic list --brokers redpanda:9092`
   - Expect: `record.ingested.v1`, `record.ingested.v2`, `record.ingested.v1.dlq`, `record.ingested.v2.dlq`

3) Verify canonical schema includes new columns
   - `docker compose exec postgres psql -U exchange -d exchange -c "SELECT column_name FROM information_schema.columns WHERE table_name='canonical_records' AND column_name IN ('event_version','record_kind','schema_hint') ORDER BY column_name;"`
   - Expect 3 rows: `event_version`, `record_kind`, `schema_hint`

4) v2 ingest happy path
   - `curl -X POST http://localhost:8080/v2/ingest \
     -H "Authorization: Bearer dev" \
     -H "Idempotency-Key: v2-demo-$(date +%s)" \
     -H "Content-Type: application/json" \
     -d '{
       "source": "demo-source",
       "record_kind": "claim",
       "schema_hint": "partnerA.v2",
       "patient": { "given_name": "Sam", "family_name": "Lee", "dob": "1990-01-01" },
       "payload": { "example": true }
     }'`
   - Save the `record_id` from the response
   - Verify raw object exists in MinIO under `<source>/<record_id>.json` (MinIO console is OK)
   - Verify canonical row exists and has v2 metadata:
     - `docker compose exec postgres psql -U exchange -d exchange -c "SELECT record_id, event_version, record_kind, schema_hint, normalized FROM canonical_records WHERE record_id = '<record_id>';"`
   - Confirm `event_version = 2`, `record_kind = claim`, `schema_hint = partnerA.v2`
   - Confirm `normalized` includes only `patient_token` and `meta` (no patient fields)

5) v2 idempotency semantics
   - Same `Idempotency-Key` + same body returns same `record_id`
   - Same `Idempotency-Key` + different body returns `409`
   ```bash
   IDK=v2-idem-$(date +%s)
   curl -s -X POST http://localhost:8080/v2/ingest \
     -H "Authorization: Bearer dev" \
     -H "Idempotency-Key: $IDK" \
     -H "Content-Type: application/json" \
     -d '{"source":"demo-source","record_kind":"claim","schema_hint":"partnerA.v2","patient":{"given_name":"Sam","family_name":"Lee","dob":"1990-01-01"},"payload":{"example":true}}'
   curl -s -X POST http://localhost:8080/v2/ingest \
     -H "Authorization: Bearer dev" \
     -H "Idempotency-Key: $IDK" \
     -H "Content-Type: application/json" \
     -d '{"source":"demo-source","record_kind":"claim","schema_hint":"partnerA.v2","patient":{"given_name":"Sam","family_name":"Lee","dob":"1990-01-01"},"payload":{"example":true}}'
   curl -i -X POST http://localhost:8080/v2/ingest \
     -H "Authorization: Bearer dev" \
     -H "Idempotency-Key: $IDK" \
     -H "Content-Type: application/json" \
     -d '{"source":"demo-source","record_kind":"claim","schema_hint":"partnerA.v2","patient":{"given_name":"Sam","family_name":"Lee","dob":"1990-01-02"},"payload":{"example":true}}'
   ```
   - Expect the first two responses to match (`record_id`), and the last to return `409`

6) v2 validation
   - Invalid `record_kind` returns `400`
   ```bash
   curl -i -X POST http://localhost:8080/v2/ingest \
     -H "Authorization: Bearer dev" \
     -H "Idempotency-Key: v2-bad-$(date +%s)" \
     -H "Content-Type: application/json" \
     -d '{"source":"demo-source","record_kind":"bad_kind","schema_hint":"partnerA.v2","patient":{"given_name":"Sam","family_name":"Lee","dob":"1990-01-01"},"payload":{"example":true}}'
   ```

7) v1 remains supported
   - `curl -X POST http://localhost:8080/v1/ingest \
     -H "Authorization: Bearer dev" \
     -H "Idempotency-Key: v1-demo-$(date +%s)" \
     -H "Content-Type: application/json" \
     -d '{
       "source": "demo-source",
       "record_type": "lab_result",
       "patient": { "first_name": "Sam", "last_name": "Lee", "dob": "1990-01-01" },
       "payload": { "example": true }
     }'`
   - Verify canonical row has v1 metadata:
     - `docker compose exec postgres psql -U exchange -d exchange -c "SELECT record_id, event_version, record_kind, schema_hint FROM canonical_records WHERE record_id = '<record_id>';"`
   - Expect `event_version = 1` and `record_kind`/`schema_hint` are `NULL`

8) DLQ routing by version
   - Stop tokenizer:
     - `docker compose stop tokenizer`
   - Ingest a v2 record (same payload as step 4 with new Idempotency-Key)
   - Wait for retries to exhaust, then consume one DLQ message:
     - `docker compose exec redpanda rpk topic consume record.ingested.v2.dlq -n 1 --brokers redpanda:9092`
   - Restart tokenizer:
     - `docker compose start tokenizer`
