# AI Development Context — Datavant-Style Exchange

This document exists to give AI assistants (ChatGPT, Codex, Claude) **persistent context** when working on this repo.

If you are an AI reading this:
- Assume the human developer understands system design and backend fundamentals
- Prioritize correctness, reliability, and clarity over cleverness
- Do NOT change contracts without explicit instruction

---

## Project goal

Demonstrate **senior/staff-level backend platform thinking** aligned with Datavant-style healthcare data exchange:
- deterministic behavior
- idempotency
- reliability under partial failure
- security and privacy boundaries
- clear system decomposition

This is NOT a toy CRUD app.

---

## Current implemented state (authoritative)

### ingest-api-go
- Language: Go
- REST endpoints: `GET /healthz`, `POST /v1/ingest`, `POST /v2/ingest`
- Requires `Idempotency-Key` (min length enforced)
- v1 `record_type` enum: `encounter`, `claim`, `lab_result`
- v2 requires `record_kind` enum (`encounter`, `claim`, `lab_result`) and `schema_hint`
- Idempotency uses Postgres `idempotency_keys`:
  - request hash is SHA-256 of body, stored as hex
  - same key + same body → replayed response (same `record_id`/`correlation_id`)
  - same key + different body → 409 conflict
  - per-key serialization via `pg_advisory_xact_lock(hashtext(key))`
  - idempotency response stored in the same DB transaction as the outbox insert
- Writes raw JSON payloads to S3 (MinIO) with key format: `<source>/<record_id>.json`
- Enqueues outbox row; publisher publishes asynchronously
  - outbox row includes versioned topic, key, payload (bucket/key, sha256, size, `correlation_id`)
- Correlation ID uses `X-Correlation-Id` if acceptable, otherwise generates a UUID
- Logs are structured and PII-safe
- Local dev auth shortcut: `Authorization: Bearer dev` when `ENV=local`
- Otherwise JWT validation is enforced (issuer/audience/secret)
- Known limitation: MinIO write and DB transaction are not atomic; a crash between them can orphan a raw object

### tokenizer-go
- REST endpoints: `GET /healthz`, `POST /tokenize`
- Deterministic HMAC-SHA256 tokenization
- Auth boundary mirrors ingest (local dev shortcut; otherwise JWT)
- No PII in logs

### normalizer-worker-py
- Consumes `record.ingested.v1` and `record.ingested.v2` (via `KAFKA_TOPICS` or single-topic mode)
- Fetches raw objects from MinIO
- Writes to `canonical_records` idempotently (ON CONFLICT DO NOTHING)
- Persists v2 metadata to `canonical_records` (`event_version`, `record_kind`, `schema_hint`; nullable for v1)
- Parses `occurred_at` into a timezone-aware datetime before inserting `ingested_at`
- Retries with bounded backoff; on failure publishes to `record.ingested.v1.dlq` or `record.ingested.v2.dlq`
- Commits Kafka offsets only after DB success (including dup) or DLQ publish success

### replayer-cli-py
- Deterministic replay tool
- Emits v1, v2, or auto-inferred events from raw storage (`--emit-version {v1,v2,auto}`, default v1)
- Deterministic inference rules based on raw object shape
- Verifies canonical completeness

### Infrastructure
- Docker Compose
- Postgres (idempotency_keys, outbox_events, audit_log tables exist)
- MinIO bucket: `raw-objects`
- Redpanda topics auto-created via `redpanda-init`

---

## Important constraints

- **Contracts are source of truth**
  - OpenAPI, event schemas, GraphQL schema
- Avoid changing existing API shapes
- Avoid logging PII
- Prefer small, composable changes
- Favor explicit error handling
- Do not “fix” known limitations unless explicitly instructed.


---

## Known next steps (planned work)

### 1. Derived analytics / data processing (Python + SQL)
- Batch-oriented Python jobs that read from canonical records
- Materialize derived tables using explicit SQL
- Idempotent, verifiable backfills and data quality checks


---

## Development rules for AI

When generating code:
- Keep diffs minimal
- Prefer explicit SQL over ORMs
- Prefer standard libraries
- Do not invent infrastructure
- Ask before large refactors

When unsure:
- Ask clarifying questions instead of guessing
