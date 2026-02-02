# Datavant-Style Exchange (Mini Platform)

A Datavant-inspired, local-first mini platform that simulates **secure healthcare data exchange**:

**REST ingest → raw storage (S3) → event streaming (Kafka) → normalization + tokenization → canonical write → replay/backfill → query**

This project is intentionally scoped to demonstrate **engineering judgment** rather than raw scale:
system decomposition, idempotency, reliability, security boundaries, and operational correctness.

---

## Why this exists

This repository mirrors the real-world platform concerns:

- **Architectural ownership & end-to-end delivery**  
  One engineer owns request lifecycle from API → storage → messaging → durability.

- **System decomposition**  
  Clear boundaries between ingestion, storage, messaging, and (future) processing layers.

- **Reliability & correctness**  
  Idempotent APIs, durable writes, deterministic behavior, and explicit failure handling.

- **Security & privacy by design**  
  JWT-based auth boundaries, no PII in logs, tokenization as a first-class concept.

- **Operational thinking**  
  Dockerized local infra, health checks, startup ordering, observability hooks.

---

## Architecture (current state)

### Implemented services
- **`ingest-api-go`**
  - REST API (`GET /healthz`, `POST /v1/ingest`)
  - Payload validation
  - Idempotency via Postgres (see notes on current guarantees)
  - Raw object persistence to S3 (MinIO)
  - Event publication to Kafka (Redpanda) via Postgres outbox + background publisher
  - Correlation IDs and structured logging
- **`tokenizer-go`**
  - REST API (`GET /healthz`, `POST /tokenize`)
  - Deterministic HMAC tokenization of patient identifiers
  - Auth boundary (`Bearer dev` in `ENV=local`, otherwise JWT validation)
  - No PII in logs
  - Canonicalization rules documented in `docs/decisions/0001-tokenization-canonicalization.md`
- **`normalizer-worker-py`**
  - Kafka consumer for `record.ingested.v1`
  - Fetches raw objects from MinIO
  - Writes canonical records idempotently to Postgres
  - Bounded retries + DLQ publish on failure
- **`replayer-cli-py`**
  - CLI tool for deterministic replay of raw objects from MinIO
  - Re-emits `record.ingested.v1` events
  - Supports completeness verification (raw vs canonical)

### Planned services
- `graphql-api-next` — read-only product-facing query layer

### Local infrastructure
- **MinIO** — S3-compatible raw object storage  
- **Redpanda** — Kafka-compatible event streaming  
- **Postgres** — idempotency, outbox, audit, canonical data  
- **Prometheus / Grafana** — metrics & dashboards (hooked, minimal)

---

## Contracts-first development

All behavior is driven by versioned contracts:

- `contracts/api/*openapi.yaml` — REST APIs
- `contracts/events/*json` — Kafka event schemas
- `contracts/graphql/schema.graphql` — GraphQL schema (planned)

**Rule:** contracts are immutable without version bumps + ADRs.

---

## Data flow

### Ingest path (online)
1. Client calls `POST /v1/ingest` with `Idempotency-Key` (min length enforced)
2. API validates payload + computes request hash (SHA-256 hex)
3. **Idempotency enforced via Postgres**
   - same key + same body → same response
   - same key + different body → 409 conflict
4. Raw JSON written to S3 (MinIO)
5. Event written to Postgres `outbox_events` (topic/key/payload)
6. Background publisher publishes to Kafka and marks `published_at`
7. API returns `202 Accepted`

### Replay / backfill
- Reprocess raw objects from S3
- Re-emit events deterministically
- Validate canonical completeness

---

## Security model (local + production intent)

- JWT-based authentication with issuer + audience validation
- **Local dev shortcut:**  
  `Authorization: Bearer dev` is accepted when `ENV=local`
- No PII is logged
- Tokenization is a strict downstream boundary

---

## Reliability & correctness primitives

### Implemented
- **Idempotent ingest API**
- **Outbox pattern** (DB-backed queue + async publisher)
- **Per-key idempotency serialization via Postgres advisory lock**
- Deterministic request hashing
- Durable raw object storage
- Reprocessing workflows
- Bounded retries + DLQ handling (normalizer worker)
- Correlation IDs across logs and events

### Planned
- See "Planned milestones" below

### Notes on current guarantees
- Idempotency + outbox rows are persisted transactionally after MinIO write succeeds
- Kafka downtime no longer breaks ingest; events queue in Postgres and publish later
- Remaining limitation: MinIO and Postgres are not atomic, so a crash after MinIO write but before tx commit can orphan a raw object

---

## Quickstart (local)

```bash
cp .env.example .env
make up
```

Endpoints

Ingest API: http://localhost:8080

Health: GET /healthz

MinIO Console: http://localhost:9001

Grafana: http://localhost:3000

Prometheus: http://localhost:9090

Demo: Idempotency
# First request
curl -X POST http://localhost:8080/v1/ingest \
  -H "Authorization: Bearer dev" \
  -H "Idempotency-Key: demo-12345678" \
  -H "Content-Type: application/json" \
  -d '{...}'

# Same key + same body → same record_id
# Same key + different body → 409 conflict

Design tradeoffs

Docker Compose over Kubernetes for clarity and speed

Local S3/Kafka equivalents (MinIO/Redpanda) for realism without cloud cost

Auth shortcuts in local dev, strict boundaries in design

Status

Ingest + idempotency: complete

Outbox pattern: complete

Normalization pipeline: complete

Tokenization: complete

Reprocessing / backfill: complete

## Next milestones (planned)

The core data exchange platform is complete. The next phase focuses on **schema evolution and Python/SQL-heavy data processing**, aligned with real-world data platform needs.

Planned milestones:

- **Schema evolution (v2 ingest)**
  - Introduce `/v2/ingest` with a versioned event schema
  - Dual-version event handling in downstream consumers
  - Backward compatibility and replay support across versions

- **Derived analytics / data processing (Python + SQL)**
  - Batch-oriented Python jobs that read from canonical records
  - Materialize derived tables using explicit SQL (aggregations, window functions)
  - Idempotent, verifiable backfills and data quality checks
  - Designed to simulate downstream analytics or interoperability pipelines
