# Datavant-Style Exchange (Mini Platform)

A Datavant-inspired, local-first mini platform that simulates **secure healthcare data exchange**:

**REST ingest → raw storage (S3) → event streaming (Kafka) → (next) normalization + query**

This project is intentionally scoped to demonstrate **senior/staff-level engineering judgment** rather than raw scale:
system decomposition, idempotency, reliability, security boundaries, and operational correctness.

---

## Why this exists (aligned to the Datavant Senior SWE role)

This repository mirrors the real-world platform concerns described in Datavant’s Senior Software Engineer role:

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
  - REST API (`POST /v1/ingest`)
  - Payload validation
  - **True idempotency** via Postgres
  - Raw object persistence to S3 (MinIO)
  - Event publication to Kafka (Redpanda)
  - Correlation IDs and structured logging

### Planned services
- `tokenizer-go` — deterministic PII tokenization service
- `normalizer-worker-py` — async consumer → canonical SQL writes
- `graphql-api-next` — read-only product-facing query layer

### Local infrastructure
- **MinIO** — S3-compatible raw object storage  
- **Redpanda** — Kafka-compatible event streaming  
- **Postgres** — idempotency, outbox, audit, canonical data (future)  
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
1. Client calls `POST /v1/ingest` with `Idempotency-Key`
2. API validates payload + computes request hash
3. **Idempotency enforced via Postgres**
   - same key + same body → same response
   - same key + different body → 409 conflict
4. Raw JSON written to S3 (MinIO)
5. Event published to Kafka (`record.ingested.v1`)
6. API returns `202 Accepted`

### Replay / backfill (planned)
- Reprocess raw objects from S3
- Re-emit events deterministically
- Validate canonical completeness

---

## Security model (local + production intent)

- JWT-based authentication with issuer + audience validation
- **Local dev shortcut:**  
  `Authorization: Bearer dev` is accepted when `ENV=local`
- No PII is logged
- Tokenization will be a strict downstream boundary (planned)

---

## Reliability & correctness primitives

### Implemented
- **Idempotent ingest API**
- Deterministic request hashing
- Durable raw object storage
- Correlation IDs across logs and events

### Planned
- **Outbox pattern** (DB → Kafka consistency)
- Retry + backoff
- DLQ handling + reprocessing
- Schema versioning (v1 → v2 migration path)

---

## Quickstart (local)

```bash
cp .env.example .env
make up


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

Outbox pattern: next

Normalization pipeline: planned

Tokenization: planned