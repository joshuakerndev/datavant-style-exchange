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
- REST endpoint: `POST /v1/ingest`
- Requires `Idempotency-Key`
- Enforces true idempotency using Postgres:
  - same key + same body → same response
  - same key + different body → 409 conflict
- Writes raw JSON payloads to S3 (MinIO)
- Publishes `record.ingested.v1` events to Kafka (Redpanda)
- Uses correlation IDs
- Logs are structured and PII-safe
- Local dev auth shortcut: `Authorization: Bearer dev` when `ENV=local`

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

---

## Known next steps (planned work)

### 1. Outbox pattern (high priority)
- Insert Kafka events into `outbox_events` table instead of publishing inline
- Background publisher marks rows as published
- Eliminates orphaned raw objects if Kafka is unavailable

### 2. Normalizer worker (Python)
- Consume `record.ingested.v1`
- Call tokenizer
- Write canonical SQL tables
- Retry + DLQ

### 3. Tokenizer service
- Deterministic HMAC-based tokenization
- Strict auth boundary
- No PII leaves tokenizer

### 4. v2 ingest + schema migration
- Support `/v2/ingest`
- New event schema
- Backward compatibility

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
