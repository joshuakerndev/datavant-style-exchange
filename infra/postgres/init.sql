-- Minimal shared tables (we’ll add per-service migrations later)

CREATE TABLE IF NOT EXISTS audit_log (
  id bigserial PRIMARY KEY,
  occurred_at timestamptz NOT NULL DEFAULT now(),
  correlation_id text NOT NULL,
  actor text NOT NULL,
  action text NOT NULL,
  entity_type text NOT NULL,
  entity_id text NOT NULL,
  status text NOT NULL,
  details jsonb
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
  id bigserial PRIMARY KEY,
  key text NOT NULL UNIQUE,
  request_sha256 text NOT NULL,
  response jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox_events (
  id bigserial PRIMARY KEY,
  topic text NOT NULL,
  key text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);

CREATE TABLE IF NOT EXISTS canonical_records (
  record_id uuid PRIMARY KEY,
  source text NOT NULL,
  raw_object_key text NOT NULL,
  raw_sha256 text NOT NULL,
  normalized jsonb NOT NULL,
  ingested_at timestamptz NOT NULL,
  normalized_at timestamptz NOT NULL DEFAULT now(),
  correlation_id text NOT NULL
);

ALTER TABLE canonical_records
  ADD COLUMN IF NOT EXISTS event_version text,
  ADD COLUMN IF NOT EXISTS record_kind text,
  ADD COLUMN IF NOT EXISTS schema_hint text;
