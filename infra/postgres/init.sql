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

CREATE TABLE IF NOT EXISTS raw_object_manifest (
  record_id text PRIMARY KEY CHECK (record_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
  source text NOT NULL,
  bucket text NOT NULL,
  object_key text NOT NULL,
  sha256 text NOT NULL,
  size_bytes bigint NOT NULL,
  state text NOT NULL CHECK (state IN ('written','enqueued','canonicalized','orphaned')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_object_manifest_source_state
  ON raw_object_manifest (source, state);

CREATE INDEX IF NOT EXISTS idx_raw_object_manifest_state_updated_at
  ON raw_object_manifest (state, updated_at);
