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

CREATE TABLE IF NOT EXISTS processing_stage_state (
  record_id uuid NOT NULL,
  pipeline text NOT NULL,
  status text NOT NULL CHECK (status IN ('running','succeeded','failed_retryable','dlq')),
  attempt_count integer NOT NULL DEFAULT 0,
  last_attempt_at timestamptz,
  last_success_at timestamptz,
  next_retry_at timestamptz,
  correlation_id text,
  event_version text,
  event_id text,
  kafka_topic text,
  kafka_partition integer,
  kafka_offset bigint,
  last_error_type text,
  last_error_message text,
  last_error_stage text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (record_id, pipeline)
);

CREATE INDEX IF NOT EXISTS idx_processing_stage_state_status_updated_at
  ON processing_stage_state (status, updated_at);

CREATE TABLE IF NOT EXISTS processing_attempts (
  record_id uuid NOT NULL,
  pipeline text NOT NULL,
  attempt_no integer NOT NULL,
  status text NOT NULL CHECK (status IN ('started','succeeded','failed','dlq_published')),
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  error_type text,
  error_message text,
  error_stage text,
  transient boolean,
  result text,
  correlation_id text,
  event_version text,
  event_id text,
  kafka_topic text,
  kafka_partition integer,
  kafka_offset bigint,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (record_id, pipeline, attempt_no)
);

CREATE INDEX IF NOT EXISTS idx_processing_attempts_record_pipeline_attempt
  ON processing_attempts (record_id, pipeline, attempt_no);

CREATE TABLE IF NOT EXISTS job_runs (
  job_run_id bigserial PRIMARY KEY,
  job_name text NOT NULL,
  status text NOT NULL CHECK (status IN ('running','succeeded','failed')),
  started_at timestamptz NOT NULL DEFAULT now(),
  ended_at timestamptz,
  params jsonb NOT NULL DEFAULT '{}'::jsonb,
  progress jsonb NOT NULL DEFAULT '{}'::jsonb,
  error text
);

CREATE INDEX IF NOT EXISTS idx_job_runs_name_started_at
  ON job_runs (job_name, started_at DESC);

CREATE TABLE IF NOT EXISTS job_checkpoints (
  job_name text PRIMARY KEY,
  checkpoint jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS derived_patient_record_index (
  patient_token text NOT NULL,
  record_id uuid NOT NULL,
  source text NOT NULL,
  event_version text NOT NULL,
  record_kind text NOT NULL,
  schema_hint text,
  ingested_at timestamptz NOT NULL,
  normalized_at timestamptz NOT NULL,
  PRIMARY KEY (patient_token, record_id)
);

CREATE INDEX IF NOT EXISTS idx_derived_patient_record_index_patient_token
  ON derived_patient_record_index (patient_token);

CREATE INDEX IF NOT EXISTS idx_derived_patient_record_index_source_ingested_at
  ON derived_patient_record_index (source, ingested_at);
