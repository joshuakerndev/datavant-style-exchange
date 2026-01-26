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
