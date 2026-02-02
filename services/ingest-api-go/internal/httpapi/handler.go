package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/segmentio/kafka-go"

	"datavant-style-exchange/services/ingest-api-go/internal/config"
	"datavant-style-exchange/services/ingest-api-go/internal/ingest"
	"datavant-style-exchange/services/ingest-api-go/internal/metrics"
)

const (
	maxBodyBytes    int64         = 10 << 20
	minIdempotency  int           = 8
	minCorrelation  int           = 8
	minSourceLength int           = 1
	minPatientDOB   int           = 1
	minRecordType   int           = 1
	minPatientName  int           = 1
	storageTimeout                = 5 * time.Second
	dbTimeout                     = 5 * time.Second
)

type Handler struct {
	cfg        config.Config
	logger     *slog.Logger
	minio      *minio.Client
	kafkaWrite *kafka.Writer
	db         *sql.DB
}

func New(cfg config.Config, logger *slog.Logger, minioClient *minio.Client, kafkaWriter *kafka.Writer, db *sql.DB) *Handler {
	return &Handler{
		cfg:        cfg,
		logger:     logger,
		minio:      minioClient,
		kafkaWrite: kafkaWriter,
		db:         db,
	}
}

func (h *Handler) Healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) IngestV1(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if len(idempotencyKey) < minIdempotency {
		h.recordIngestStatus(http.StatusBadRequest)
		h.respondError(w, http.StatusBadRequest, "missing or invalid Idempotency-Key")
		return
	}

	correlationID := r.Header.Get("X-Correlation-Id")
	if len(correlationID) < minCorrelation {
		correlationID = uuid.NewString()
	}

	bodyReader := http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer bodyReader.Close()
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		h.recordIngestStatus(http.StatusBadRequest)
		h.respondError(w, http.StatusBadRequest, "invalid body")
		return
	}

	sha := sha256.Sum256(body)
	shaHex := hex.EncodeToString(sha[:])

	tx, err := h.db.BeginTx(r.Context(), nil)
	if err != nil {
		h.logger.Error("failed to open transaction", "error", err)
		h.recordIngestStatus(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to process request")
		return
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := h.lockIdempotencyKey(r.Context(), tx, idempotencyKey); err != nil {
		h.logger.Error("failed to lock idempotency key", "error", err)
		h.recordIngestStatus(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to process request")
		return
	}

	idempotentResponse, conflict, err := h.checkIdempotencyTx(r.Context(), tx, idempotencyKey, shaHex)
	if err != nil {
		h.logger.Error("failed to check idempotency", "error", err)
		h.recordIngestStatus(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to process request")
		return
	}
	if conflict {
		h.recordIngestStatus(http.StatusConflict)
		h.respondError(w, http.StatusConflict, "idempotency conflict")
		return
	}
	if idempotentResponse != nil {
		h.recordIngestStatus(http.StatusAccepted)
		writeJSONBytes(w, http.StatusAccepted, idempotentResponse)
		return
	}

	var record ingest.PartnerRecordV1
	if err := json.Unmarshal(body, &record); err != nil {
		h.recordIngestStatus(http.StatusBadRequest)
		h.respondError(w, http.StatusBadRequest, "invalid payload")
		return
	}
	if validationErr := validateRecord(record); validationErr != nil {
		h.recordIngestStatus(http.StatusBadRequest)
		h.respondError(w, http.StatusBadRequest, validationErr.Error())
		return
	}

	recordID := uuid.NewString()
	key := record.Source + "/" + recordID + ".json"

	if err := h.writeRawObject(r.Context(), key, body); err != nil {
		h.logger.Error("failed to write raw object", "error", err, "record_id", recordID, "correlation_id", correlationID)
		h.recordIngestStatus(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to persist raw object")
		return
	}

	response := ingest.IngestAccepted{RecordID: recordID, CorrelationID: correlationID}
	responseBytes, err := json.Marshal(response)
	if err != nil {
		h.recordIngestStatus(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to build response")
		return
	}

	event := buildRecordIngestedEvent(record, recordID, correlationID, h.cfg.RawBucket, key, shaHex, int64(len(body)))
	eventBytes, err := json.Marshal(event)
	if err != nil {
		h.recordIngestStatus(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to build event")
		return
	}

	if err := h.insertOutboxEventTx(r.Context(), tx, h.cfg.TopicRecordIngestedV1, recordID, eventBytes); err != nil {
		h.logger.Error("failed to persist outbox event", "error", err, "record_id", recordID, "correlation_id", correlationID)
		h.recordIngestStatus(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to persist outbox event")
		return
	}

	if err := h.storeIdempotencyTx(r.Context(), tx, idempotencyKey, shaHex, responseBytes); err != nil {
		h.logger.Error("failed to store idempotency", "error", err, "record_id", recordID, "correlation_id", correlationID)
		h.recordIngestStatus(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to persist idempotency")
		return
	}

	if err := tx.Commit(); err != nil {
		h.logger.Error("failed to commit ingest transaction", "error", err, "record_id", recordID, "correlation_id", correlationID)
		h.recordIngestStatus(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to process request")
		return
	}

	h.logger.Info("record ingested",
		"record_id", recordID,
		"correlation_id", correlationID,
		"source", record.Source,
		"size_bytes", len(body),
	)
	h.recordIngestStatus(http.StatusAccepted)
	writeJSONBytes(w, http.StatusAccepted, responseBytes)
}

func (h *Handler) IngestV2(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if len(idempotencyKey) < minIdempotency {
		h.recordIngestStatusV2(http.StatusBadRequest)
		h.respondError(w, http.StatusBadRequest, "missing or invalid Idempotency-Key")
		return
	}

	correlationID := r.Header.Get("X-Correlation-Id")
	if len(correlationID) < minCorrelation {
		correlationID = uuid.NewString()
	}

	bodyReader := http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer bodyReader.Close()
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		h.recordIngestStatusV2(http.StatusBadRequest)
		h.respondError(w, http.StatusBadRequest, "invalid body")
		return
	}

	sha := sha256.Sum256(body)
	shaHex := hex.EncodeToString(sha[:])

	tx, err := h.db.BeginTx(r.Context(), nil)
	if err != nil {
		h.logger.Error("failed to open transaction", "error", err)
		h.recordIngestStatusV2(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to process request")
		return
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := h.lockIdempotencyKey(r.Context(), tx, idempotencyKey); err != nil {
		h.logger.Error("failed to lock idempotency key", "error", err)
		h.recordIngestStatusV2(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to process request")
		return
	}

	idempotentResponse, conflict, err := h.checkIdempotencyTx(r.Context(), tx, idempotencyKey, shaHex)
	if err != nil {
		h.logger.Error("failed to check idempotency", "error", err)
		h.recordIngestStatusV2(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to process request")
		return
	}
	if conflict {
		h.recordIngestStatusV2(http.StatusConflict)
		h.respondError(w, http.StatusConflict, "idempotency conflict")
		return
	}
	if idempotentResponse != nil {
		h.recordIngestStatusV2(http.StatusAccepted)
		writeJSONBytes(w, http.StatusAccepted, idempotentResponse)
		return
	}

	var record ingest.PartnerRecordV2
	if err := json.Unmarshal(body, &record); err != nil {
		h.recordIngestStatusV2(http.StatusBadRequest)
		h.respondError(w, http.StatusBadRequest, "invalid payload")
		return
	}
	if validationErr := validateRecordV2(record); validationErr != nil {
		h.recordIngestStatusV2(http.StatusBadRequest)
		h.respondError(w, http.StatusBadRequest, validationErr.Error())
		return
	}

	recordID := uuid.NewString()
	key := record.Source + "/" + recordID + ".json"

	if err := h.writeRawObject(r.Context(), key, body); err != nil {
		h.logger.Error("failed to write raw object", "error", err, "record_id", recordID, "correlation_id", correlationID)
		h.recordIngestStatusV2(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to persist raw object")
		return
	}

	response := ingest.IngestAccepted{RecordID: recordID, CorrelationID: correlationID}
	responseBytes, err := json.Marshal(response)
	if err != nil {
		h.recordIngestStatusV2(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to build response")
		return
	}

	event := buildRecordIngestedV2Event(record, recordID, correlationID, h.cfg.RawBucket, key, shaHex, int64(len(body)))
	eventBytes, err := json.Marshal(event)
	if err != nil {
		h.recordIngestStatusV2(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to build event")
		return
	}

	if err := h.insertOutboxEventTx(r.Context(), tx, h.cfg.TopicRecordIngestedV2, recordID, eventBytes); err != nil {
		h.logger.Error("failed to persist outbox event", "error", err, "record_id", recordID, "correlation_id", correlationID)
		h.recordIngestStatusV2(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to persist outbox event")
		return
	}

	if err := h.storeIdempotencyTx(r.Context(), tx, idempotencyKey, shaHex, responseBytes); err != nil {
		h.logger.Error("failed to store idempotency", "error", err, "record_id", recordID, "correlation_id", correlationID)
		h.recordIngestStatusV2(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to persist idempotency")
		return
	}

	if err := tx.Commit(); err != nil {
		h.logger.Error("failed to commit ingest transaction", "error", err, "record_id", recordID, "correlation_id", correlationID)
		h.recordIngestStatusV2(http.StatusInternalServerError)
		h.respondError(w, http.StatusInternalServerError, "failed to process request")
		return
	}

	h.logger.Info("record ingested",
		"record_id", recordID,
		"correlation_id", correlationID,
		"source", record.Source,
		"size_bytes", len(body),
	)
	h.recordIngestStatusV2(http.StatusAccepted)
	writeJSONBytes(w, http.StatusAccepted, responseBytes)
}

func validateRecord(record ingest.PartnerRecordV1) error {
	switch {
	case len(record.Source) < minSourceLength:
		return errors.New("missing source")
	case len(record.RecordType) < minRecordType:
		return errors.New("missing record_type")
	case record.RecordType != "encounter" && record.RecordType != "claim" && record.RecordType != "lab_result":
		return errors.New("invalid record_type")
	case len(record.Patient.FirstName) < minPatientName:
		return errors.New("missing patient.first_name")
	case len(record.Patient.LastName) < minPatientName:
		return errors.New("missing patient.last_name")
	case len(record.Patient.DOB) < minPatientDOB:
		return errors.New("missing patient.dob")
	case record.Payload == nil:
		return errors.New("missing payload")
	default:
		return nil
	}
}

func validateRecordV2(record ingest.PartnerRecordV2) error {
	switch {
	case len(record.Source) < minSourceLength:
		return errors.New("missing source")
	case len(record.RecordKind) < minRecordType:
		return errors.New("missing record_kind")
	case record.RecordKind != "encounter" && record.RecordKind != "claim" && record.RecordKind != "lab_result":
		return errors.New("invalid record_kind")
	case len(record.SchemaHint) < minRecordType:
		return errors.New("missing schema_hint")
	case len(record.Patient.GivenName) < minPatientName:
		return errors.New("missing patient.given_name")
	case len(record.Patient.FamilyName) < minPatientName:
		return errors.New("missing patient.family_name")
	case len(record.Patient.DOB) < minPatientDOB:
		return errors.New("missing patient.dob")
	case record.Payload == nil:
		return errors.New("missing payload")
	default:
		return nil
	}
}

func (h *Handler) writeRawObject(ctx context.Context, key string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, storageTimeout)
	defer cancel()

	reader := bytes.NewReader(body)
	_, err := h.minio.PutObject(
		ctx,
		h.cfg.RawBucket,
		key,
		reader,
		int64(reader.Len()),
		minio.PutObjectOptions{ContentType: "application/json"},
	)
	return err
}

func buildRecordIngestedEvent(record ingest.PartnerRecordV1, recordID, correlationID, bucket, key, sha string, sizeBytes int64) ingest.RecordIngestedV1Event {
	return ingest.RecordIngestedV1Event{
		EventVersion:  "1",
		EventType:     "record.ingested",
		EventID:       uuid.NewString(),
		OccurredAt:    time.Now().UTC(),
		CorrelationID: correlationID,
		Source:        record.Source,
		RecordID:      recordID,
		RawObject: ingest.RecordIngestedV1Raw{
			Bucket:    bucket,
			Key:       key,
			SHA256:    sha,
			SizeBytes: sizeBytes,
		},
	}
}

func buildRecordIngestedV2Event(record ingest.PartnerRecordV2, recordID, correlationID, bucket, key, sha string, sizeBytes int64) ingest.RecordIngestedV2Event {
	return ingest.RecordIngestedV2Event{
		EventVersion:  "2",
		EventType:     "record.ingested",
		EventID:       uuid.NewString(),
		OccurredAt:    time.Now().UTC(),
		CorrelationID: correlationID,
		Source:        record.Source,
		RecordID:      recordID,
		RecordKind:    record.RecordKind,
		SchemaHint:    record.SchemaHint,
		RawObject: ingest.RecordIngestedV2Raw{
			Bucket:    bucket,
			Key:       key,
			SHA256:    sha,
			SizeBytes: sizeBytes,
		},
	}
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	payload, err := json.Marshal(body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	writeJSONBytes(w, status, payload)
}

func writeJSONBytes(w http.ResponseWriter, status int, payload []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

func (h *Handler) respondError(w http.ResponseWriter, status int, message string) {
	response := map[string]string{"error": message}
	writeJSON(w, status, response)
}

func (h *Handler) recordIngestStatus(status int) {
	metrics.IngestRequests.WithLabelValues("/v1/ingest", strconv.Itoa(status)).Inc()
}

func (h *Handler) recordIngestStatusV2(status int) {
	metrics.IngestRequests.WithLabelValues("/v2/ingest", strconv.Itoa(status)).Inc()
}

func (h *Handler) lockIdempotencyKey(ctx context.Context, tx *sql.Tx, key string) error {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	_, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", key)
	return err
}

func (h *Handler) checkIdempotencyTx(ctx context.Context, tx *sql.Tx, key, requestSHA string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	var storedSHA string
	var response []byte
	err := tx.QueryRowContext(ctx,
		"SELECT request_sha256, response FROM idempotency_keys WHERE key=$1",
		key,
	).Scan(&storedSHA, &response)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if storedSHA != requestSHA {
		return nil, true, nil
	}
	return response, false, nil
}

func (h *Handler) insertOutboxEventTx(ctx context.Context, tx *sql.Tx, topic, key string, payload []byte) error {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	_, err := tx.ExecContext(ctx,
		`INSERT INTO outbox_events (topic, key, payload)
		 VALUES ($1, $2, $3::jsonb)`,
		topic,
		key,
		payload,
	)
	return err
}

func (h *Handler) storeIdempotencyTx(ctx context.Context, tx *sql.Tx, key, requestSHA string, response []byte) error {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	_, err := tx.ExecContext(ctx,
		`INSERT INTO idempotency_keys (key, request_sha256, response)
		 VALUES ($1, $2, $3::jsonb)`,
		key,
		requestSHA,
		response,
	)
	if err != nil {
		return err
	}

	return nil
}
