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
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/segmentio/kafka-go"

	"datavant-style-exchange/services/ingest-api-go/internal/config"
	"datavant-style-exchange/services/ingest-api-go/internal/ingest"
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
	publishTimeout                = 5 * time.Second
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
		h.respondError(w, http.StatusBadRequest, "invalid body")
		return
	}

	sha := sha256.Sum256(body)
	shaHex := hex.EncodeToString(sha[:])

	idempotentResponse, conflict, err := h.checkIdempotency(r.Context(), idempotencyKey, shaHex)
	if err != nil {
		h.logger.Error("failed to check idempotency", "error", err)
		h.respondError(w, http.StatusInternalServerError, "failed to process request")
		return
	}
	if conflict {
		h.respondError(w, http.StatusConflict, "idempotency conflict")
		return
	}
	if idempotentResponse != nil {
		writeJSONBytes(w, http.StatusAccepted, idempotentResponse)
		return
	}

	var record ingest.PartnerRecordV1
	if err := json.Unmarshal(body, &record); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid payload")
		return
	}
	if validationErr := validateRecord(record); validationErr != nil {
		h.respondError(w, http.StatusBadRequest, validationErr.Error())
		return
	}

	recordID := uuid.NewString()
	key := record.Source + "/" + recordID + ".json"

	if err := h.writeRawObject(r.Context(), key, body); err != nil {
		h.logger.Error("failed to write raw object", "error", err, "record_id", recordID, "correlation_id", correlationID)
		h.respondError(w, http.StatusInternalServerError, "failed to persist raw object")
		return
	}

	if err := h.publishEvent(r.Context(), record, recordID, correlationID, key, shaHex, int64(len(body))); err != nil {
		h.logger.Error("failed to publish event", "error", err, "record_id", recordID, "correlation_id", correlationID)
		h.respondError(w, http.StatusInternalServerError, "failed to publish event")
		return
	}

	h.logger.Info("record ingested",
		"record_id", recordID,
		"correlation_id", correlationID,
		"source", record.Source,
		"size_bytes", len(body),
	)

	response := ingest.IngestAccepted{RecordID: recordID, CorrelationID: correlationID}
	responseBytes, err := json.Marshal(response)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "failed to build response")
		return
	}
	if err := h.storeIdempotency(r.Context(), idempotencyKey, shaHex, responseBytes); err != nil {
		h.logger.Error("failed to store idempotency", "error", err, "record_id", recordID, "correlation_id", correlationID)
		h.respondError(w, http.StatusInternalServerError, "failed to persist idempotency")
		return
	}
	writeJSONBytes(w, http.StatusAccepted, responseBytes)
}

func validateRecord(record ingest.PartnerRecordV1) error {
	switch {
	case len(record.Source) < minSourceLength:
		return errors.New("missing source")
	case len(record.RecordType) < minRecordType:
		return errors.New("missing record_type")
	case record.RecordType != "encounter":
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

func (h *Handler) publishEvent(ctx context.Context, record ingest.PartnerRecordV1, recordID, correlationID, key, sha string, sizeBytes int64) error {
	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	event := ingest.RecordIngestedV1Event{
		EventVersion:  "1",
		EventType:     "record.ingested",
		EventID:       uuid.NewString(),
		OccurredAt:    time.Now().UTC(),
		CorrelationID: correlationID,
		Source:        record.Source,
		RecordID:      recordID,
		RawObject: ingest.RecordIngestedV1Raw{
			Bucket:    h.cfg.RawBucket,
			Key:       key,
			SHA256:    sha,
			SizeBytes: sizeBytes,
		},
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return h.kafkaWrite.WriteMessages(ctx, kafka.Message{
		Key:   []byte(recordID),
		Value: payload,
		Time:  event.OccurredAt,
	})
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

func (h *Handler) checkIdempotency(ctx context.Context, key, requestSHA string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	var storedSHA string
	var response []byte
	err := h.db.QueryRowContext(ctx,
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

func (h *Handler) storeIdempotency(ctx context.Context, key, requestSHA string, response []byte) error {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	var insertedKey string
	err := h.db.QueryRowContext(ctx,
		`INSERT INTO idempotency_keys (key, request_sha256, response)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (key) DO NOTHING
		 RETURNING key`,
		key,
		requestSHA,
		string(response),
	).Scan(&insertedKey)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}

	return nil
}
