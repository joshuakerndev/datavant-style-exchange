package httpapi

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"datavant-style-exchange/services/tokenizer-go/internal/config"
)

const maxBodyBytes int64 = 1 << 20

const (
	dateLayout       = "2006-01-02"
	correlationMin   = 8
)

type Handler struct {
	cfg    config.Config
	logger *slog.Logger
}

type tokenizeRequest struct {
	GivenName  string  `json:"given_name"`
	FamilyName string  `json:"family_name"`
	DOB        string  `json:"dob"`
	SSN        *string `json:"ssn"`
}

type tokenizeResponse struct {
	PatientToken string `json:"patient_token"`
}

func New(cfg config.Config, logger *slog.Logger) *Handler {
	return &Handler{cfg: cfg, logger: logger}
}

func (h *Handler) Healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) Tokenize(w http.ResponseWriter, r *http.Request) {
	correlationID := strings.TrimSpace(r.Header.Get("X-Correlation-Id"))
	if len(correlationID) < correlationMin {
		correlationID = ""
	}

	bodyReader := http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer bodyReader.Close()

	payload, err := io.ReadAll(bodyReader)
	if err != nil {
		h.logRequest(correlationID, http.StatusBadRequest)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}

	var req tokenizeRequest
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		h.logRequest(correlationID, http.StatusBadRequest)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}

	givenName, familyName, dob, ssn, err := canonicalize(req)
	if err != nil {
		h.logRequest(correlationID, http.StatusBadRequest)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}

	input := buildTokenInput(givenName, familyName, dob, ssn)
	token := h.hmacToken(input)

	writeJSON(w, http.StatusOK, tokenizeResponse{PatientToken: token})
	h.logRequest(correlationID, http.StatusOK)
}

func canonicalize(req tokenizeRequest) (string, string, string, string, error) {
	given := canonicalizeName(req.GivenName)
	family := canonicalizeName(req.FamilyName)
	if given == "" || family == "" {
		return "", "", "", "", errors.New("missing name")
	}

	dob, err := canonicalizeDOB(req.DOB)
	if err != nil {
		return "", "", "", "", err
	}

	ssn := ""
	if req.SSN != nil {
		ssn = canonicalizeSSN(*req.SSN)
	}

	return given, family, dob, ssn, nil
}

func canonicalizeName(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	parts := strings.Fields(trimmed)
	return strings.ToLower(strings.Join(parts, " "))
}

func canonicalizeDOB(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", errors.New("missing dob")
	}
	parsed, err := time.Parse(dateLayout, trimmed)
	if err != nil {
		return "", err
	}
	return parsed.Format(dateLayout), nil
}

func canonicalizeSSN(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(trimmed))
	for _, r := range trimmed {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func buildTokenInput(givenName, familyName, dob, ssn string) string {
	return "given_name=" + givenName + "|family_name=" + familyName + "|dob=" + dob + "|ssn=" + ssn
}

func (h *Handler) hmacToken(input string) string {
	mac := hmac.New(sha256.New, []byte(h.cfg.TokenizerHmacSecret))
	_, _ = mac.Write([]byte(input))
	return hex.EncodeToString(mac.Sum(nil))
}

func (h *Handler) logRequest(correlationID string, status int) {
	if correlationID == "" {
		h.logger.Info("tokenize request", "status", status)
		return
	}
	h.logger.Info("tokenize request", "status", status, "correlation_id", correlationID)
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	payload, err := json.Marshal(body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}
