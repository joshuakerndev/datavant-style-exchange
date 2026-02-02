package ingest

import "time"

type PartnerRecordV1 struct {
	Source     string                 `json:"source"`
	RecordType string                 `json:"record_type"`
	Patient    PartnerPatientV1       `json:"patient"`
	Payload    map[string]interface{} `json:"payload"`
}

type PartnerRecordV2 struct {
	Source     string                 `json:"source"`
	RecordKind string                 `json:"record_kind"`
	SchemaHint string                 `json:"schema_hint"`
	Patient    PartnerPatientV2       `json:"patient"`
	Payload    map[string]interface{} `json:"payload"`
}

type PartnerPatientV1 struct {
	FirstName string  `json:"first_name"`
	LastName  string  `json:"last_name"`
	DOB       string  `json:"dob"`
	SSN       *string `json:"ssn,omitempty"`
}

type PartnerPatientV2 struct {
	GivenName  string  `json:"given_name"`
	FamilyName string  `json:"family_name"`
	DOB        string  `json:"dob"`
	SSN        *string `json:"ssn,omitempty"`
}

type RecordIngestedV1Event struct {
	EventVersion  string              `json:"event_version"`
	EventType     string              `json:"event_type"`
	EventID       string              `json:"event_id"`
	OccurredAt    time.Time           `json:"occurred_at"`
	CorrelationID string              `json:"correlation_id"`
	Source        string              `json:"source"`
	RecordID      string              `json:"record_id"`
	RawObject     RecordIngestedV1Raw `json:"raw_object"`
}

type RecordIngestedV2Event struct {
	EventVersion  string              `json:"event_version"`
	EventType     string              `json:"event_type"`
	EventID       string              `json:"event_id"`
	OccurredAt    time.Time           `json:"occurred_at"`
	CorrelationID string              `json:"correlation_id"`
	Source        string              `json:"source"`
	RecordID      string              `json:"record_id"`
	RecordKind    string              `json:"record_kind"`
	SchemaHint    string              `json:"schema_hint"`
	RawObject     RecordIngestedV2Raw `json:"raw_object"`
}

type RecordIngestedV1Raw struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

type RecordIngestedV2Raw struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

type IngestAccepted struct {
	RecordID      string `json:"record_id"`
	CorrelationID string `json:"correlation_id"`
}
