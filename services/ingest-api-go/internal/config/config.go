package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

type Config struct {
	Addr                  string
	Env                   string
	MinioEndpoint         string
	MinioSecure           bool
	MinioAccessKey        string
	MinioSecretKey        string
	RawBucket             string
	PostgresDSN           string
	KafkaBrokers          []string
	TopicRecordIngestedV1 string
	TopicRecordIngestedV2 string
	JwtIssuer             string
	JwtAudience           string
	JwtSecret             string
}

func Load() (Config, error) {
	cfg := Config{
		Addr:                  getEnvDefault("INGEST_ADDR", ":8080"),
		Env:                   getEnvDefault("ENV", "local"),
		MinioAccessKey:        os.Getenv("MINIO_ACCESS_KEY"),
		MinioSecretKey:        os.Getenv("MINIO_SECRET_KEY"),
		RawBucket:             os.Getenv("RAW_BUCKET"),
		PostgresDSN:           buildPostgresDSN(),
		TopicRecordIngestedV1: os.Getenv("TOPIC_RECORD_INGESTED_V1"),
		TopicRecordIngestedV2: os.Getenv("TOPIC_RECORD_INGESTED_V2"),
		JwtIssuer:             os.Getenv("JWT_ISSUER"),
		JwtAudience:           os.Getenv("JWT_AUDIENCE"),
		JwtSecret:             os.Getenv("JWT_SECRET"),
	}

	minioEndpoint := os.Getenv("MINIO_ENDPOINT")
	endpoint, secure, err := parseEndpoint(minioEndpoint)
	if err != nil {
		return Config{}, err
	}
	cfg.MinioEndpoint = endpoint
	cfg.MinioSecure = secure

	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	for _, broker := range brokers {
		trimmed := strings.TrimSpace(broker)
		if trimmed != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, trimmed)
		}
	}

	if cfg.MinioAccessKey == "" || cfg.MinioSecretKey == "" || cfg.RawBucket == "" {
		return Config{}, fmt.Errorf("missing MINIO_ACCESS_KEY, MINIO_SECRET_KEY, or RAW_BUCKET")
	}
	if cfg.PostgresDSN == "" {
		return Config{}, fmt.Errorf("missing POSTGRES configuration")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("missing KAFKA_BROKERS")
	}
	if cfg.TopicRecordIngestedV1 == "" {
		return Config{}, fmt.Errorf("missing TOPIC_RECORD_INGESTED_V1")
	}
	if cfg.TopicRecordIngestedV2 == "" {
		return Config{}, fmt.Errorf("missing TOPIC_RECORD_INGESTED_V2")
	}
	if cfg.JwtIssuer == "" || cfg.JwtAudience == "" || cfg.JwtSecret == "" {
		return Config{}, fmt.Errorf("missing JWT_ISSUER, JWT_AUDIENCE, or JWT_SECRET")
	}

	return cfg, nil
}

func buildPostgresDSN() string {
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	db := os.Getenv("POSTGRES_DB")
	user := os.Getenv("POSTGRES_USER")
	pass := os.Getenv("POSTGRES_PASSWORD")
	if host == "" || port == "" || db == "" || user == "" || pass == "" {
		return ""
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, db)
}

func parseEndpoint(raw string) (string, bool, error) {
	if raw == "" {
		return "", false, fmt.Errorf("missing MINIO_ENDPOINT")
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		parsed, err := url.Parse(raw)
		if err != nil {
			return "", false, fmt.Errorf("invalid MINIO_ENDPOINT: %w", err)
		}
		if parsed.Host == "" {
			return "", false, fmt.Errorf("invalid MINIO_ENDPOINT: missing host")
		}
		return parsed.Host, parsed.Scheme == "https", nil
	}
	return raw, false, nil
}

func getEnvDefault(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}
