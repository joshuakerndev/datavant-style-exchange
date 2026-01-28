package config

import (
	"fmt"
	"os"
)

type Config struct {
	Addr                 string
	Env                  string
	JwtIssuer            string
	JwtAudience          string
	JwtSecret            string
	TokenizerHmacSecret  string
}

func Load() (Config, error) {
	cfg := Config{
		Addr:                getEnvDefault("TOKENIZER_ADDR", ":8081"),
		Env:                 getEnvDefault("ENV", "local"),
		JwtIssuer:           os.Getenv("JWT_ISSUER"),
		JwtAudience:         os.Getenv("JWT_AUDIENCE"),
		JwtSecret:           os.Getenv("JWT_SECRET"),
		TokenizerHmacSecret: os.Getenv("TOKENIZER_HMAC_SECRET"),
	}

	if cfg.JwtIssuer == "" || cfg.JwtAudience == "" || cfg.JwtSecret == "" {
		return Config{}, fmt.Errorf("missing JWT_ISSUER, JWT_AUDIENCE, or JWT_SECRET")
	}
	if cfg.TokenizerHmacSecret == "" {
		return Config{}, fmt.Errorf("missing TOKENIZER_HMAC_SECRET")
	}

	return cfg, nil
}

func getEnvDefault(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}
