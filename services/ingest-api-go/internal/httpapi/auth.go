package httpapi

import (
	"errors"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"datavant-style-exchange/services/ingest-api-go/internal/config"
)

func AuthMiddleware(cfg config.Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cfg.Env == "local" && strings.TrimSpace(r.Header.Get("Authorization")) == "Bearer dev" {
				next.ServeHTTP(w, r)
				return
			}

			token, err := parseBearer(r.Header.Get("Authorization"))
			if err != nil {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}

			claims := &jwt.RegisteredClaims{}
			parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (interface{}, error) {
				if t.Method != jwt.SigningMethodHS256 {
					return nil, errors.New("unexpected signing method")
				}
				return []byte(cfg.JwtSecret), nil
			})
			if err != nil || parsed == nil || !parsed.Valid {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
			if err := validateClaims(claims, cfg); err != nil {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func parseBearer(value string) (string, error) {
	if value == "" {
		return "", errors.New("missing auth")
	}
	parts := strings.SplitN(value, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return "", errors.New("invalid auth")
	}
	if parts[1] == "" {
		return "", errors.New("missing token")
	}
	return parts[1], nil
}

func validateClaims(claims *jwt.RegisteredClaims, cfg config.Config) error {
	if claims == nil {
		return errors.New("missing claims")
	}
	if claims.Issuer != cfg.JwtIssuer {
		return errors.New("invalid issuer")
	}
	found := false
	for _, aud := range claims.Audience {
		if aud == cfg.JwtAudience {
			found = true
			break
		}
	}
	if !found {
		return errors.New("invalid audience")
	}
	return nil
}
