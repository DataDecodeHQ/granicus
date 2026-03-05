package server

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
)

type APIKey struct {
	Name string `yaml:"name" json:"name"`
	Key  string `yaml:"key" json:"key"`
}

func AuthMiddleware(keys []APIKey, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Health endpoint is exempt from auth
		if r.URL.Path == "/api/v1/health" {
			next.ServeHTTP(w, r)
			return
		}

		// No keys configured = auth disabled
		if len(keys) == 0 {
			next.ServeHTTP(w, r)
			return
		}

		auth := r.Header.Get("Authorization")
		if auth == "" {
			writeJSON(w, http.StatusUnauthorized, ErrorResponse{Error: "authorization required"})
			return
		}

		token := strings.TrimPrefix(auth, "Bearer ")
		if token == auth {
			writeJSON(w, http.StatusUnauthorized, ErrorResponse{Error: "invalid authorization format"})
			return
		}

		for _, k := range keys {
			if subtle.ConstantTimeCompare([]byte(token), []byte(k.Key)) == 1 {
				slog.Info("auth request", "key", k.Name)
				next.ServeHTTP(w, r)
				return
			}
		}

		writeJSON(w, http.StatusForbidden, ErrorResponse{Error: "invalid API key"})
	})
}
