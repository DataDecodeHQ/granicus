package server

import (
	"crypto/subtle"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
)

type APIKey struct {
	Name string `yaml:"name" json:"name"`
	Key  string `yaml:"key" json:"key"`
}

// IsCloudMode returns true when the server is running with cloud backends.
func IsCloudMode() bool {
	stateBackend := os.Getenv("GRANICUS_STATE_BACKEND")
	pipelineSource := os.Getenv("GRANICUS_PIPELINE_SOURCE")
	cloudValues := map[string]bool{"firestore": true, "gcs": true}
	return cloudValues[stateBackend] || cloudValues[pipelineSource]
}

// ValidateAuth checks that API keys are configured when running in cloud mode.
// Returns an error if cloud mode is detected and no keys are provided,
// unless GRANICUS_REQUIRE_AUTH=false is set as an explicit override.
func ValidateAuth(keys []APIKey) error {
	if !IsCloudMode() {
		if len(keys) == 0 {
			slog.Warn("auth_validation", "mode", "local", "status", "no_api_keys", "note", "auth disabled in local mode")
		}
		return nil
	}

	// Cloud mode: check for override
	if strings.EqualFold(os.Getenv("GRANICUS_REQUIRE_AUTH"), "false") {
		slog.Warn("auth_validation", "mode", "cloud", "status", "auth_overridden", "note", "GRANICUS_REQUIRE_AUTH=false -- running without auth in cloud mode")
		return nil
	}

	if len(keys) == 0 {
		return fmt.Errorf("API keys required in cloud mode; configure api_keys in server config or set GRANICUS_REQUIRE_AUTH=false to override")
	}

	slog.Info("auth_validation", "mode", "cloud", "status", "ok", "key_count", len(keys))
	return nil
}

// AuthMiddleware returns an HTTP handler that validates Bearer token authentication against configured API keys.
func AuthMiddleware(keys []APIKey, next http.Handler) http.Handler {
	if len(keys) == 0 {
		slog.Warn("auth_disabled", "reason", "no_api_keys_configured", "note", "all endpoints are unauthenticated")
	}
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

		// Check X-API-Key header first (used when Authorization carries an IAM identity token)
		token := r.Header.Get("X-API-Key")

		// Fall back to Authorization: Bearer <key>
		if token == "" {
			auth := r.Header.Get("Authorization")
			if auth == "" {
				writeJSON(w, http.StatusUnauthorized, ErrorResponse{Error: "authorization required"})
				return
			}
			token = strings.TrimPrefix(auth, "Bearer ")
			if token == auth {
				writeJSON(w, http.StatusUnauthorized, ErrorResponse{Error: "invalid authorization format"})
				return
			}
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
