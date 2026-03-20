package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func dummyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
}

func TestAuth_MissingHeader_Returns401(t *testing.T) {
	keys := []APIKey{{Name: "test", Key: "grnc_sk_test123"}}
	handler := AuthMiddleware(keys, dummyHandler())

	req := httptest.NewRequest("GET", "/api/v1/trigger/foo", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}

func TestAuth_WrongKey_Returns403(t *testing.T) {
	keys := []APIKey{{Name: "test", Key: "grnc_sk_test123"}}
	handler := AuthMiddleware(keys, dummyHandler())

	req := httptest.NewRequest("GET", "/api/v1/trigger/foo", nil)
	req.Header.Set("Authorization", "Bearer wrong_key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("expected 403, got %d", w.Code)
	}
}

func TestAuth_ValidKey_PassesThrough(t *testing.T) {
	keys := []APIKey{{Name: "test", Key: "grnc_sk_test123"}}
	handler := AuthMiddleware(keys, dummyHandler())

	req := httptest.NewRequest("GET", "/api/v1/trigger/foo", nil)
	req.Header.Set("Authorization", "Bearer grnc_sk_test123")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestAuth_HealthExempt(t *testing.T) {
	keys := []APIKey{{Name: "test", Key: "grnc_sk_test123"}}
	handler := AuthMiddleware(keys, dummyHandler())

	req := httptest.NewRequest("GET", "/api/v1/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 without auth, got %d", w.Code)
	}
}

func TestAuth_XAPIKeyOnly_Returns401(t *testing.T) {
	keys := []APIKey{{Name: "test", Key: "grnc_sk_test123"}}
	handler := AuthMiddleware(keys, dummyHandler())

	req := httptest.NewRequest("GET", "/api/v1/trigger/foo", nil)
	req.Header.Set("X-API-Key", "grnc_sk_test123")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for X-API-Key-only request, got %d", w.Code)
	}
}

func TestAuth_NoKeysConfigured_PassesThrough(t *testing.T) {
	handler := AuthMiddleware(nil, dummyHandler())

	req := httptest.NewRequest("GET", "/api/v1/trigger/foo", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 when no keys, got %d", w.Code)
	}
}

func TestIsCloudMode_Local(t *testing.T) {
	t.Setenv("GRANICUS_STATE_BACKEND", "")
	t.Setenv("GRANICUS_PIPELINE_SOURCE", "")

	if IsCloudMode() {
		t.Error("expected false with no env vars set")
	}
}

func TestIsCloudMode_Firestore(t *testing.T) {
	t.Setenv("GRANICUS_STATE_BACKEND", "firestore")
	t.Setenv("GRANICUS_PIPELINE_SOURCE", "")

	if !IsCloudMode() {
		t.Error("expected true when GRANICUS_STATE_BACKEND=firestore")
	}
}

func TestIsCloudMode_GCS(t *testing.T) {
	t.Setenv("GRANICUS_STATE_BACKEND", "")
	t.Setenv("GRANICUS_PIPELINE_SOURCE", "gcs")

	if !IsCloudMode() {
		t.Error("expected true when GRANICUS_PIPELINE_SOURCE=gcs")
	}
}

func TestValidateAuth_LocalNoKeys(t *testing.T) {
	t.Setenv("GRANICUS_STATE_BACKEND", "")
	t.Setenv("GRANICUS_PIPELINE_SOURCE", "")

	if err := ValidateAuth(nil); err != nil {
		t.Errorf("expected nil in local mode with no keys, got %v", err)
	}
}

func TestValidateAuth_CloudNoKeys(t *testing.T) {
	t.Setenv("GRANICUS_STATE_BACKEND", "firestore")
	t.Setenv("GRANICUS_REQUIRE_AUTH", "")

	if err := ValidateAuth(nil); err == nil {
		t.Error("expected error in cloud mode with no keys")
	}
}

func TestValidateAuth_CloudWithKeys(t *testing.T) {
	t.Setenv("GRANICUS_STATE_BACKEND", "firestore")
	t.Setenv("GRANICUS_REQUIRE_AUTH", "")

	keys := []APIKey{{Name: "prod", Key: "grnc_sk_prod123"}}
	if err := ValidateAuth(keys); err != nil {
		t.Errorf("expected nil in cloud mode with keys, got %v", err)
	}
}

func TestValidateAuth_CloudOverride(t *testing.T) {
	t.Setenv("GRANICUS_STATE_BACKEND", "firestore")
	t.Setenv("GRANICUS_REQUIRE_AUTH", "false")

	if err := ValidateAuth(nil); err != nil {
		t.Errorf("expected nil when GRANICUS_REQUIRE_AUTH=false, got %v", err)
	}
}
