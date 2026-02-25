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

func TestAuth_NoKeysConfigured_PassesThrough(t *testing.T) {
	handler := AuthMiddleware(nil, dummyHandler())

	req := httptest.NewRequest("GET", "/api/v1/trigger/foo", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 when no keys, got %d", w.Code)
	}
}
