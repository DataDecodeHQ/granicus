package logging

import (
	"log/slog"
	"testing"
)

func TestInit_ServerMode(t *testing.T) {
	Init(true)
	handler := slog.Default().Handler()
	if _, ok := handler.(*slog.JSONHandler); !ok {
		t.Errorf("expected *slog.JSONHandler, got %T", handler)
	}
}

func TestInit_LocalMode(t *testing.T) {
	Init(false)
	handler := slog.Default().Handler()
	if _, ok := handler.(*slog.TextHandler); !ok {
		t.Errorf("expected *slog.TextHandler, got %T", handler)
	}
}

func TestInit_Idempotent(t *testing.T) {
	Init(true)
	Init(true)
	Init(false)
}
