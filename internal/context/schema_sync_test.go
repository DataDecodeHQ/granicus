package context

import (
	"testing"
)

func TestSyncSchemas_NilClient(t *testing.T) {
	// SyncSchemas with nil client and empty datasets should return nil without panic
	result := SyncSchemas(nil, nil)
	if result != nil {
		t.Errorf("expected nil, got %d rows", len(result))
	}
}

func TestSyncSchemas_EmptyDatasets(t *testing.T) {
	// SyncSchemas with empty dataset list should return nil
	result := SyncSchemas(nil, []string{})
	if result != nil {
		t.Errorf("expected nil, got %d rows", len(result))
	}
}
