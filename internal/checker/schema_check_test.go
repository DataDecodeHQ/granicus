package checker

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/DataDecodeHQ/granicus/internal/events"
)

func newSchemaTestStore(t *testing.T) *events.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "events.db")
	s, err := events.New(dbPath)
	if err != nil {
		t.Fatalf("creating store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCheckSchemaStability_FirstRun(t *testing.T) {
	store := newSchemaTestStore(t)
	schema := []SchemaColumn{
		{Name: "id", DataType: "INT64"},
		{Name: "name", DataType: "STRING"},
	}

	diff, err := CheckSchemaStability(store, "pipe", "asset1", "run_001", schema, SchemaCheckWarn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff == nil {
		t.Fatal("expected non-nil diff on first run")
	}
	if len(diff.Changes) != 0 {
		t.Errorf("first run should have no changes, got %d: %v", len(diff.Changes), diff.Changes)
	}
}

func TestCheckSchemaStability_ColumnAdded(t *testing.T) {
	store := newSchemaTestStore(t)

	initial := []SchemaColumn{
		{Name: "id", DataType: "INT64"},
		{Name: "status", DataType: "STRING"},
	}
	_, err := CheckSchemaStability(store, "pipe", "asset1", "run_001", initial, SchemaCheckWarn)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}

	updated := []SchemaColumn{
		{Name: "id", DataType: "INT64"},
		{Name: "status", DataType: "STRING"},
		{Name: "created_at", DataType: "TIMESTAMP"},
	}
	diff, err := CheckSchemaStability(store, "pipe", "asset1", "run_002", updated, SchemaCheckWarn)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if len(diff.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d: %v", len(diff.Changes), diff.Changes)
	}
	if diff.Changes[0].ChangeType != "column_added" {
		t.Errorf("expected column_added, got %s", diff.Changes[0].ChangeType)
	}
	if diff.Changes[0].Column != "created_at" {
		t.Errorf("expected column created_at, got %s", diff.Changes[0].Column)
	}
	if diff.Changes[0].NewType != "TIMESTAMP" {
		t.Errorf("expected NewType TIMESTAMP, got %s", diff.Changes[0].NewType)
	}
}

func TestCheckSchemaStability_ColumnRemoved(t *testing.T) {
	store := newSchemaTestStore(t)

	initial := []SchemaColumn{
		{Name: "id", DataType: "INT64"},
		{Name: "status", DataType: "STRING"},
		{Name: "deprecated_col", DataType: "STRING"},
	}
	_, err := CheckSchemaStability(store, "pipe", "asset1", "run_001", initial, SchemaCheckWarn)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}

	updated := []SchemaColumn{
		{Name: "id", DataType: "INT64"},
		{Name: "status", DataType: "STRING"},
	}
	diff, err := CheckSchemaStability(store, "pipe", "asset1", "run_002", updated, SchemaCheckWarn)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if len(diff.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d: %v", len(diff.Changes), diff.Changes)
	}
	if diff.Changes[0].ChangeType != "column_removed" {
		t.Errorf("expected column_removed, got %s", diff.Changes[0].ChangeType)
	}
	if diff.Changes[0].Column != "deprecated_col" {
		t.Errorf("expected column deprecated_col, got %s", diff.Changes[0].Column)
	}
	if diff.Changes[0].OldType != "STRING" {
		t.Errorf("expected OldType STRING, got %s", diff.Changes[0].OldType)
	}
}

func TestCheckSchemaStability_TypeChanged(t *testing.T) {
	store := newSchemaTestStore(t)

	initial := []SchemaColumn{
		{Name: "id", DataType: "INT64"},
		{Name: "amount", DataType: "FLOAT64"},
	}
	_, err := CheckSchemaStability(store, "pipe", "asset1", "run_001", initial, SchemaCheckWarn)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}

	updated := []SchemaColumn{
		{Name: "id", DataType: "INT64"},
		{Name: "amount", DataType: "NUMERIC"},
	}
	diff, err := CheckSchemaStability(store, "pipe", "asset1", "run_002", updated, SchemaCheckWarn)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if len(diff.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d: %v", len(diff.Changes), diff.Changes)
	}
	if diff.Changes[0].ChangeType != "type_changed" {
		t.Errorf("expected type_changed, got %s", diff.Changes[0].ChangeType)
	}
	if diff.Changes[0].Column != "amount" {
		t.Errorf("expected column amount, got %s", diff.Changes[0].Column)
	}
	if diff.Changes[0].OldType != "FLOAT64" {
		t.Errorf("expected OldType FLOAT64, got %s", diff.Changes[0].OldType)
	}
	if diff.Changes[0].NewType != "NUMERIC" {
		t.Errorf("expected NewType NUMERIC, got %s", diff.Changes[0].NewType)
	}
}

func TestCheckSchemaStability_NoChanges(t *testing.T) {
	store := newSchemaTestStore(t)

	schema := []SchemaColumn{
		{Name: "id", DataType: "INT64"},
		{Name: "name", DataType: "STRING"},
	}
	_, _ = CheckSchemaStability(store, "pipe", "asset1", "run_001", schema, SchemaCheckWarn)

	diff, err := CheckSchemaStability(store, "pipe", "asset1", "run_002", schema, SchemaCheckWarn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(diff.Changes) != 0 {
		t.Errorf("expected no changes, got %d: %v", len(diff.Changes), diff.Changes)
	}
}

func TestCheckSchemaStability_ErrorResponse_BlocksOnChange(t *testing.T) {
	store := newSchemaTestStore(t)

	initial := []SchemaColumn{{Name: "id", DataType: "INT64"}}
	_, _ = CheckSchemaStability(store, "pipe", "asset1", "run_001", initial, SchemaCheckError)

	updated := []SchemaColumn{
		{Name: "id", DataType: "INT64"},
		{Name: "extra", DataType: "STRING"},
	}
	diff, err := CheckSchemaStability(store, "pipe", "asset1", "run_002", updated, SchemaCheckError)
	if err == nil {
		t.Error("expected error when response=error and schema changed")
	}
	if diff == nil || len(diff.Changes) == 0 {
		t.Error("expected non-empty diff alongside error")
	}
}

func TestCheckSchemaStability_ErrorResponse_NoError_WhenStable(t *testing.T) {
	store := newSchemaTestStore(t)

	schema := []SchemaColumn{{Name: "id", DataType: "INT64"}}
	_, _ = CheckSchemaStability(store, "pipe", "asset1", "run_001", schema, SchemaCheckError)

	_, err := CheckSchemaStability(store, "pipe", "asset1", "run_002", schema, SchemaCheckError)
	if err != nil {
		t.Errorf("expected no error when schema stable with error response, got: %v", err)
	}
}

func TestCheckSchemaStability_IgnoreResponse(t *testing.T) {
	store := newSchemaTestStore(t)

	initial := []SchemaColumn{{Name: "id", DataType: "INT64"}}
	_, _ = CheckSchemaStability(store, "pipe", "asset1", "run_001", initial, SchemaCheckWarn)

	updated := []SchemaColumn{
		{Name: "id", DataType: "INT64"},
		{Name: "new_col", DataType: "STRING"},
	}
	diff, err := CheckSchemaStability(store, "pipe", "asset1", "run_002", updated, SchemaCheckIgnore)
	if err != nil {
		t.Errorf("ignore response should not return error: %v", err)
	}
	if diff != nil {
		t.Errorf("ignore response should return nil diff, got %v", diff)
	}
}

func TestCheckSchemaStability_EmitsSchemaChangedEvent(t *testing.T) {
	store := newSchemaTestStore(t)

	initial := []SchemaColumn{{Name: "id", DataType: "INT64"}}
	_, _ = CheckSchemaStability(store, "pipe", "asset1", "run_001", initial, SchemaCheckWarn)

	updated := []SchemaColumn{
		{Name: "id", DataType: "INT64"},
		{Name: "new_col", DataType: "STRING"},
	}
	_, _ = CheckSchemaStability(store, "pipe", "asset1", "run_002", updated, SchemaCheckWarn)

	evts, err := store.Query(events.QueryFilters{
		Asset:     "asset1",
		EventType: "schema_changed",
	})
	if err != nil {
		t.Fatalf("querying events: %v", err)
	}
	if len(evts) != 1 {
		t.Fatalf("expected 1 schema_changed event, got %d", len(evts))
	}
	if !strings.Contains(evts[0].Summary, "asset1") {
		t.Errorf("summary should mention asset name: %s", evts[0].Summary)
	}
	if evts[0].Severity != "warning" {
		t.Errorf("expected severity warning, got %s", evts[0].Severity)
	}
}

func TestCheckSchemaStability_ErrorResponseEmitsErrorSeverity(t *testing.T) {
	store := newSchemaTestStore(t)

	initial := []SchemaColumn{{Name: "id", DataType: "INT64"}}
	_, _ = CheckSchemaStability(store, "pipe", "asset1", "run_001", initial, SchemaCheckError)

	updated := []SchemaColumn{{Name: "id", DataType: "INT64"}, {Name: "x", DataType: "BOOL"}}
	_, _ = CheckSchemaStability(store, "pipe", "asset1", "run_002", updated, SchemaCheckError)

	evts, _ := store.Query(events.QueryFilters{Asset: "asset1", EventType: "schema_changed"})
	if len(evts) != 1 {
		t.Fatalf("expected 1 schema_changed event, got %d", len(evts))
	}
	if evts[0].Severity != "error" {
		t.Errorf("expected severity error, got %s", evts[0].Severity)
	}
}

func TestCheckSchemaStability_MultipleRuns_SnapshotProgresses(t *testing.T) {
	store := newSchemaTestStore(t)

	v1 := []SchemaColumn{{Name: "id", DataType: "INT64"}}
	v2 := []SchemaColumn{{Name: "id", DataType: "INT64"}, {Name: "col_b", DataType: "STRING"}}
	v3 := []SchemaColumn{{Name: "id", DataType: "INT64"}, {Name: "col_b", DataType: "STRING"}, {Name: "col_c", DataType: "DATE"}}

	_, _ = CheckSchemaStability(store, "pipe", "asset1", "run_001", v1, SchemaCheckWarn)

	diff2, _ := CheckSchemaStability(store, "pipe", "asset1", "run_002", v2, SchemaCheckWarn)
	if len(diff2.Changes) != 1 || diff2.Changes[0].Column != "col_b" {
		t.Errorf("run 2: expected 1 change (col_b added), got %v", diff2.Changes)
	}

	diff3, _ := CheckSchemaStability(store, "pipe", "asset1", "run_003", v3, SchemaCheckWarn)
	if len(diff3.Changes) != 1 || diff3.Changes[0].Column != "col_c" {
		t.Errorf("run 3: expected 1 change (col_c added), got %v", diff3.Changes)
	}
}

func TestComputeSchemaDiff_AllChangeTypes(t *testing.T) {
	prev := []SchemaColumn{
		{Name: "id", DataType: "INT64"},
		{Name: "old_col", DataType: "STRING"},
		{Name: "amount", DataType: "FLOAT64"},
	}
	current := []SchemaColumn{
		{Name: "id", DataType: "INT64"},
		{Name: "amount", DataType: "NUMERIC"},
		{Name: "new_col", DataType: "BOOL"},
	}

	diff := computeSchemaDiff("tbl", prev, current)

	if diff.Asset != "tbl" {
		t.Errorf("expected asset tbl, got %s", diff.Asset)
	}
	if len(diff.Changes) != 3 {
		t.Fatalf("expected 3 changes, got %d: %v", len(diff.Changes), diff.Changes)
	}

	byType := make(map[string]SchemaChange)
	for _, c := range diff.Changes {
		byType[c.ChangeType] = c
	}

	removed, ok := byType["column_removed"]
	if !ok {
		t.Error("expected column_removed change")
	} else if removed.Column != "old_col" {
		t.Errorf("removed column: expected old_col, got %s", removed.Column)
	}

	added, ok := byType["column_added"]
	if !ok {
		t.Error("expected column_added change")
	} else if added.Column != "new_col" {
		t.Errorf("added column: expected new_col, got %s", added.Column)
	}

	changed, ok := byType["type_changed"]
	if !ok {
		t.Error("expected type_changed change")
	} else {
		if changed.Column != "amount" {
			t.Errorf("type_changed column: expected amount, got %s", changed.Column)
		}
		if changed.OldType != "FLOAT64" || changed.NewType != "NUMERIC" {
			t.Errorf("type_changed types: expected FLOAT64->NUMERIC, got %s->%s", changed.OldType, changed.NewType)
		}
	}
}
