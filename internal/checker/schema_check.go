package checker

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Andrew-DataDecode/Granicus/internal/events"
)

// SchemaColumn represents a single column in a table schema.
type SchemaColumn struct {
	Name     string `json:"name"`
	DataType string `json:"data_type"`
}

// SchemaChange describes a single schema change detected between runs.
type SchemaChange struct {
	ChangeType string `json:"change_type"` // "column_added", "column_removed", "type_changed"
	Column     string `json:"column"`
	OldType    string `json:"old_type,omitempty"`
	NewType    string `json:"new_type,omitempty"`
}

// SchemaDiff holds all schema changes detected for an asset between runs.
type SchemaDiff struct {
	Asset   string         `json:"asset"`
	Changes []SchemaChange `json:"changes"`
}

// Schema check response values (matches AssetConfig.SchemaCheck).
const (
	SchemaCheckWarn   = "warn"
	SchemaCheckError  = "error"
	SchemaCheckIgnore = "ignore"
)

// CheckSchemaStability compares currentSchema against the previous schema
// snapshot stored in the events store. It always stores the current schema
// as a new snapshot for future comparison.
//
// response must be "warn", "error", or "ignore" (empty defaults to "warn").
// Returns an error if response is "error" and changes are detected.
// Returns nil diff and nil error when response is "ignore".
func CheckSchemaStability(
	store *events.Store,
	pipeline, asset, runID string,
	currentSchema []SchemaColumn,
	response string,
) (*SchemaDiff, error) {
	if response == SchemaCheckIgnore {
		return nil, nil
	}
	if response == "" {
		response = SchemaCheckWarn
	}

	prev, found, err := loadLastSchemaSnapshot(store, asset)
	if err != nil {
		return nil, fmt.Errorf("loading schema snapshot for %s: %w", asset, err)
	}

	if err := storeSchemaSnapshot(store, pipeline, asset, runID, currentSchema); err != nil {
		return nil, fmt.Errorf("storing schema snapshot for %s: %w", asset, err)
	}

	if !found {
		return &SchemaDiff{Asset: asset}, nil
	}

	diff := computeSchemaDiff(asset, prev, currentSchema)
	if len(diff.Changes) == 0 {
		return diff, nil
	}

	severity := "warning"
	if response == SchemaCheckError {
		severity = "error"
	}

	if err := store.Emit(events.Event{
		RunID:     runID,
		Pipeline:  pipeline,
		Asset:     asset,
		EventType: "schema_changed",
		Severity:  severity,
		Timestamp: time.Now().UTC(),
		Summary:   fmt.Sprintf("Schema changed for %s: %d change(s)", asset, len(diff.Changes)),
		Details: map[string]any{
			"changes":      diff.Changes,
			"change_count": len(diff.Changes),
		},
	}); err != nil {
		return diff, fmt.Errorf("emitting schema_changed event: %w", err)
	}

	if response == SchemaCheckError {
		return diff, fmt.Errorf("schema changed for asset %s: %d change(s) detected", asset, len(diff.Changes))
	}
	return diff, nil
}

func loadLastSchemaSnapshot(store *events.Store, asset string) ([]SchemaColumn, bool, error) {
	evts, err := store.Query(events.QueryFilters{
		Asset:     asset,
		EventType: "schema_snapshot",
	})
	if err != nil {
		return nil, false, err
	}
	if len(evts) == 0 {
		return nil, false, nil
	}
	last := evts[len(evts)-1]
	cols, err := schemaColumnsFromDetails(last.Details)
	if err != nil {
		return nil, false, err
	}
	return cols, true, nil
}

func storeSchemaSnapshot(store *events.Store, pipeline, asset, runID string, schema []SchemaColumn) error {
	return store.Emit(events.Event{
		RunID:     runID,
		Pipeline:  pipeline,
		Asset:     asset,
		EventType: "schema_snapshot",
		Severity:  "info",
		Timestamp: time.Now().UTC(),
		Summary:   fmt.Sprintf("Schema snapshot for %s: %d columns", asset, len(schema)),
		Details: map[string]any{
			"columns": schema,
		},
	})
}

func computeSchemaDiff(asset string, prev, current []SchemaColumn) *SchemaDiff {
	diff := &SchemaDiff{Asset: asset}

	prevMap := make(map[string]string, len(prev))
	for _, c := range prev {
		prevMap[c.Name] = c.DataType
	}

	currMap := make(map[string]string, len(current))
	for _, c := range current {
		currMap[c.Name] = c.DataType
	}

	for _, c := range prev {
		if _, ok := currMap[c.Name]; !ok {
			diff.Changes = append(diff.Changes, SchemaChange{
				ChangeType: "column_removed",
				Column:     c.Name,
				OldType:    c.DataType,
			})
		}
	}

	for _, c := range current {
		oldType, existed := prevMap[c.Name]
		if !existed {
			diff.Changes = append(diff.Changes, SchemaChange{
				ChangeType: "column_added",
				Column:     c.Name,
				NewType:    c.DataType,
			})
		} else if oldType != c.DataType {
			diff.Changes = append(diff.Changes, SchemaChange{
				ChangeType: "type_changed",
				Column:     c.Name,
				OldType:    oldType,
				NewType:    c.DataType,
			})
		}
	}

	return diff
}

func schemaColumnsFromDetails(details map[string]any) ([]SchemaColumn, error) {
	raw, ok := details["columns"]
	if !ok {
		return nil, fmt.Errorf("schema snapshot missing 'columns' key")
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("marshaling columns: %w", err)
	}
	var cols []SchemaColumn
	if err := json.Unmarshal(data, &cols); err != nil {
		return nil, fmt.Errorf("unmarshaling columns: %w", err)
	}
	return cols, nil
}
