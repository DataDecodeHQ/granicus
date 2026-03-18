package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/DataDecodeHQ/granicus/internal/state"
)

// initStateBackend creates the appropriate StateBackend based on env vars.
// GRANICUS_STATE_BACKEND=sqlite (default) or firestore.
func initStateBackend(projectRoot, pipeline, envName string) (state.StateBackend, error) {
	backend := os.Getenv("GRANICUS_STATE_BACKEND")

	switch backend {
	case "firestore":
		ctx := context.Background()
		return state.NewFirestoreStateBackend(ctx, "", pipeline)

	case "sqlite", "":
		stateDBPath := filepath.Join(projectRoot, ".granicus", "state.db")
		return state.New(stateDBPath)

	default:
		return nil, fmt.Errorf("unknown state backend: %s (use sqlite or firestore)", backend)
	}
}
