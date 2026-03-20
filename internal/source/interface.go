package source

import (
	"context"
	"time"
)

// PipelineSource abstracts how pipeline files are obtained. LocalSource reads
// from a directory on disk (current behavior). GCSVersionedSource fetches
// versioned archives from GCS (cloud deployment).
type PipelineSource interface {
	// Fetch retrieves pipeline files to a local directory. An empty version
	// string means "active version". The returned cleanup function removes
	// any temporary files; callers must call it when done.
	Fetch(ctx context.Context, pipeline string, version string) (dir string, cleanup func(), err error)

	// List returns all versions of a pipeline, newest first.
	List(ctx context.Context, pipeline string) ([]Version, error)

	// Active returns the currently active version for a pipeline.
	Active(ctx context.Context, pipeline string) (Version, error)

	// Register uploads a new version from a local directory.
	Register(ctx context.Context, pipeline string, sourceDir string) (Version, error)

	// Activate sets which version scheduled runs will use.
	Activate(ctx context.Context, pipeline string, version int) error
}

// Version describes a pipeline version in the registry.
type Version struct {
	Pipeline    string    `json:"pipeline"`
	Number      int       `json:"number"`
	ContentHash string    `json:"content_hash"`
	PushedBy    string    `json:"pushed_by"`
	PushedAt    time.Time `json:"pushed_at"`
	FileCount   int       `json:"file_count"`
	SizeBytes   int64     `json:"size_bytes"`
	Active      bool      `json:"active"`
}
