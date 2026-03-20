package pipe_registry

import (
	"context"
	"fmt"
)

// LocalRegistry reads pipeline files directly from a local directory.
// No versioning — this is what `granicus run pipeline.yaml` uses on a laptop.
type LocalRegistry struct {
	dir string
}

// NewLocalRegistry creates a LocalRegistry rooted at the given directory.
func NewLocalRegistry(dir string) *LocalRegistry {
	return &LocalRegistry{dir: dir}
}

// Fetch returns the local directory path directly. The cleanup function is a no-op.
func (s *LocalRegistry) Fetch(_ context.Context, pipeline string, version string) (string, func(), error) {
	return s.dir, func() {}, nil
}

// List is not supported for local sources.
func (s *LocalRegistry) List(_ context.Context, pipeline string) ([]Version, error) {
	return nil, fmt.Errorf("versioning not available in local mode")
}

// Active is not supported for local sources.
func (s *LocalRegistry) Active(_ context.Context, pipeline string) (Version, error) {
	return Version{}, fmt.Errorf("versioning not available in local mode")
}

// Register is not supported for local sources.
func (s *LocalRegistry) Register(_ context.Context, pipeline string, sourceDir string) (Version, error) {
	return Version{}, fmt.Errorf("versioning not available in local mode")
}

// Activate is not supported for local sources.
func (s *LocalRegistry) Activate(_ context.Context, pipeline string, version int) error {
	return fmt.Errorf("versioning not available in local mode")
}

// Verify LocalRegistry implements PipelineRegistry at compile time.
var _ PipelineRegistry = (*LocalRegistry)(nil)
