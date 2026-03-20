package source

import (
	"context"
	"fmt"
)

// LocalSource reads pipeline files directly from a local directory.
// No versioning — this is what `granicus run pipeline.yaml` uses on a laptop.
type LocalSource struct {
	dir string
}

// NewLocalSource creates a LocalSource rooted at the given directory.
func NewLocalSource(dir string) *LocalSource {
	return &LocalSource{dir: dir}
}

// Fetch returns the local directory path directly. The cleanup function is a no-op.
func (s *LocalSource) Fetch(_ context.Context, pipeline string, version string) (string, func(), error) {
	return s.dir, func() {}, nil
}

// List is not supported for local sources.
func (s *LocalSource) List(_ context.Context, pipeline string) ([]Version, error) {
	return nil, fmt.Errorf("versioning not available in local mode")
}

// Active is not supported for local sources.
func (s *LocalSource) Active(_ context.Context, pipeline string) (Version, error) {
	return Version{}, fmt.Errorf("versioning not available in local mode")
}

// Register is not supported for local sources.
func (s *LocalSource) Register(_ context.Context, pipeline string, sourceDir string) (Version, error) {
	return Version{}, fmt.Errorf("versioning not available in local mode")
}

// Activate is not supported for local sources.
func (s *LocalSource) Activate(_ context.Context, pipeline string, version int) error {
	return fmt.Errorf("versioning not available in local mode")
}

// Verify LocalSource implements PipelineSource at compile time.
var _ PipelineSource = (*LocalSource)(nil)
