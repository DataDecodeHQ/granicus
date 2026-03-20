package source

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	gstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/codes"
)

// GCSVersionedSource implements PipelineSource using GCS for storage and
// Firestore for the version registry. Pipeline versions are stored as
// tar.gz archives at gs://<bucket>/<pipeline>/versions/v<N>_<hash>.tar.gz
type GCSVersionedSource struct {
	gcs       *storage.Client
	firestore *firestore.Client
	bucket    string
}

// NewGCSVersionedSource creates a GCS-backed pipeline source.
func NewGCSVersionedSource(ctx context.Context, firestoreProject, bucket string) (*GCSVersionedSource, error) {
	if firestoreProject == "" {
		firestoreProject = os.Getenv("GRANICUS_FIRESTORE_PROJECT")
	}
	if bucket == "" {
		bucket = os.Getenv("GRANICUS_PIPELINES_BUCKET")
		if bucket == "" {
			bucket = "granicus-pipelines"
		}
	}

	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}

	fsClient, err := firestore.NewClient(ctx, firestoreProject)
	if err != nil {
		gcsClient.Close()
		return nil, fmt.Errorf("creating Firestore client: %w", err)
	}

	return &GCSVersionedSource{
		gcs:       gcsClient,
		firestore: fsClient,
		bucket:    bucket,
	}, nil
}

// dag:boundary
func (s *GCSVersionedSource) versionsCol(pipeline string) *firestore.CollectionRef {
	return s.firestore.Collection("pipelines").Doc(pipeline).Collection("versions")
}

// Fetch downloads a pipeline version from GCS, extracts to a temp dir.
// If pipeline is empty, fetches all pipelines with an active version
// into subdirectories of a combined temp dir.
func (s *GCSVersionedSource) Fetch(ctx context.Context, pipeline string, version string) (string, func(), error) {
	if pipeline == "" {
		return s.fetchAll(ctx)
	}
	return s.fetchOne(ctx, pipeline, version)
}

// fetchAll discovers all pipelines in Firestore and fetches each active one
// into a subdirectory of a combined temp dir.
func (s *GCSVersionedSource) fetchAll(ctx context.Context) (string, func(), error) {
	// List all pipeline docs
	iter := s.firestore.Collection("pipelines").Documents(ctx)
	defer iter.Stop()

	var pipelineNames []string
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return "", nil, fmt.Errorf("listing pipelines: %w", err)
		}
		pipelineNames = append(pipelineNames, doc.Ref.ID)
	}

	combinedDir, err := os.MkdirTemp("", "granicus-all-")
	if err != nil {
		return "", nil, fmt.Errorf("creating combined temp dir: %w", err)
	}

	for _, name := range pipelineNames {
		dir, cleanup, err := s.fetchOne(ctx, name, "")
		if err != nil {
			// Skip pipelines without an active version
			continue
		}
		// Move contents into a subdirectory named after the pipeline
		destDir := filepath.Join(combinedDir, name)
		if rerr := os.Rename(dir, destDir); rerr != nil {
			cleanup()
			continue
		}
	}

	cleanup := func() {
		os.RemoveAll(combinedDir)
	}
	return combinedDir, cleanup, nil
}

func (s *GCSVersionedSource) fetchOne(ctx context.Context, pipeline string, version string) (string, func(), error) {
	var ver Version
	var err error

	if version == "" {
		ver, err = s.Active(ctx, pipeline)
		if err != nil {
			return "", nil, fmt.Errorf("getting active version: %w", err)
		}
	} else {
		// Look up specific version
		vers, err := s.List(ctx, pipeline)
		if err != nil {
			return "", nil, err
		}
		found := false
		for _, v := range vers {
			if fmt.Sprintf("%d", v.Number) == version || fmt.Sprintf("v%d", v.Number) == version {
				ver = v
				found = true
				break
			}
		}
		if !found {
			return "", nil, fmt.Errorf("version %s not found for pipeline %s", version, pipeline)
		}
	}

	// Download archive from GCS
	objectName := fmt.Sprintf("%s/versions/v%d_%s.tar.gz", pipeline, ver.Number, ver.ContentHash)
	reader, err := s.gcs.Bucket(s.bucket).Object(objectName).NewReader(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("downloading version %d: %w", ver.Number, err)
	}
	defer reader.Close()

	// Extract to temp dir
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("granicus-%s-v%d-", pipeline, ver.Number))
	if err != nil {
		return "", nil, fmt.Errorf("creating temp dir: %w", err)
	}

	if err := extractTarGz(reader, tmpDir); err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, fmt.Errorf("extracting archive: %w", err)
	}

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return tmpDir, cleanup, nil
}

// List returns all versions of a pipeline from Firestore, newest first.
// dag:boundary
func (s *GCSVersionedSource) List(ctx context.Context, pipeline string) ([]Version, error) {
	iter := s.versionsCol(pipeline).OrderBy("number", firestore.Desc).Documents(ctx)
	defer iter.Stop()

	var versions []Version
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing versions: %w", err)
		}
		var ver Version
		if err := doc.DataTo(&ver); err != nil {
			return nil, err
		}
		versions = append(versions, ver)
	}
	return versions, nil
}

// Active returns the currently active version for a pipeline.
func (s *GCSVersionedSource) Active(ctx context.Context, pipeline string) (Version, error) {
	iter := s.versionsCol(pipeline).Where("active", "==", true).Limit(1).Documents(ctx)
	defer iter.Stop()

	doc, err := iter.Next()
	if err == iterator.Done {
		return Version{}, fmt.Errorf("no active version for pipeline %s", pipeline)
	}
	if err != nil {
		return Version{}, fmt.Errorf("querying active version: %w", err)
	}

	var ver Version
	if err := doc.DataTo(&ver); err != nil {
		return Version{}, err
	}
	return ver, nil
}

// Register packages a pipeline directory as a new version, uploads to GCS,
// and registers in Firestore.
// dag:boundary
func (s *GCSVersionedSource) Register(ctx context.Context, pipeline string, sourceDir string) (Version, error) {
	// Compute content hash and create archive
	hash, fileCount, sizeBytes, archivePath, err := createArchive(sourceDir)
	if err != nil {
		return Version{}, fmt.Errorf("creating archive: %w", err)
	}
	defer os.Remove(archivePath)

	// Determine next version number
	versions, err := s.List(ctx, pipeline)
	if err != nil {
		return Version{}, err
	}
	nextNum := 1
	if len(versions) > 0 {
		nextNum = versions[0].Number + 1
	}

	// Check for duplicate content hash
	for _, v := range versions {
		if v.ContentHash == hash {
			return v, fmt.Errorf("content hash %s already exists as version %d", hash, v.Number)
		}
	}

	// Upload to GCS
	objectName := fmt.Sprintf("%s/versions/v%d_%s.tar.gz", pipeline, nextNum, hash)
	writer := s.gcs.Bucket(s.bucket).Object(objectName).NewWriter(ctx)

	archiveData, err := os.ReadFile(archivePath)
	if err != nil {
		return Version{}, fmt.Errorf("reading archive: %w", err)
	}
	if _, err := writer.Write(archiveData); err != nil {
		writer.Close()
		return Version{}, fmt.Errorf("uploading archive: %w", err)
	}
	if err := writer.Close(); err != nil {
		return Version{}, fmt.Errorf("closing GCS writer: %w", err)
	}

	// Register in Firestore
	ver := Version{
		Pipeline:    pipeline,
		Number:      nextNum,
		ContentHash: hash,
		PushedBy:    currentUser(),
		PushedAt:    time.Now().UTC(),
		FileCount:   fileCount,
		SizeBytes:   sizeBytes,
		Active:      false,
	}

	// Ensure pipeline parent doc exists (required for fetchAll discovery)
	_, err = s.firestore.Collection("pipelines").Doc(pipeline).Set(ctx, map[string]any{
		"name":       pipeline,
		"updated_at": time.Now().UTC(),
	}, firestore.MergeAll)
	if err != nil {
		return Version{}, fmt.Errorf("creating pipeline doc: %w", err)
	}

	docID := fmt.Sprintf("v%d", nextNum)
	_, err = s.versionsCol(pipeline).Doc(docID).Set(ctx, ver)
	if err != nil {
		return Version{}, fmt.Errorf("registering version: %w", err)
	}

	return ver, nil
}

// Activate sets the active version for a pipeline via Firestore transaction.
// dag:boundary
func (s *GCSVersionedSource) Activate(ctx context.Context, pipeline string, version int) error {
	return s.firestore.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		// Find and deactivate current active version
		iter := s.versionsCol(pipeline).Where("active", "==", true).Documents(ctx)
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				iter.Stop()
				return err
			}
			if err := tx.Update(doc.Ref, []firestore.Update{{Path: "active", Value: false}}); err != nil {
				iter.Stop()
				return err
			}
		}
		iter.Stop()

		// Activate the target version
		targetDoc := s.versionsCol(pipeline).Doc(fmt.Sprintf("v%d", version))
		doc, err := tx.Get(targetDoc)
		if err != nil {
			if gstatus.Code(err) == codes.NotFound {
				return fmt.Errorf("version %d not found for pipeline %s", version, pipeline)
			}
			return err
		}
		_ = doc // exists check
		return tx.Update(targetDoc, []firestore.Update{{Path: "active", Value: true}})
	})
}

func currentUser() string {
	if u := os.Getenv("USER"); u != "" {
		return u
	}
	return "unknown"
}

// Diff returns files that differ between two versions.
func (s *GCSVersionedSource) Diff(ctx context.Context, pipeline string, versionA, versionB int) (added, removed, modified []string, err error) {
	dirA, cleanupA, err := s.Fetch(ctx, pipeline, fmt.Sprintf("%d", versionA))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("fetching version %d: %w", versionA, err)
	}
	defer cleanupA()

	dirB, cleanupB, err := s.Fetch(ctx, pipeline, fmt.Sprintf("%d", versionB))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("fetching version %d: %w", versionB, err)
	}
	defer cleanupB()

	filesA := listFiles(dirA)
	filesB := listFiles(dirB)

	setA := make(map[string]string, len(filesA))
	for _, f := range filesA {
		setA[f] = ""
	}
	setB := make(map[string]string, len(filesB))
	for _, f := range filesB {
		setB[f] = ""
	}

	for _, f := range filesB {
		if _, ok := setA[f]; !ok {
			added = append(added, f)
		}
	}
	for _, f := range filesA {
		if _, ok := setB[f]; !ok {
			removed = append(removed, f)
		}
	}

	// Check for content changes in common files
	for _, f := range filesA {
		if _, ok := setB[f]; ok {
			dataA, errA := os.ReadFile(filepath.Join(dirA, f))
			dataB, errB := os.ReadFile(filepath.Join(dirB, f))
			if errA != nil || errB != nil || string(dataA) != string(dataB) {
				modified = append(modified, f)
			}
		}
	}

	sort.Strings(added)
	sort.Strings(removed)
	sort.Strings(modified)
	return added, removed, modified, nil
}

func listFiles(dir string) []string {
	var files []string
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(dir, path)
		files = append(files, rel)
		return nil
	})
	return files
}

// Verify GCSVersionedSource implements PipelineSource at compile time.
var _ PipelineSource = (*GCSVersionedSource)(nil)
