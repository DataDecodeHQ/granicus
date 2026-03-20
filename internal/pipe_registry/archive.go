package pipe_registry

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// createArchive creates a tar.gz archive from sourceDir, computes content hash,
// and returns (hash, fileCount, totalSize, archivePath, error).
func createArchive(sourceDir string) (string, int, int64, string, error) {
	tmpFile, err := os.CreateTemp("", "granicus-archive-*.tar.gz")
	if err != nil {
		return "", 0, 0, "", err
	}
	defer tmpFile.Close()

	hasher := sha256.New()
	gzWriter := gzip.NewWriter(io.MultiWriter(tmpFile, hasher))
	tarWriter := tar.NewWriter(gzWriter)

	var fileCount int
	var totalSize int64

	err = filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip hidden files and directories
		if strings.HasPrefix(info.Name(), ".") && info.Name() != "." {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		rel, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = rel

		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		written, err := io.Copy(tarWriter, f)
		if err != nil {
			return err
		}

		fileCount++
		totalSize += written
		return nil
	})

	if err != nil {
		os.Remove(tmpFile.Name())
		return "", 0, 0, "", err
	}

	if err := tarWriter.Close(); err != nil {
		os.Remove(tmpFile.Name())
		return "", 0, 0, "", err
	}
	if err := gzWriter.Close(); err != nil {
		os.Remove(tmpFile.Name())
		return "", 0, 0, "", err
	}

	hash := fmt.Sprintf("%x", hasher.Sum(nil))[:12]
	return hash, fileCount, totalSize, tmpFile.Name(), nil
}

// extractTarGz extracts a gzipped tar archive from reader into destDir.
// dag:boundary
func extractTarGz(reader io.Reader, destDir string) error {
	gzReader, err := gzip.NewReader(reader)
	if err != nil {
		return fmt.Errorf("creating gzip reader: %w", err)
	}
	defer gzReader.Close()

	tarReader := tar.NewReader(gzReader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading tar header: %w", err)
		}

		target := filepath.Join(destDir, header.Name)

		// Guard against path traversal
		if !strings.HasPrefix(filepath.Clean(target), filepath.Clean(destDir)) {
			return fmt.Errorf("path traversal detected: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			f, err := os.Create(target)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tarReader); err != nil {
				f.Close()
				return err
			}
			f.Close()
			if err := os.Chmod(target, os.FileMode(header.Mode)); err != nil {
				return err
			}
		}
	}

	return nil
}
