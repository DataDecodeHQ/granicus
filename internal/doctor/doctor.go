package doctor

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	_ "modernc.org/sqlite"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
)

type Status string

const (
	StatusPass    Status = "pass"
	StatusFail    Status = "fail"
	StatusWarn    Status = "warn"
	StatusSkipped Status = "skipped"
)

type CheckResult struct {
	Name    string `json:"name"`
	Status  Status `json:"status"`
	Message string `json:"message"`
}

// RunChecks executes all doctor checks. cfg may be nil if no config was provided.
func RunChecks(cfg *config.PipelineConfig, projectRoot string) []CheckResult {
	var results []CheckResult

	results = append(results, checkGoVersion())

	if cfg != nil {
		for name, conn := range cfg.Connections {
			switch conn.Type {
			case "bigquery":
				results = append(results, checkBQConnectivity(name, conn))
			case "gcs":
				results = append(results, checkGCSConfig(name, conn))
			}
		}
	}

	grainicusDir := filepath.Join(projectRoot, ".granicus")
	results = append(results, checkStateDB(filepath.Join(grainicusDir, "state.db")))
	results = append(results, checkEventsDB(filepath.Join(grainicusDir, "events.db")))
	results = append(results, checkDiskSpace(grainicusDir))

	return results
}

func checkGoVersion() CheckResult {
	return CheckResult{
		Name:    "go_version",
		Status:  StatusPass,
		Message: runtime.Version(),
	}
}

func checkBQConnectivity(connName string, conn *config.ConnectionConfig) CheckResult {
	name := "bq:" + connName
	project := conn.Properties["project"]
	if project == "" {
		return CheckResult{Name: name, Status: StatusFail, Message: "missing project property"}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var opts []option.ClientOption
	if creds := conn.Properties["credentials"]; creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}

	client, err := bigquery.NewClient(ctx, project, opts...)
	if err != nil {
		return CheckResult{Name: name, Status: StatusFail, Message: fmt.Sprintf("client: %v", err)}
	}
	defer client.Close()

	q := client.Query("SELECT 1 AS n")
	it, err := q.Read(ctx)
	if err != nil {
		return CheckResult{Name: name, Status: StatusFail, Message: fmt.Sprintf("query: %v", err)}
	}

	var row []bigquery.Value
	if err := it.Next(&row); err != nil && err != iterator.Done {
		return CheckResult{Name: name, Status: StatusFail, Message: fmt.Sprintf("read: %v", err)}
	}

	return CheckResult{Name: name, Status: StatusPass, Message: fmt.Sprintf("project=%s", project)}
}

func checkGCSConfig(connName string, conn *config.ConnectionConfig) CheckResult {
	name := "gcs:" + connName
	bucket := conn.Properties["bucket"]
	if bucket == "" {
		return CheckResult{Name: name, Status: StatusFail, Message: "missing bucket property"}
	}

	credMethod := "ADC (Application Default Credentials)"
	if creds := conn.Properties["credentials"]; creds != "" {
		if _, err := os.Stat(creds); os.IsNotExist(err) {
			return CheckResult{Name: name, Status: StatusFail, Message: fmt.Sprintf("credentials not found: %s", creds)}
		}
		credMethod = "file: " + creds
	} else if envCreds := os.Getenv("GCS_SERVICE_ACCOUNT"); envCreds != "" {
		if _, err := os.Stat(envCreds); os.IsNotExist(err) {
			return CheckResult{Name: name, Status: StatusWarn, Message: fmt.Sprintf("GCS_SERVICE_ACCOUNT set but file not found: %s", envCreds)}
		}
		credMethod = "env: GCS_SERVICE_ACCOUNT"
	}

	return CheckResult{Name: name, Status: StatusPass, Message: fmt.Sprintf("bucket=%s, credentials=%s", bucket, credMethod)}
}

func checkStateDB(dbPath string) CheckResult {
	name := "state.db"

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		dir := filepath.Dir(dbPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return CheckResult{Name: name, Status: StatusFail, Message: fmt.Sprintf("cannot create dir: %v", err)}
		}
		f, err := os.CreateTemp(dir, ".write-test-*")
		if err != nil {
			return CheckResult{Name: name, Status: StatusFail, Message: "directory not writable"}
		}
		f.Close()
		os.Remove(f.Name())
		return CheckResult{Name: name, Status: StatusPass, Message: "not yet created, directory writable"}
	}

	// Check file is writable
	f, err := os.OpenFile(dbPath, os.O_RDWR, 0)
	if err != nil {
		return CheckResult{Name: name, Status: StatusFail, Message: "not writable"}
	}
	f.Close()

	// Check integrity
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return CheckResult{Name: name, Status: StatusFail, Message: fmt.Sprintf("open: %v", err)}
	}
	defer db.Close()

	var result string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&result); err != nil {
		return CheckResult{Name: name, Status: StatusFail, Message: fmt.Sprintf("integrity_check: %v", err)}
	}
	if result != "ok" {
		return CheckResult{Name: name, Status: StatusFail, Message: fmt.Sprintf("corrupted: %s", result)}
	}

	return CheckResult{Name: name, Status: StatusPass, Message: "writable, integrity ok"}
}

func checkEventsDB(dbPath string) CheckResult {
	name := "events.db"

	store, err := events.New(dbPath)
	if err != nil {
		return CheckResult{Name: name, Status: StatusFail, Message: fmt.Sprintf("open: %v", err)}
	}
	store.Close()

	// Verify file is writable after creation
	f, err := os.OpenFile(dbPath, os.O_RDWR, 0)
	if err != nil {
		return CheckResult{Name: name, Status: StatusFail, Message: "not writable"}
	}
	f.Close()

	return CheckResult{Name: name, Status: StatusPass, Message: "writable"}
}

func checkDiskSpace(dir string) CheckResult {
	name := "disk_space"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return CheckResult{Name: name, Status: StatusWarn, Message: fmt.Sprintf("cannot check: %v", err)}
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return CheckResult{Name: name, Status: StatusWarn, Message: fmt.Sprintf("statfs: %v", err)}
	}

	availableBytes := stat.Bavail * uint64(stat.Bsize)
	availableMB := availableBytes / (1024 * 1024)

	switch {
	case availableMB < 100:
		return CheckResult{Name: name, Status: StatusFail, Message: fmt.Sprintf("%d MB available (critical: < 100 MB)", availableMB)}
	case availableMB < 1024:
		return CheckResult{Name: name, Status: StatusWarn, Message: fmt.Sprintf("%d MB available (low: < 1 GB)", availableMB)}
	default:
		return CheckResult{Name: name, Status: StatusPass, Message: fmt.Sprintf("%.1f GB available", float64(availableMB)/1024)}
	}
}
