package logging

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type RunSummary struct {
	RunID           string    `json:"run_id"`
	Pipeline        string    `json:"pipeline"`
	StartTime       time.Time `json:"start_time"`
	EndTime         time.Time `json:"end_time"`
	DurationSeconds float64   `json:"duration_seconds"`
	TotalNodes      int       `json:"total_nodes"`
	Succeeded       int       `json:"succeeded"`
	Failed          int       `json:"failed"`
	Skipped         int       `json:"skipped"`
	Status          string    `json:"status"`
	Config          RunConfig `json:"config"`
}

type RunConfig struct {
	MaxParallel  int      `json:"max_parallel"`
	AssetsFilter []string `json:"assets_filter"`
}

type NodeEntry struct {
	Asset       string            `json:"asset"`
	Status      string            `json:"status"`
	StartTime   string            `json:"start_time"`
	EndTime     string            `json:"end_time"`
	DurationMs  int64             `json:"duration_ms"`
	ExitCode    int               `json:"exit_code"`
	Error       string            `json:"error"`
	Stdout      string            `json:"stdout,omitempty"`
	Stderr      string            `json:"stderr,omitempty"`
	StdoutLines int               `json:"stdout_lines"`
	StderrLines int               `json:"stderr_lines"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

const (
	MaxOutputBytes = 10 * 1024
	TruncMarker    = "[truncated]"
)

type Store struct {
	baseDir string
	mu      sync.Mutex
}

func NewStore(baseDir string) *Store {
	return &Store{baseDir: baseDir}
}

func (s *Store) BaseDir() string {
	return s.baseDir
}

func GenerateRunID() string {
	now := time.Now()
	suffix := fmt.Sprintf("%04x", rand.Intn(0xFFFF))
	return fmt.Sprintf("run_%s_%s_%s",
		now.Format("20060102"),
		now.Format("150405"),
		suffix[:4],
	)
}

func (s *Store) runDir(runID string) string {
	return filepath.Join(s.baseDir, ".granicus", "runs", runID)
}

func (s *Store) ensureDir(runID string) error {
	return os.MkdirAll(s.runDir(runID), 0755)
}

func truncField(val string) string {
	if len(val) <= MaxOutputBytes {
		return val
	}
	return val[:MaxOutputBytes] + TruncMarker
}

func CountLines(s string) int {
	if s == "" {
		return 0
	}
	return strings.Count(s, "\n") + 1
}

func (s *Store) WriteNodeResult(runID string, entry NodeEntry) error {
	if err := s.ensureDir(runID); err != nil {
		return err
	}
	entry.Stdout = truncField(entry.Stdout)
	entry.Stderr = truncField(entry.Stderr)

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.runDir(runID), "nodes.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	return err
}

func (s *Store) WriteRunSummary(runID string, summary RunSummary) error {
	if err := s.ensureDir(runID); err != nil {
		return err
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.runDir(runID), "run.json"), data, 0644)
}

func (s *Store) ReadRunSummary(runID string) (*RunSummary, error) {
	data, err := os.ReadFile(filepath.Join(s.runDir(runID), "run.json"))
	if err != nil {
		return nil, err
	}
	var summary RunSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		return nil, err
	}
	return &summary, nil
}

func (s *Store) ReadNodeResults(runID string) ([]NodeEntry, error) {
	data, err := os.ReadFile(filepath.Join(s.runDir(runID), "nodes.jsonl"))
	if err != nil {
		return nil, err
	}
	var results []NodeEntry
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var entry NodeEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return nil, err
		}
		results = append(results, entry)
	}
	return results, nil
}

func (s *Store) ListRuns() ([]RunSummary, error) {
	runsDir := filepath.Join(s.baseDir, ".granicus", "runs")
	entries, err := os.ReadDir(runsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var runs []RunSummary
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		summary, err := s.ReadRunSummary(e.Name())
		if err != nil {
			continue
		}
		runs = append(runs, *summary)
	}

	sort.Slice(runs, func(i, j int) bool {
		return runs[i].RunID > runs[j].RunID // reverse chronological
	})
	return runs, nil
}
