package migrate

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	Version02 = "0.2"
	Version03 = "0.3"
	Version04 = "0.4"

	LatestVersion = Version04
)

// Change describes a single modification made during migration.
type Change struct {
	Description string
}

// Result holds the outcome of a migration.
type Result struct {
	FromVersion string
	ToVersion   string
	Changes     []Change
	Content     []byte
	AlreadyCurrent bool
}

// DetectVersion infers the config version from raw YAML content.
// Configs without a version field are treated as 0.2.
func DetectVersion(content []byte) string {
	for _, line := range strings.Split(string(content), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "version:") {
			rest := strings.TrimPrefix(trimmed, "version:")
			rest = strings.TrimSpace(rest)
			rest = strings.Trim(rest, `"'`)
			return rest
		}
	}
	return Version02
}

// Migrate applies all necessary migrations from fromVersion up to LatestVersion.
// Returns the transformed content and a list of changes made.
func Migrate(content []byte, fromVersion string) (*Result, error) {
	result := &Result{
		FromVersion: fromVersion,
		ToVersion:   fromVersion,
		Content:     content,
	}

	if fromVersion == LatestVersion {
		result.AlreadyCurrent = true
		return result, nil
	}

	migrations := []struct {
		from string
		to   string
		fn   func([]byte) ([]byte, []Change, error)
	}{
		{Version02, Version03, migrate02to03},
		{Version03, Version04, migrate03to04},
	}

	current := content
	currentVersion := fromVersion
	for _, m := range migrations {
		if currentVersion != m.from {
			continue
		}
		updated, changes, err := m.fn(current)
		if err != nil {
			return nil, fmt.Errorf("migrating %s -> %s: %w", m.from, m.to, err)
		}
		current = updated
		currentVersion = m.to
		result.Changes = append(result.Changes, changes...)
		result.ToVersion = m.to

		if currentVersion == LatestVersion {
			break
		}
	}

	result.Content = current
	return result, nil
}

// migrate02to03 applies the 0.2 -> 0.3 migration.
// 0.3 adds a version header to the config file.
// Python module path resolution is handled by the runner (no pipeline.yaml changes needed).
func migrate02to03(content []byte) ([]byte, []Change, error) {
	var changes []Change

	updated := addVersionHeader(content, "0.3")
	changes = append(changes, Change{Description: "added version: \"0.3\" header"})

	return updated, changes, nil
}

// migrate03to04 renames connection terminology to resource terminology.
func migrate03to04(content []byte) ([]byte, []Change, error) {
	var changes []Change
	text := string(content)

	replacements := []struct {
		old, new, desc string
	}{
		{"connections:", "resources:", "renamed connections: to resources:"},
		{"destination_connection:", "destination_resource:", "renamed destination_connection: to destination_resource:"},
		{"source_connection:", "source_resource:", "renamed source_connection: to source_resource:"},
	}
	for _, r := range replacements {
		if strings.Contains(text, r.old) {
			text = strings.ReplaceAll(text, r.old, r.new)
			changes = append(changes, Change{Description: r.desc})
		}
	}

	// Rename source-level connection: to resource: (indented under sources)
	lines := strings.Split(text, "\n")
	inSources := false
	sourceIndent := 0
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		indent := len(line) - len(strings.TrimLeft(line, " "))

		if trimmed == "sources:" {
			inSources = true
			sourceIndent = indent
			continue
		}
		if inSources && indent <= sourceIndent && trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			inSources = false
		}
		if inSources && strings.HasPrefix(trimmed, "connection:") {
			lines[i] = strings.Replace(line, "connection:", "resource:", 1)
			if len(changes) == 0 || changes[len(changes)-1].Description != "renamed source connection: to resource:" {
				changes = append(changes, Change{Description: "renamed source connection: to resource:"})
			}
		}
	}
	text = strings.Join(lines, "\n")

	// Bump version
	text = strings.Replace(text, `version: "0.3"`, `version: "0.4"`, 1)
	changes = append(changes, Change{Description: `updated version to "0.4"`})

	return []byte(text), changes, nil
}

// addVersionHeader inserts a version field at the top of the YAML content.
// Preserves existing content. Skips comment lines at the top.
func addVersionHeader(content []byte, version string) []byte {
	header := fmt.Sprintf("version: %q\n", version)
	lines := strings.Split(string(content), "\n")

	// Find insertion point: after leading comments
	insertAt := 0
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || trimmed == "" {
			insertAt = i + 1
		} else {
			break
		}
	}

	result := make([]string, 0, len(lines)+1)
	result = append(result, lines[:insertAt]...)
	result = append(result, strings.TrimSuffix(header, "\n"))
	result = append(result, lines[insertAt:]...)

	return []byte(strings.Join(result, "\n"))
}

// WriteBackup creates a timestamped backup of the file at configPath.
func WriteBackup(configPath string) (string, error) {
	content, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("reading config for backup: %w", err)
	}

	ts := time.Now().Format("20060102-150405")
	backupPath := configPath + "." + ts + ".bak"

	if err := os.WriteFile(backupPath, content, 0644); err != nil {
		return "", fmt.Errorf("writing backup: %w", err)
	}

	return backupPath, nil
}
