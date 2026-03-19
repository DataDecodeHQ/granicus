package graph

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

var depPattern = regexp.MustCompile(`^\s*(?:--|#)\s*depends_on:\s*(\S+)\s*$`)

const maxScanLines = 50

// ParseDependencies extracts dependency names from a source file's granicus directives or legacy comments.
func ParseDependencies(filePath string) ([]string, error) {
	found, d, err := ParseDirectivesWithBlock(filePath)
	if err != nil {
		return nil, err
	}
	if found {
		return d.DependsOn, nil
	}
	// Fall back to legacy regex format for old-style "-- depends_on: asset" comments
	return parseLegacyDependencies(filePath)
}

func parseLegacyDependencies(filePath string) ([]string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var deps []string
	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		if lineNum > maxScanLines {
			break
		}
		matches := depPattern.FindStringSubmatch(scanner.Text())
		if matches != nil {
			deps = append(deps, matches[1])
		}
	}
	return deps, scanner.Err()
}

type AssetDirectives struct {
	DependsOn    []string
	TimeColumn   string
	IntervalUnit string
	Lookback     int
	StartDate    string
	BatchSize    int
	Produces     []string
}

// ParseAllDependencies parses dependencies for every asset in the pipeline config.
func ParseAllDependencies(cfg *config.PipelineConfig, projectRoot string) (map[string][]string, error) {
	result := make(map[string][]string)
	var missing []string

	for _, asset := range cfg.Assets {
		path := filepath.Join(projectRoot, asset.Source)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			missing = append(missing, asset.Source)
			continue
		}
		deps, err := ParseDependencies(path)
		if err != nil {
			return nil, fmt.Errorf("parsing %s: %w", asset.Source, err)
		}
		if len(deps) > 0 {
			result[asset.Name] = deps
		}
	}

	if len(missing) > 0 {
		return nil, fmt.Errorf("missing source files: %v", missing)
	}

	return result, nil
}

// ParseAllDirectives parses both dependencies and full directives for every asset in the pipeline config.
func ParseAllDirectives(cfg *config.PipelineConfig, projectRoot string) (map[string][]string, map[string]*Directives, error) {
	deps := make(map[string][]string)
	directives := make(map[string]*Directives)
	var missing []string

	for _, asset := range cfg.Assets {
		path := filepath.Join(projectRoot, asset.Source)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			missing = append(missing, asset.Source)
			continue
		}
		d, err := ParseDirectives(path)
		if err != nil {
			return nil, nil, fmt.Errorf("parsing %s: %w", asset.Source, err)
		}
		if len(d.DependsOn) > 0 {
			deps[asset.Name] = d.DependsOn
		}
		if len(asset.DependsOn) > 0 {
			deps[asset.Name] = append(deps[asset.Name], asset.DependsOn...)
		}
		directives[asset.Name] = &d
	}

	if len(missing) > 0 {
		return nil, nil, fmt.Errorf("missing source files: %v", missing)
	}

	return deps, directives, nil
}

// SourcePhantomNodes creates no-op asset inputs for declared sources so checks can depend on them.
func SourcePhantomNodes(cfg *config.PipelineConfig) []AssetInput {
	var inputs []AssetInput
	for name := range cfg.Sources {
		inputs = append(inputs, AssetInput{
			Name:  "source:" + name,
			Type:  AssetTypeSource,
			Layer: "source",
		})
	}
	return inputs
}

// ConfigToAssetInputs converts pipeline config assets into graph AssetInput structs.
func ConfigToAssetInputs(cfg *config.PipelineConfig) []AssetInput {
	inputs := make([]AssetInput, len(cfg.Assets))
	for i, a := range cfg.Assets {
		var timeout time.Duration
		if a.Timeout != "" {
			var terr error
			timeout, terr = time.ParseDuration(a.Timeout)
			if terr != nil {
				slog.Warn("invalid asset timeout, using zero", "asset", a.Name, "timeout", a.Timeout, "error", terr)
			}
		}
		var maxAttempts int
		var backoffBase time.Duration
		var retryableErrors []string
		if a.Retry != nil {
			maxAttempts = a.Retry.MaxAttempts
			if a.Retry.BackoffBase != "" {
				var berr error
				backoffBase, berr = time.ParseDuration(a.Retry.BackoffBase)
				if berr != nil {
					slog.Warn("invalid retry backoff_base, using zero", "asset", a.Name, "backoff_base", a.Retry.BackoffBase, "error", berr)
				}
			}
			retryableErrors = a.Retry.RetryableErrors
		}
		inputs[i] = AssetInput{
			Name:                  a.Name,
			Type:                  a.Type,
			Source:                a.Source,
			DestinationConnection: a.DestinationConnection,
			SourceConnection:      a.SourceConnection,
			Layer:                 a.Layer,
			Grain:                 a.Grain,
			DefaultChecks:         a.DefaultChecks,
			Timeout:               timeout,
			MaxAttempts:           maxAttempts,
			BackoffBase:           backoffBase,
			RetryableErrors:       retryableErrors,
		}
	}
	return inputs
}
