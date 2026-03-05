package graph

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/analytehealth/granicus/internal/config"
)

var depPattern = regexp.MustCompile(`^\s*(?:--|#)\s*depends_on:\s*(\S+)\s*$`)

const maxScanLines = 50

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

func ConfigToAssetInputs(cfg *config.PipelineConfig) []AssetInput {
	inputs := make([]AssetInput, len(cfg.Assets))
	for i, a := range cfg.Assets {
		var timeout time.Duration
		if a.Timeout != "" {
			timeout, _ = time.ParseDuration(a.Timeout)
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
		}
	}
	return inputs
}
