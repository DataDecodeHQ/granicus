package graph

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/analytehealth/granicus/internal/config"
)

var depPattern = regexp.MustCompile(`^\s*(?:--|#)\s*depends_on:\s*(\S+)\s*$`)

const maxScanLines = 50

func ParseDependencies(filePath string) ([]string, error) {
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

func ConfigToAssetInputs(cfg *config.PipelineConfig) []AssetInput {
	inputs := make([]AssetInput, len(cfg.Assets))
	for i, a := range cfg.Assets {
		inputs[i] = AssetInput{
			Name:                  a.Name,
			Type:                  a.Type,
			Source:                a.Source,
			DestinationConnection: a.DestinationConnection,
			SourceConnection:      a.SourceConnection,
		}
	}
	return inputs
}
