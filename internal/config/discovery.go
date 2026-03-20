package config

import (
	"os"
	"path/filepath"
	"strings"
)

type DiscoveryConfig struct {
	Paths []DiscoveryPath `yaml:"discovery"`
}

type DiscoveryPath struct {
	Path              string   `yaml:"path"`
	Exclude           []string `yaml:"exclude,omitempty"`
	DefaultConnection string   `yaml:"default_connection,omitempty"`
}

var extensionTypeMap = map[string]string{
	".sql": "sql",
	".py":  "python",
	".sh":  "shell",
}

var directoryLayerMap = map[string]string{
	"staging":      "staging",
	"intermediate": "intermediate",
	"entity":       "entity",
	"report":       "report",
}

// DiscoverAssets walks the configured discovery paths and returns asset configs inferred from file extensions and directory structure.
func DiscoverAssets(pipelineDir string, discoveryPaths []DiscoveryPath) ([]AssetConfig, error) {
	var discovered []AssetConfig

	for _, dp := range discoveryPaths {
		searchDir := filepath.Join(pipelineDir, dp.Path)
		if _, err := os.Stat(searchDir); os.IsNotExist(err) {
			continue
		}

		err := filepath.Walk(searchDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}

			ext := filepath.Ext(path)
			assetType, known := extensionTypeMap[ext]
			if !known {
				return nil
			}

			// Check excludes
			relPath, _ := filepath.Rel(pipelineDir, path)
			for _, pattern := range dp.Exclude {
				if matched, _ := filepath.Match(pattern, filepath.Base(path)); matched {
					return nil
				}
				if matched, _ := filepath.Match(pattern, relPath); matched {
					return nil
				}
			}

			// Skip check files
			base := filepath.Base(path)
			if strings.HasPrefix(base, "check_") {
				return nil
			}

			name := strings.TrimSuffix(base, ext)

			// Infer layer from directory path
			layer := ""
			dir := filepath.Dir(relPath)
			for seg, l := range directoryLayerMap {
				if strings.Contains(dir, seg) {
					layer = l
					break
				}
			}

			asset := AssetConfig{
				Name:                  name,
				Type:                  assetType,
				Source:                relPath,
				Layer:                 layer,
				DestinationResource: dp.DefaultConnection,
			}

			discovered = append(discovered, asset)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return discovered, nil
}

// MergeDiscoveredAssets combines explicit and discovered assets, skipping discovered assets whose names already exist.
func MergeDiscoveredAssets(explicit []AssetConfig, discovered []AssetConfig) []AssetConfig {
	seen := make(map[string]bool)
	for _, a := range explicit {
		seen[a.Name] = true
	}

	result := make([]AssetConfig, len(explicit))
	copy(result, explicit)

	for _, a := range discovered {
		if !seen[a.Name] {
			result = append(result, a)
			seen[a.Name] = true
		}
	}

	return result
}
