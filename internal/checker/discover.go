package checker

import (
	"path/filepath"
	"strings"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

// DiscoverChecks scans the checks directory for check files and attaches them to matching assets.
func DiscoverChecks(pipelineDir string, assets []config.AssetConfig) []config.AssetConfig {
	checksDir := filepath.Join(pipelineDir, "checks")
	matches, err := filepath.Glob(filepath.Join(checksDir, "check_*"))
	if err != nil || len(matches) == 0 {
		return assets
	}

	// Index existing checks by source path for dedup
	existing := make(map[string]map[string]bool) // asset_name -> set of source paths
	for _, a := range assets {
		existing[a.Name] = make(map[string]bool)
		for _, c := range a.Checks {
			existing[a.Name][c.Source] = true
		}
	}

	// Match check files to assets
	for _, match := range matches {
		base := filepath.Base(match)
		nameNoExt := strings.TrimSuffix(base, filepath.Ext(base))

		if !strings.HasPrefix(nameNoExt, "check_") {
			continue
		}
		suffix := nameNoExt[len("check_"):]

		for i := range assets {
			if !strings.HasPrefix(suffix, assets[i].Name) {
				continue
			}

			relPath, _ := filepath.Rel(filepath.Dir(pipelineDir), match)
			if relPath == "" {
				relPath = match
			}
			// Use path relative to project root
			checkSource := filepath.Join("checks", base)

			if existing[assets[i].Name] != nil && existing[assets[i].Name][checkSource] {
				continue
			}

			checkName := nameNoExt
			assets[i].Checks = append(assets[i].Checks, config.CheckConfig{
				Name:   checkName,
				Source: checkSource,
			})
			if existing[assets[i].Name] == nil {
				existing[assets[i].Name] = make(map[string]bool)
			}
			existing[assets[i].Name][checkSource] = true
			break
		}
	}

	return assets
}
