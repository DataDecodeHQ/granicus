package runner

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

// SubprocessEnvConfig holds the parameters for building a common subprocess environment.
type SubprocessEnvConfig struct {
	Asset        *Asset
	ProjectRoot  string
	RunID        string
	MetadataPath string
	DestConn     *config.ResourceConfig
	SrcConn      *config.ResourceConfig
	Refs         map[string]string
}

// buildSubprocessEnv constructs the base environment variable slice shared by all runners.
// It always sets the four required GRANICUS_* vars, and conditionally appends interval,
// connection, refs, and Python SDK path entries.
func buildSubprocessEnv(cfg SubprocessEnvConfig) []string {
	env := []string{
		"GRANICUS_ASSET_NAME=" + cfg.Asset.Name,
		"GRANICUS_RUN_ID=" + cfg.RunID,
		"GRANICUS_PROJECT_ROOT=" + cfg.ProjectRoot,
		"GRANICUS_METADATA_PATH=" + cfg.MetadataPath,
	}

	if cfg.Asset.IntervalStart != "" {
		env = append(env, "GRANICUS_INTERVAL_START="+cfg.Asset.IntervalStart)
		env = append(env, "GRANICUS_INTERVAL_END="+cfg.Asset.IntervalEnd)
	}

	if cfg.DestConn != nil {
		if connJSON, err := json.Marshal(flattenResource(cfg.DestConn)); err == nil {
			env = append(env, "GRANICUS_DEST_RESOURCE="+string(connJSON))
		}
	}
	if cfg.SrcConn != nil {
		if connJSON, err := json.Marshal(flattenResource(cfg.SrcConn)); err == nil {
			env = append(env, "GRANICUS_SOURCE_RESOURCE="+string(connJSON))
		}
	}

	if len(cfg.Refs) > 0 {
		if refsJSON, err := json.Marshal(cfg.Refs); err == nil {
			env = append(env, "GRANICUS_REFS="+string(refsJSON))
		}
	}

	if sdkPath := findSDKPath(); sdkPath != "" {
		env = appendPythonPath(env, sdkPath)
	}

	return env
}

// readMetadata reads and unmarshals the JSON metadata file at metadataPath.
// Returns nil, nil if the file does not exist or is empty.
func readMetadata(metadataPath string) (map[string]string, error) {
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var meta map[string]string
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// flattenResource converts a ResourceConfig into a flat string map suitable
// for JSON serialisation into a subprocess environment variable.
func flattenResource(conn *config.ResourceConfig) map[string]string {
	flat := make(map[string]string, len(conn.Properties)+2)
	flat["name"] = conn.Name
	flat["type"] = conn.Type
	for k, v := range conn.Properties {
		flat[k] = v
	}
	return flat
}
