package runner

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

// BuiltinFuncMap returns the default template function map with built-in SQL helpers.
func BuiltinFuncMap() template.FuncMap {
	return template.FuncMap{
		"cast_to_currency": func(col string) string {
			return fmt.Sprintf("CAST(ROUND(%s, 2) AS NUMERIC)", col)
		},
	}
}

type RefContext struct {
	Assets         []RefAsset
	Datasets       map[string]string
	DefaultDataset string
	Prefix         string
}

type RefAsset struct {
	Name    string
	Layer   string
	Dataset string
}

// BuildRefFunc returns a closure that resolves asset names to fully-qualified BigQuery table references.
func BuildRefFunc(ctx RefContext) func(string) (string, error) {
	lookup := make(map[string]RefAsset, len(ctx.Assets))
	for _, a := range ctx.Assets {
		lookup[a.Name] = a
	}

	return func(name string) (string, error) {
		asset, ok := lookup[name]
		if !ok {
			return "", fmt.Errorf("ref(%q): asset not found in pipeline", name)
		}

		dataset := ctx.DefaultDataset
		if asset.Dataset != "" {
			dataset = asset.Dataset
		} else if asset.Layer != "" && ctx.Datasets != nil {
			if ds, found := ctx.Datasets[asset.Layer]; found {
				dataset = ds
			}
		}

		tableName := name
		if ctx.Prefix != "" {
			tableName = ctx.Prefix + name
		}

		return fmt.Sprintf("`%s.%s`", dataset, tableName), nil
	}
}

type ResolvedSource struct {
	ResourceType string
	Project        string
	Identifier     string
}

type SourceContext struct {
	Sources map[string]ResolvedSource
}

// BuildSourceFunc returns a closure that resolves source and table names to fully-qualified references.
func BuildSourceFunc(ctx SourceContext) func(string, string) (string, error) {
	return func(sourceName, tableName string) (string, error) {
		src, ok := ctx.Sources[sourceName]
		if !ok {
			return "", fmt.Errorf("source(%q, %q): source not declared in pipeline", sourceName, tableName)
		}
		return formatSourceRef(src, tableName), nil
	}
}

func formatSourceRef(src ResolvedSource, tableName string) string {
	switch src.ResourceType {
	case "bigquery":
		return fmt.Sprintf("`%s.%s.%s`", src.Project, src.Identifier, tableName)
	case "gcs":
		return fmt.Sprintf("gs://%s/%s", src.Identifier, tableName)
	case "s3":
		return fmt.Sprintf("s3://%s/%s", src.Identifier, tableName)
	case "iceberg":
		return fmt.Sprintf("%s.%s", src.Identifier, tableName)
	case "postgres", "mysql":
		return fmt.Sprintf("%s.%s", src.Identifier, tableName)
	default:
		return fmt.Sprintf("`%s.%s.%s`", src.Project, src.Identifier, tableName)
	}
}

// LoadFunctions reads SQL files from a directory and returns them as template functions with positional arg substitution.
func LoadFunctions(dir string) (template.FuncMap, error) {
	funcs := make(template.FuncMap)

	if dir == "" {
		return funcs, nil
	}

	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return funcs, nil
		}
		return funcs, err
	}
	if !info.IsDir() {
		return funcs, nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return funcs, fmt.Errorf("reading functions dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		name := strings.TrimSuffix(entry.Name(), ".sql")
		content, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return funcs, fmt.Errorf("reading function %s: %w", name, err)
		}

		body := strings.TrimSpace(string(content))
		funcs[name] = func(args ...string) string {
			result := body
			for i, arg := range args {
				placeholder := fmt.Sprintf("$%d", i+1)
				result = strings.ReplaceAll(result, placeholder, arg)
			}
			return result
		}
	}

	return funcs, nil
}

// MergeFuncMaps combines multiple template function maps into one, with later maps taking precedence.
func MergeFuncMaps(maps ...template.FuncMap) template.FuncMap {
	merged := make(template.FuncMap)
	for _, m := range maps {
		for k, v := range m {
			merged[k] = v
		}
	}
	return merged
}
