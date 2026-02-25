package runner

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

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

func MergeFuncMaps(maps ...template.FuncMap) template.FuncMap {
	merged := make(template.FuncMap)
	for _, m := range maps {
		for k, v := range m {
			merged[k] = v
		}
	}
	return merged
}
