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
