package graph

import (
	"bufio"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Directives struct {
	DependsOn    []string `yaml:"depends_on"`
	TimeColumn   string   `yaml:"time_column"`
	IntervalUnit string   `yaml:"interval_unit"`
	Lookback     int      `yaml:"lookback"`
	StartDate    string   `yaml:"start_date"`
	BatchSize    int      `yaml:"batch_size"`
	Produces     []string `yaml:"produces"`
}

type directivesRoot struct {
	Granicus Directives `yaml:"granicus"`
}

func ParseDirectives(filePath string) (Directives, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return Directives{}, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var yamlLines []string
	inBlock := false
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		if lineNum > maxScanLines {
			break
		}

		line := scanner.Text()
		stripped := stripCommentPrefix(line)

		if !inBlock {
			trimmed := strings.TrimSpace(stripped)
			if trimmed == "granicus:" {
				inBlock = true
				yamlLines = append(yamlLines, stripped)
			}
			continue
		}

		// In block: check if line is still a comment line
		if !isCommentLine(line) {
			break
		}
		// Check if the content after stripping is indented (belongs to the YAML block)
		if stripped == "" || stripped[0] == ' ' || stripped[0] == '\t' {
			yamlLines = append(yamlLines, stripped)
		} else {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return Directives{}, err
	}

	if len(yamlLines) == 0 {
		return Directives{}, nil
	}

	yamlText := strings.Join(yamlLines, "\n")
	var root directivesRoot
	if err := yaml.Unmarshal([]byte(yamlText), &root); err != nil {
		return Directives{}, err
	}

	return root.Granicus, nil
}

func isCommentLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	return strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "#")
}

func stripCommentPrefix(line string) string {
	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "-- ") {
		return trimmed[3:]
	}
	if trimmed == "--" {
		return ""
	}
	if strings.HasPrefix(trimmed, "# ") {
		return trimmed[2:]
	}
	if trimmed == "#" {
		return ""
	}
	return trimmed
}
