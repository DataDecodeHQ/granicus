package validate

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/graph"
)

type Status string

const (
	StatusPass  Status = "pass"
	StatusWarn  Status = "warn"
	StatusError Status = "error"
)

type ValidationResult struct {
	Name    string
	Status  Status
	Details map[string]string
	Items   []string
}

type CollectedRef struct {
	AssetName string
	RefName   string
}

type CollectedSource struct {
	AssetName  string
	SourceName string
	TableName  string
}

// CollectingRefFunc returns a template ref() function that validates references and collects them for later analysis.
func CollectingRefFunc(assets []string) (func(string) (string, error), *[]CollectedRef) {
	lookup := make(map[string]bool, len(assets))
	for _, a := range assets {
		lookup[a] = true
	}
	var collected []CollectedRef

	fn := func(name string) (string, error) {
		collected = append(collected, CollectedRef{RefName: name})
		if !lookup[name] {
			return "", fmt.Errorf("ref(%q): asset not found in pipeline", name)
		}
		return "`placeholder." + name + "`", nil
	}
	return fn, &collected
}

// CollectingSourceFunc returns a template source() function that validates source references and collects them for later analysis.
func CollectingSourceFunc(sources map[string]config.SourceConfig) (func(string, string) (string, error), *[]CollectedSource) {
	var collected []CollectedSource

	fn := func(sourceName, tableName string) (string, error) {
		collected = append(collected, CollectedSource{SourceName: sourceName, TableName: tableName})
		src, ok := sources[sourceName]
		if !ok {
			return "", fmt.Errorf("source(%q, %q): source not declared in pipeline", sourceName, tableName)
		}
		return fmt.Sprintf("`placeholder.%s.%s`", src.Identifier, tableName), nil
	}
	return fn, &collected
}

// validateTemplateParse parses and executes all SQL templates in cfg, returning a template_parse ValidationResult.
func validateTemplateParse(cfg *config.PipelineConfig, projectRoot string, funcMap template.FuncMap, data interface{}) ValidationResult {
	var parseErrors []string
	var parsePass []string

	for _, asset := range cfg.Assets {
		if asset.Type != "sql" {
			continue
		}
		sourcePath := filepath.Join(projectRoot, asset.Source)
		rawSQL, err := os.ReadFile(sourcePath)
		if err != nil {
			parseErrors = append(parseErrors, fmt.Sprintf("%s: %v", asset.Name, err))
			continue
		}

		tmpl := template.New(asset.Name).Funcs(funcMap)
		tmpl, err = tmpl.Parse(string(rawSQL))
		if err != nil {
			parseErrors = append(parseErrors, fmt.Sprintf("%s: %v", asset.Name, err))
			continue
		}

		var buf strings.Builder
		if err := tmpl.Execute(&buf, data); err != nil {
			parseErrors = append(parseErrors, fmt.Sprintf("%s: %v", asset.Name, err))
			continue
		}

		parsePass = append(parsePass, asset.Name)
	}

	if len(parseErrors) > 0 {
		return ValidationResult{
			Name:   "template_parse",
			Status: StatusError,
			Items:  parseErrors,
			Details: map[string]string{
				"passed": fmt.Sprintf("%d", len(parsePass)),
				"failed": fmt.Sprintf("%d", len(parseErrors)),
			},
		}
	}
	return ValidationResult{
		Name:   "template_parse",
		Status: StatusPass,
		Details: map[string]string{
			"checked": fmt.Sprintf("%d", len(parsePass)),
		},
	}
}

// validateRefResolution checks that all collected refs resolve to known asset names, returning a ref_resolution ValidationResult.
func validateRefResolution(refs []CollectedRef, assetNames []string) ValidationResult {
	lookup := make(map[string]bool, len(assetNames))
	for _, a := range assetNames {
		lookup[a] = true
	}

	var unresolvedRefs []string
	for _, r := range refs {
		if !lookup[r.RefName] {
			unresolvedRefs = append(unresolvedRefs, fmt.Sprintf("ref(%q): not found", r.RefName))
		}
	}

	if len(unresolvedRefs) > 0 {
		return ValidationResult{
			Name:   "ref_resolution",
			Status: StatusError,
			Items:  unresolvedRefs,
			Details: map[string]string{
				"refs_checked": fmt.Sprintf("%d", len(refs)),
			},
		}
	}
	return ValidationResult{
		Name:   "ref_resolution",
		Status: StatusPass,
		Details: map[string]string{
			"refs_checked": fmt.Sprintf("%d", len(refs)),
		},
	}
}

// ValidateTemplates parses and executes all SQL templates, checking for parse errors and unresolved ref/source calls.
func ValidateTemplates(cfg *config.PipelineConfig, g *graph.Graph, projectRoot string) []ValidationResult {
	var results []ValidationResult

	assetNames := make([]string, 0, len(cfg.Assets))
	for _, a := range cfg.Assets {
		assetNames = append(assetNames, a.Name)
	}
	// Add source phantom node names so ref() to source nodes doesn't fail
	for name := range cfg.Sources {
		assetNames = append(assetNames, "source:"+name)
	}

	refFunc, refs := CollectingRefFunc(assetNames)
	sourceFunc, sources := CollectingSourceFunc(cfg.Sources)

	funcMap := template.FuncMap{
		"ref":    refFunc,
		"source": sourceFunc,
	}

	// Load user functions
	if cfg.FunctionsDir != "" {
		funcDir := cfg.FunctionsDir
		if !filepath.IsAbs(funcDir) {
			funcDir = filepath.Join(projectRoot, funcDir)
		}
		if entries, err := os.ReadDir(funcDir); err == nil {
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
					continue
				}
				name := strings.TrimSuffix(entry.Name(), ".sql")
				content, err := os.ReadFile(filepath.Join(funcDir, entry.Name()))
				if err != nil {
					continue
				}
				body := strings.TrimSpace(string(content))
				funcMap[name] = func(args ...string) string {
					result := body
					for i, arg := range args {
						placeholder := fmt.Sprintf("$%d", i+1)
						result = strings.ReplaceAll(result, placeholder, arg)
					}
					return result
				}
			}
		}
	}

	// Also add cast_to_currency builtin
	funcMap["cast_to_currency"] = func(col string) string {
		return fmt.Sprintf("CAST(ROUND(%s, 2) AS NUMERIC)", col)
	}

	data := struct {
		Project string
		Dataset string
		Prefix  string
	}{
		Project: "placeholder_project",
		Dataset: "placeholder_dataset",
		Prefix:  cfg.Prefix,
	}

	results = append(results, validateTemplateParse(cfg, projectRoot, funcMap, data))
	results = append(results, validateRefResolution(*refs, assetNames))

	// source() resolution
	if len(cfg.Sources) > 0 || len(*sources) > 0 {
		var unresolvedSources []string
		for _, s := range *sources {
			if _, ok := cfg.Sources[s.SourceName]; !ok {
				unresolvedSources = append(unresolvedSources, fmt.Sprintf("source(%q, %q): not declared", s.SourceName, s.TableName))
			}
		}
		if len(unresolvedSources) > 0 {
			results = append(results, ValidationResult{
				Name:   "source_resolution",
				Status: StatusError,
				Items:  unresolvedSources,
				Details: map[string]string{
					"sources_checked": fmt.Sprintf("%d", len(*sources)),
				},
			})
		} else {
			results = append(results, ValidationResult{
				Name:   "source_resolution",
				Status: StatusPass,
				Details: map[string]string{
					"sources_checked": fmt.Sprintf("%d", len(*sources)),
				},
			})
		}
	}

	return results
}

// DetectOrphanFiles finds SQL files in asset directories that are not referenced by any pipeline asset.
func DetectOrphanFiles(cfg *config.PipelineConfig, projectRoot string) []ValidationResult {
	referenced := make(map[string]bool)
	for _, a := range cfg.Assets {
		absPath := filepath.Join(projectRoot, a.Source)
		referenced[absPath] = true
		// Also check for check sources
		for _, c := range a.Checks {
			if c.Source != "" {
				referenced[filepath.Join(projectRoot, c.Source)] = true
			}
		}
	}

	// Infer directories from asset sources
	dirs := make(map[string]bool)
	for _, a := range cfg.Assets {
		dir := filepath.Dir(filepath.Join(projectRoot, a.Source))
		dirs[dir] = true
		// Walk up to find parent sql/ or checks/ dir
		for d := dir; d != projectRoot && d != "/" && d != "."; d = filepath.Dir(d) {
			base := filepath.Base(d)
			if base == "sql" || base == "checks" {
				dirs[d] = true
				break
			}
		}
	}

	var orphans []string
	for dir := range dirs {
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".sql") {
				return nil
			}
			if !referenced[path] {
				rel, _ := filepath.Rel(projectRoot, path)
				orphans = append(orphans, rel)
			}
			return nil
		})
	}

	if len(orphans) > 0 {
		return []ValidationResult{{
			Name:   "orphan_files",
			Status: StatusWarn,
			Items:  orphans,
			Details: map[string]string{
				"orphan_count": fmt.Sprintf("%d", len(orphans)),
			},
		}}
	}
	return []ValidationResult{{
		Name:   "orphan_files",
		Status: StatusPass,
		Details: map[string]string{
			"scanned_dirs": fmt.Sprintf("%d", len(dirs)),
		},
	}}
}

// CheckDependsOnConsistency warns when ref() calls and depends_on declarations are mismatched.
func CheckDependsOnConsistency(cfg *config.PipelineConfig, g *graph.Graph, projectRoot string, collectedRefs []CollectedRef) []ValidationResult {
	// Build per-asset ref usage map
	refsByAsset := make(map[string]map[string]bool)
	for _, r := range collectedRefs {
		if r.AssetName == "" {
			continue
		}
		if refsByAsset[r.AssetName] == nil {
			refsByAsset[r.AssetName] = make(map[string]bool)
		}
		refsByAsset[r.AssetName][r.RefName] = true
	}

	var warnings []string
	for name, asset := range g.Assets {
		if asset.Type == "source" {
			continue
		}
		deps := make(map[string]bool)
		for _, d := range asset.DependsOn {
			deps[d] = true
		}
		refs := refsByAsset[name]

		// ref without depends_on
		for ref := range refs {
			if !deps[ref] {
				warnings = append(warnings, fmt.Sprintf("%s: ref(%q) but not in depends_on", name, ref))
			}
		}

		// depends_on without ref (only if asset has any refs at all)
		if len(refs) > 0 {
			for dep := range deps {
				if !refs[dep] && !strings.HasPrefix(dep, "source:") {
					warnings = append(warnings, fmt.Sprintf("%s: depends_on %q but never ref'd", name, dep))
				}
			}
		}
	}

	if len(warnings) > 0 {
		return []ValidationResult{{
			Name:   "depends_on_consistency",
			Status: StatusWarn,
			Items:  warnings,
		}}
	}
	return []ValidationResult{{
		Name:   "depends_on_consistency",
		Status: StatusPass,
	}}
}

var layerOrder = map[string]int{
	"source":       0,
	"staging":      1,
	"intermediate": 2,
	"entity":       3,
	"analytics":    3,
	"report":       4,
}

// CheckLayerDirection detects dependencies that violate the expected layer ordering (e.g., staging depending on entity).
func CheckLayerDirection(g *graph.Graph) []ValidationResult {
	var violations []string

	for name, asset := range g.Assets {
		myLevel, myOK := layerOrder[asset.Layer]
		if !myOK {
			continue
		}
		for _, dep := range asset.DependsOn {
			depAsset, ok := g.Assets[dep]
			if !ok {
				continue
			}
			depLevel, depOK := layerOrder[depAsset.Layer]
			if !depOK {
				continue
			}
			if myLevel < depLevel {
				violations = append(violations, fmt.Sprintf("%s (layer=%s) depends on %s (layer=%s)", name, asset.Layer, dep, depAsset.Layer))
			}
		}
	}

	if len(violations) > 0 {
		return []ValidationResult{{
			Name:   "layer_direction",
			Status: StatusWarn,
			Items:  violations,
		}}
	}
	return []ValidationResult{{
		Name:   "layer_direction",
		Status: StatusPass,
	}}
}

// CheckDefaultChecks inspects the config to report on auto-generated default checks.
func CheckDefaultChecks(cfg *config.PipelineConfig) []ValidationResult {
	countsByLayer := make(map[string]int)
	var missingGrain []string

	for _, asset := range cfg.Assets {
		if asset.DefaultChecks != nil && !*asset.DefaultChecks {
			continue
		}
		if asset.Layer == "" {
			continue
		}
		if asset.Grain == "" {
			missingGrain = append(missingGrain, asset.Name)
			continue
		}

		switch asset.Layer {
		case "staging":
			countsByLayer["staging"] += 5 // unique_grain, not_null_grain, not_empty, no_future_timestamps, updated_at_gte_created_at
		case "intermediate":
			countsByLayer["intermediate"]++ // unique_grain (+ fan_out/row_retention if upstream)
			if len(asset.Upstream) > 0 || asset.PrimaryUpstream != "" {
				countsByLayer["intermediate"] += 2
			}
		case "entity", "analytics":
			countsByLayer["entity"]++ // unique_grain
		case "report":
			countsByLayer["report"]++ // row_count
		}
		countsByLayer["total"] += countsByLayer[asset.Layer]
	}

	// Count source checks
	for _, src := range cfg.Sources {
		n := 0
		if len(src.Tables) > 0 {
			n += len(src.Tables) // exists_not_empty per table
			if src.ExpectedFresh != "" {
				n++ // freshness once per source
			}
			if src.PrimaryKey != "" && len(src.Tables) == 1 {
				n += 2 // pk_not_null + pk_unique only for single-table
			}
			if len(src.ExpectedColumns) > 0 && len(src.Tables) == 1 {
				n++ // expected_columns only for single-table
			}
		} else if strings.Contains(src.Identifier, ".") {
			n = 1 // exists_not_empty
			if src.PrimaryKey != "" {
				n += 2
			}
			if src.ExpectedFresh != "" {
				n++
			}
			if len(src.ExpectedColumns) > 0 {
				n++
			}
		}
		countsByLayer["source"] += n
	}

	var items []string
	if len(missingGrain) > 0 {
		items = append(items, fmt.Sprintf("assets missing grain (no default checks): %s", strings.Join(missingGrain, ", ")))
	}

	details := make(map[string]string)
	total := 0
	for _, l := range []string{"staging", "intermediate", "entity", "analytics", "report", "source"} {
		if n := countsByLayer[l]; n > 0 {
			details[l] = fmt.Sprintf("%d", n)
			total += n
		}
	}
	details["total"] = fmt.Sprintf("%d", total)

	status := StatusPass
	if len(missingGrain) > 0 {
		status = StatusWarn
	}

	return []ValidationResult{{
		Name:    "default_checks",
		Status:  status,
		Details: details,
		Items:   items,
	}}
}

// CheckSourceContracts inspects source declarations for contract completeness.
func CheckSourceContracts(cfg *config.PipelineConfig) []ValidationResult {
	if len(cfg.Sources) == 0 {
		return nil
	}

	var withContract []string
	var missingContract []string

	for name, src := range cfg.Sources {
		var fields []string
		if src.PrimaryKey != "" {
			fields = append(fields, "primary_key")
		}
		if src.ExpectedFresh != "" {
			fields = append(fields, "expected_freshness")
		}
		if len(src.ExpectedColumns) > 0 {
			fields = append(fields, "expected_columns")
		}

		if len(fields) > 0 {
			withContract = append(withContract, fmt.Sprintf("%s [%s]", name, strings.Join(fields, ", ")))
		} else {
			missingContract = append(missingContract, name)
		}
	}

	var items []string
	if len(missingContract) > 0 {
		items = append(items, fmt.Sprintf("sources without contract declarations: %s", strings.Join(missingContract, ", ")))
	}

	details := map[string]string{
		"total":         fmt.Sprintf("%d", len(cfg.Sources)),
		"with_contract": fmt.Sprintf("%d", len(withContract)),
		"missing":       fmt.Sprintf("%d", len(missingContract)),
	}

	status := StatusPass
	if len(missingContract) > 0 {
		status = StatusWarn
	}

	return []ValidationResult{{
		Name:    "source_contracts",
		Status:  status,
		Details: details,
		Items:   items,
	}}
}

// CheckOrphanedChecks verifies every SQL file in checks/ is wired to an asset.
func CheckOrphanedChecks(cfg *config.PipelineConfig, projectRoot string) []ValidationResult {
	checksDir := filepath.Join(projectRoot, "checks")
	entries, err := os.ReadDir(checksDir)
	if err != nil {
		return nil // no checks dir, nothing to validate
	}

	// Build set of all check sources referenced by assets
	wired := make(map[string]bool)
	for _, a := range cfg.Assets {
		for _, c := range a.Checks {
			wired[c.Source] = true
		}
	}

	var orphans []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		checkSource := filepath.Join("checks", e.Name())
		if !wired[checkSource] {
			orphans = append(orphans, e.Name())
		}
	}

	if len(orphans) > 0 {
		return []ValidationResult{{
			Name:   "orphaned_checks",
			Status: StatusWarn,
			Items:  orphans,
			Details: map[string]string{
				"orphan_count": fmt.Sprintf("%d", len(orphans)),
				"total_checks": fmt.Sprintf("%d", len(entries)),
			},
		}}
	}
	return []ValidationResult{{
		Name:   "orphaned_checks",
		Status: StatusPass,
		Details: map[string]string{
			"wired_checks": fmt.Sprintf("%d", len(wired)),
		},
	}}
}

// Matches {{.Project}}.LITERAL_DATASET.table (with or without backticks) but NOT {{.Project}}.{{.Dataset}}.table
var hardcodedRefPattern = regexp.MustCompile(`\{\{\s*\.Project\s*\}\}\.([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)`)

// DetectHardcodedRefs finds SQL templates that reference datasets directly via {{.Project}}.LITERAL instead of using ref().
func DetectHardcodedRefs(cfg *config.PipelineConfig, projectRoot string) []ValidationResult {
	var violations []string

	for _, asset := range cfg.Assets {
		if asset.Type != "sql" {
			continue
		}
		sourcePath := filepath.Join(projectRoot, asset.Source)
		content, err := os.ReadFile(sourcePath)
		if err != nil {
			continue
		}

		matches := hardcodedRefPattern.FindAllStringSubmatch(string(content), -1)
		for _, m := range matches {
			violations = append(violations, fmt.Sprintf("%s: hardcoded ref `{{.Project}}.%s`", asset.Name, m[1]))
		}
	}

	if len(violations) > 0 {
		return []ValidationResult{{
			Name:   "hardcoded_refs",
			Status: StatusError,
			Items:  violations,
			Details: map[string]string{
				"violation_count": fmt.Sprintf("%d", len(violations)),
			},
		}}
	}
	return []ValidationResult{{
		Name:   "hardcoded_refs",
		Status: StatusPass,
	}}
}