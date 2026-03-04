package monitor

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/Andrew-DataDecode/Granicus/internal/executor"
)

func CollectCheckErrors(dbPath, pipeline string, run *executor.RunResult) error {
	var errors []CurrentError
	runAt := run.EndTime.UTC().Format(time.RFC3339)

	for _, r := range run.Results {
		asset, checkName, ok := parseCheckNode(r.AssetName)
		if !ok {
			continue
		}

		var severity string
		switch r.Status {
		case "failed":
			severity = "error"
		case "skipped":
			severity = "warning"
		default:
			continue
		}

		message := r.Error
		if message == "" {
			message = r.AssetName + " " + r.Status
		}

		details := buildDetails(r)

		errors = append(errors, CurrentError{
			Pipeline:    pipeline,
			Asset:       asset,
			CheckName:   checkName,
			Severity:    severity,
			Message:     message,
			DetailsJSON: details,
			RunAt:       runAt,
		})
	}

	return WriteCurrentErrors(dbPath, errors)
}

func parseCheckNode(name string) (asset, checkName string, ok bool) {
	if !strings.HasPrefix(name, "check:") {
		return "", "", false
	}
	parts := strings.SplitN(name, ":", 3)
	if len(parts) != 3 {
		return "", "", false
	}
	return parts[1], parts[2], true
}

func buildDetails(r executor.NodeResult) string {
	d := map[string]string{
		"status": r.Status,
	}
	if r.Stdout != "" {
		d["stdout"] = r.Stdout
	}
	if r.Stderr != "" {
		d["stderr"] = r.Stderr
	}
	if r.Error != "" {
		d["error"] = r.Error
	}
	for k, v := range r.Metadata {
		d[k] = v
	}

	b, err := json.Marshal(d)
	if err != nil {
		return "{}"
	}
	return string(b)
}
