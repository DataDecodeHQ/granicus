package server

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"text/template"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
)

// AlertData is the template data model populated when sending alerts.
type AlertData struct {
	Pipeline     string
	RunID        string
	Status       string
	Summary      string
	Duration     float64
	FailedAssets []string
	ErrorMessage string
	Timestamp    string
	Environment  string
	TotalCost    float64
	// Counts for template use.
	Failed    int
	Succeeded int
	Skipped   int
}

// AlertManager routes alerts to webhooks based on severity using AlertRoutingConfig.
type AlertManager struct {
	routing    *config.AlertRoutingConfig
	client     *http.Client
	eventStore *events.Store
}

// NewAlertManager creates an AlertManager that routes by severity.
// routing may be nil, in which case all Send calls are no-ops.
func NewAlertManager(routing *config.AlertRoutingConfig, eventStore *events.Store) *AlertManager {
	return &AlertManager{
		routing:    routing,
		client:     &http.Client{Timeout: 10 * time.Second},
		eventStore: eventStore,
	}
}

// SendAlerts dispatches an alert to the webhook configured for the given severity.
// Falls back to the default webhook if no severity-specific config is set.
// No-op if routing is nil or no applicable webhook is configured.
func (m *AlertManager) SendAlerts(severity string, data AlertData) {
	if m.routing == nil {
		return
	}
	cfg := m.routing.Resolve(severity)
	if cfg == nil {
		return
	}
	go m.sendAlert(cfg, severity, data)
}

// SendFailureAlerts dispatches an alert with severity "error".
func (m *AlertManager) SendFailureAlerts(data AlertData) {
	m.SendAlerts("error", data)
}

func (m *AlertManager) sendAlert(cfg *config.AlertSeverityConfig, severity string, data AlertData) {
	body, err := renderAlertBody(cfg.Template, data)
	if err != nil {
		slog.Error("alert template error", "error", err)
		return
	}

	req, err := http.NewRequest("POST", cfg.URL, strings.NewReader(body))
	if err != nil {
		slog.Error("alert request error", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.client.Do(req)
	if err != nil {
		slog.Error("alert send failed", "url", cfg.URL, "error", err)
		return
	}
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		slog.Warn("alert webhook error", "url", cfg.URL, "status", resp.StatusCode)
	}

	if m.eventStore != nil {
		eventSeverity := "info"
		if resp.StatusCode >= 400 {
			eventSeverity = "warning"
		}
		_ = m.eventStore.Emit(events.Event{
			RunID:     data.RunID,
			Pipeline:  data.Pipeline,
			EventType: "alert_sent",
			Severity:  eventSeverity,
			Summary:   fmt.Sprintf("Alert sent to %s (status %d)", cfg.URL, resp.StatusCode),
			Details: map[string]any{
				"webhook_url":    cfg.URL,
				"status_code":    resp.StatusCode,
				"alert_severity": severity,
				"failed_count":   data.Failed,
			},
		})
	}
}

// SendRunSummary sends a post-run summary notification at default severity.
func (m *AlertManager) SendRunSummary(data AlertData) {
	if m.routing == nil {
		return
	}
	severity := "info"
	if data.Failed > 0 {
		severity = "error"
	}
	m.SendAlerts(severity, data)
}

// BuildRunSummary creates AlertData from run results.
func BuildRunSummary(pipeline, runID string, results []struct {
	AssetName string
	Status    string
	Error     string
	Duration  float64
	Cost      float64
}, totalDuration float64) AlertData {
	var succeeded, failed, skipped int
	var failedAssets []string
	var errorSummary []string
	var totalCost float64

	for _, r := range results {
		switch r.Status {
		case "success":
			succeeded++
		case "failed":
			failed++
			failedAssets = append(failedAssets, r.AssetName)
			if r.Error != "" {
				msg := r.Error
				if len(msg) > 200 {
					msg = msg[:200]
				}
				errorSummary = append(errorSummary, r.AssetName+": "+msg)
			}
		case "skipped":
			skipped++
		}
		totalCost += r.Cost
	}

	status := "success"
	if failed > 0 {
		status = "completed_with_failures"
	}

	summary := fmt.Sprintf("%s: %d succeeded, %d failed, %d skipped (%.0fs)",
		pipeline, succeeded, failed, skipped, totalDuration)

	return AlertData{
		Pipeline:     pipeline,
		RunID:        runID,
		Status:       status,
		Summary:      summary,
		Duration:     totalDuration,
		FailedAssets: failedAssets,
		ErrorMessage: strings.Join(errorSummary, "; "),
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
		TotalCost:    totalCost,
		Failed:       failed,
		Succeeded:    succeeded,
		Skipped:      skipped,
	}
}

// defaultAlertTemplate is used when no template is configured.
const defaultAlertTemplate = `{"pipeline":"{{.Pipeline}}","run_id":"{{.RunID}}","status":"{{.Status}}","error":"{{.ErrorMessage}}","timestamp":"{{.Timestamp}}"}`

func renderAlertBody(tmplStr string, data AlertData) (string, error) {
	if tmplStr == "" {
		tmplStr = defaultAlertTemplate
	}
	tmpl, err := template.New("alert").Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("template parse error: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("template exec error: %w", err)
	}
	return buf.String(), nil
}
