package server

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"strings"
	"text/template"
	"time"

	"github.com/Andrew-DataDecode/Granicus/internal/events"
)

type AlertConfig struct {
	Type         string `yaml:"type"`
	URL          string `yaml:"url"`
	Method       string `yaml:"method,omitempty"`
	BodyTemplate string `yaml:"body_template,omitempty"`
}

type AlertData struct {
	Pipeline  string
	Error     string
	RunID     string
	Timestamp string
	Failed    int
	Succeeded int
	Skipped   int
}

type AlertManager struct {
	configs    []AlertConfig
	client     *http.Client
	eventStore *events.Store
}

func NewAlertManager(configs []AlertConfig, eventStore *events.Store) *AlertManager {
	return &AlertManager{
		configs:    configs,
		client:     &http.Client{Timeout: 10 * time.Second},
		eventStore: eventStore,
	}
}

func (m *AlertManager) SendFailureAlerts(data AlertData) {
	for _, cfg := range m.configs {
		go m.sendAlert(cfg, data)
	}
}

func (m *AlertManager) sendAlert(cfg AlertConfig, data AlertData) {
	method := cfg.Method
	if method == "" {
		method = "POST"
	}

	body := cfg.BodyTemplate
	if body == "" {
		body = fmt.Sprintf(`{"pipeline":"%s","run_id":"%s","error":"%s","timestamp":"%s"}`,
			data.Pipeline, data.RunID, data.Error, data.Timestamp)
	} else {
		tmpl, err := template.New("alert").Parse(body)
		if err != nil {
			log.Printf("alert: template parse error: %v", err)
			return
		}
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, data); err != nil {
			log.Printf("alert: template exec error: %v", err)
			return
		}
		body = buf.String()
	}

	req, err := http.NewRequest(method, cfg.URL, strings.NewReader(body))
	if err != nil {
		log.Printf("alert: request error: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.client.Do(req)
	if err != nil {
		log.Printf("alert: send error for %s: %v", cfg.URL, err)
		return
	}
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		log.Printf("alert: webhook %s returned %d", cfg.URL, resp.StatusCode)
	}

	if m.eventStore != nil {
		severity := "info"
		if resp.StatusCode >= 400 {
			severity = "warning"
		}
		_ = m.eventStore.Emit(events.Event{
			RunID: data.RunID, Pipeline: data.Pipeline,
			EventType: "alert_sent", Severity: severity,
			Summary: fmt.Sprintf("Alert sent to %s (status %d)", cfg.URL, resp.StatusCode),
			Details: map[string]any{
				"webhook_url":  cfg.URL,
				"status_code":  resp.StatusCode,
				"alert_type":   cfg.Type,
				"failed_count": data.Failed,
			},
		})
	}
}
