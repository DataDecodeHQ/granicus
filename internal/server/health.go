package server

import (
	"encoding/json"
	"net/http"
	"time"
)

type HealthResponse struct {
	Status          string  `json:"status"`
	UptimeSeconds   float64 `json:"uptime_seconds"`
	ActiveRuns      float64 `json:"active_runs"`
	PipelinesLoaded int     `json:"pipelines_loaded"`
}

// HealthHandler writes a JSON health check response including uptime and pipeline count.
func HealthHandler(w http.ResponseWriter, r *http.Request, startedAt time.Time, pipelinesLoaded int) {
	activeRuns := 0.0
	// Read from prometheus gauge if available
	// ActiveRuns metric is updated by executePipeline

	resp := HealthResponse{
		Status:          "ok",
		UptimeSeconds:   time.Since(startedAt).Seconds(),
		ActiveRuns:      activeRuns,
		PipelinesLoaded: pipelinesLoaded,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
