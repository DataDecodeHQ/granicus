package server

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	RunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "granicus_runs_total",
			Help: "Total pipeline runs",
		},
		[]string{"pipeline", "status"},
	)
	RunDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "granicus_run_duration_seconds",
			Help:    "Pipeline run duration",
			Buckets: prometheus.ExponentialBuckets(1, 2, 15),
		},
		[]string{"pipeline"},
	)
	NodeDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "granicus_node_duration_seconds",
			Help:    "Asset node execution duration",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 15),
		},
		[]string{"pipeline", "asset", "status"},
	)
	NodesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "granicus_nodes_total",
			Help: "Total asset node executions",
		},
		[]string{"pipeline", "status"},
	)
	ActiveRuns = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "granicus_active_runs",
			Help: "Number of currently running pipelines",
		},
	)
)

func init() {
	prometheus.MustRegister(RunsTotal, RunDuration, NodeDuration, NodesTotal, ActiveRuns)
}

func MetricsHandler() http.Handler {
	return promhttp.Handler()
}
