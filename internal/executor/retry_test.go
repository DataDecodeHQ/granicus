package executor

import "testing"

func TestClassifyError_RateLimit(t *testing.T) {
	cases := []struct {
		name string
		msg  string
	}{
		{"http 429", "googleapi: Error 429: Too Many Requests, rateLimitExceeded"},
		{"bq rate limit phrase", "Error: rate limit exceeded for project"},
		{"bq exceeded rate limits", "Exceeded rate limits: too many api requests per user per 100 seconds"},
		{"rateLimitExceeded code", "Error 429: rateLimitExceeded"},
		{"Too Many Requests text", "Too Many Requests from server"},
		{"429 in message", "received HTTP 429 from backend"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyError(tc.msg)
			if got != CategoryRateLimit {
				t.Errorf("ClassifyError(%q) = %q, want %q", tc.msg, got, CategoryRateLimit)
			}
			if !isRetryableError(tc.msg) {
				t.Errorf("isRetryableError(%q) = false, want true", tc.msg)
			}
		})
	}
}

func TestClassifyError_Quota(t *testing.T) {
	cases := []struct {
		name string
		msg  string
	}{
		{"quotaExceeded code", "Error 403: quotaExceeded"},
		{"quota exceeded phrase", "quota exceeded for project dailyQueryBytesProcessed"},
		{"RESOURCE_EXHAUSTED grpc", "rpc error: code = RESOURCE_EXHAUSTED desc = quota limit reached"},
		{"Quota exceeded cap", "Quota exceeded: Your project exceeded quota for concurrent queries"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyError(tc.msg)
			if got != CategoryQuota {
				t.Errorf("ClassifyError(%q) = %q, want %q", tc.msg, got, CategoryQuota)
			}
			if !isRetryableError(tc.msg) {
				t.Errorf("isRetryableError(%q) = false, want true", tc.msg)
			}
		})
	}
}

func TestClassifyError_Network(t *testing.T) {
	cases := []struct {
		name string
		msg  string
	}{
		{"connection reset", "read tcp: connection reset by peer"},
		{"connection refused", "dial tcp 127.0.0.1:443: connect: connection refused"},
		{"no such host", "dial tcp: lookup bigquery.googleapis.com: no such host"},
		{"DNS lookup", "DNS resolution failed for host"},
		{"dial tcp", "dial tcp: i/o timeout"},
		{"grpc UNAVAILABLE", "rpc error: code = UNAVAILABLE desc = connection dropped"},
		{"transport closing", "transport is closing"},
		{"EOF", "unexpected EOF while reading response"},
		{"broken pipe", "write tcp: broken pipe"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyError(tc.msg)
			if got != CategoryNetwork {
				t.Errorf("ClassifyError(%q) = %q, want %q", tc.msg, got, CategoryNetwork)
			}
			if !isRetryableError(tc.msg) {
				t.Errorf("isRetryableError(%q) = false, want true", tc.msg)
			}
		})
	}
}

func TestClassifyError_Timeout(t *testing.T) {
	cases := []struct {
		name string
		msg  string
	}{
		{"context deadline exceeded", "context deadline exceeded"},
		{"grpc deadline exceeded", "rpc error: code = DeadlineExceeded desc = deadline exceeded"},
		{"http 408", "HTTP 408 Request Timeout from server"},
		{"http 504", "received HTTP 504 Gateway Timeout"},
		{"Request Timeout text", "Request Timeout while waiting for upstream"},
		{"Gateway Timeout text", "Gateway Timeout after 60s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyError(tc.msg)
			if got != CategoryTimeout {
				t.Errorf("ClassifyError(%q) = %q, want %q", tc.msg, got, CategoryTimeout)
			}
			if !isRetryableError(tc.msg) {
				t.Errorf("isRetryableError(%q) = false, want true", tc.msg)
			}
		})
	}
}

func TestClassifyError_Server(t *testing.T) {
	cases := []struct {
		name string
		msg  string
	}{
		{"http 500", "HTTP 500 Internal Server Error"},
		{"http 502", "received 502 Bad Gateway from load balancer"},
		{"http 503", "HTTP 503 Service Unavailable"},
		{"grpc INTERNAL", "rpc error: code = INTERNAL desc = backend error"},
		{"Internal Server Error text", "Internal Server Error from googleapi"},
		{"Bad Gateway text", "Bad Gateway response from upstream"},
		{"Service Unavailable text", "Service Unavailable: try again later"},
		{"backendError code", "Error 500: backendError"},
		{"internalError code", "Error 500: internalError"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyError(tc.msg)
			if got != CategoryServer {
				t.Errorf("ClassifyError(%q) = %q, want %q", tc.msg, got, CategoryServer)
			}
			if !isRetryableError(tc.msg) {
				t.Errorf("isRetryableError(%q) = false, want true", tc.msg)
			}
		})
	}
}

func TestClassifyError_NotRetryable(t *testing.T) {
	cases := []struct {
		name string
		msg  string
	}{
		{"syntax error", "Error 400: Syntax error: Expected end of input but got identifier"},
		{"not found", "Error 404: Not Found"},
		{"permission denied", "Error 403: Access Denied"},
		{"exit status 1", "exit status 1"},
		{"empty", ""},
		{"generic failure", "job failed with unknown error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyError(tc.msg)
			if got != CategoryNone {
				t.Errorf("ClassifyError(%q) = %q, want %q", tc.msg, got, CategoryNone)
			}
			if isRetryableError(tc.msg) {
				t.Errorf("isRetryableError(%q) = true, want false", tc.msg)
			}
		})
	}
}
