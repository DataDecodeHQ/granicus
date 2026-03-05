package executor

import "strings"

// ErrorCategory classifies a transient error for retry decision-making.
type ErrorCategory string

const (
	// CategoryNone indicates the error is not retryable.
	CategoryNone ErrorCategory = ""

	// CategoryRateLimit covers HTTP 429 and BigQuery rate-limit signals.
	CategoryRateLimit ErrorCategory = "rate_limit"

	// CategoryQuota covers BigQuery/API quota exhaustion errors.
	CategoryQuota ErrorCategory = "quota"

	// CategoryNetwork covers connection-level failures and gRPC UNAVAILABLE.
	CategoryNetwork ErrorCategory = "network"

	// CategoryTimeout covers context deadlines and HTTP 408/504 gateway timeouts.
	CategoryTimeout ErrorCategory = "timeout"

	// CategoryServer covers HTTP 5xx server errors and gRPC INTERNAL.
	CategoryServer ErrorCategory = "server"
)

// rateLimitSignals are substrings that indicate a rate-limit response.
var rateLimitSignals = []string{
	"429",
	"rate limit",
	"rateLimitExceeded",
	"Exceeded rate limits",
	"Too Many Requests",
}

// quotaSignals are substrings that indicate quota exhaustion.
var quotaSignals = []string{
	"quotaExceeded",
	"quota exceeded",
	"RESOURCE_EXHAUSTED",
	"Quota exceeded",
}

// networkSignals are substrings that indicate a network-layer failure.
var networkSignals = []string{
	"connection reset",
	"connection refused",
	"no such host",
	"DNS",
	"dial tcp",
	"UNAVAILABLE",
	"transport is closing",
	"EOF",
	"broken pipe",
}

// timeoutSignals are substrings that indicate a timeout.
var timeoutSignals = []string{
	"context deadline exceeded",
	"deadline exceeded",
	"408",
	"504",
	"Request Timeout",
	"Gateway Timeout",
}

// serverSignals are substrings that indicate a transient server error.
var serverSignals = []string{
	"500",
	"502",
	"503",
	"INTERNAL",
	"Internal Server Error",
	"Bad Gateway",
	"Service Unavailable",
	"backendError",
	"internalError",
}

// ClassifyError returns the ErrorCategory for the given error message.
// Returns CategoryNone if the error is not considered retryable.
func ClassifyError(errMsg string) ErrorCategory {
	// Check rate limit first: it overlaps with server (429 vs 5xx numbering).
	for _, sig := range rateLimitSignals {
		if strings.Contains(errMsg, sig) {
			return CategoryRateLimit
		}
	}
	for _, sig := range quotaSignals {
		if strings.Contains(errMsg, sig) {
			return CategoryQuota
		}
	}
	for _, sig := range networkSignals {
		if strings.Contains(errMsg, sig) {
			return CategoryNetwork
		}
	}
	for _, sig := range timeoutSignals {
		if strings.Contains(errMsg, sig) {
			return CategoryTimeout
		}
	}
	for _, sig := range serverSignals {
		if strings.Contains(errMsg, sig) {
			return CategoryServer
		}
	}
	return CategoryNone
}

// isRetryableError returns true if the error message indicates a transient
// failure that warrants a retry attempt.
func isRetryableError(errMsg string) bool {
	return ClassifyError(errMsg) != CategoryNone
}

// isRetryableForPolicy returns true if the error matches a category in the
// given retryableErrors list. If the list is empty, falls back to isRetryableError.
func isRetryableForPolicy(errMsg string, retryableErrors []string) bool {
	if len(retryableErrors) == 0 {
		return isRetryableError(errMsg)
	}
	cat := ClassifyError(errMsg)
	if cat == CategoryNone {
		return false
	}
	for _, allowed := range retryableErrors {
		if string(cat) == allowed {
			return true
		}
	}
	return false
}
