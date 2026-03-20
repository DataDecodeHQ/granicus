package pool

import (
	"os"
	"strconv"
	"time"
)

// ResourceLimit defines concurrency and ramp-up parameters for a resource type.
type ResourceLimit struct {
	MaxConcurrent int
	InitialSlots  int
	RampStep      int
	RampInterval  time.Duration
	Source        string // "default" or "env"
}

var defaultLimits = map[string]ResourceLimit{
	"bigquery":  {MaxConcurrent: 100, InitialSlots: 5, RampStep: 5, RampInterval: 10 * time.Second, Source: "default"},
	"gcs":       {MaxConcurrent: 200, InitialSlots: 20, RampStep: 20, RampInterval: 5 * time.Second, Source: "default"},
	"postgres":  {MaxConcurrent: 20, InitialSlots: 3, RampStep: 3, RampInterval: 5 * time.Second, Source: "default"},
	"mysql":     {MaxConcurrent: 20, InitialSlots: 3, RampStep: 3, RampInterval: 5 * time.Second, Source: "default"},
	"snowflake": {MaxConcurrent: 50, InitialSlots: 5, RampStep: 5, RampInterval: 10 * time.Second, Source: "default"},
}

// DefaultLimit returns the ResourceLimit for a resource type, applying any
// GRANICUS_MAX_CONCURRENT_<TYPE> env var override.
func DefaultLimit(resourceType string) ResourceLimit {
	limit, ok := defaultLimits[resourceType]
	if !ok {
		limit = ResourceLimit{
			MaxConcurrent: 10,
			InitialSlots:  2,
			RampStep:      2,
			RampInterval:  5 * time.Second,
			Source:        "default",
		}
	}

	envKey := "GRANICUS_MAX_CONCURRENT_" + toUpperSnake(resourceType)
	if envVal := os.Getenv(envKey); envVal != "" {
		if n, err := strconv.Atoi(envVal); err == nil && n > 0 {
			limit.MaxConcurrent = n
			limit.Source = "env"
		}
	}

	return limit
}

// AllDefaults returns the default limits for all known resource types.
func AllDefaults() map[string]ResourceLimit {
	result := make(map[string]ResourceLimit, len(defaultLimits))
	for t := range defaultLimits {
		result[t] = DefaultLimit(t)
	}
	return result
}

func toUpperSnake(s string) string {
	var out []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		out = append(out, c)
	}
	return string(out)
}
