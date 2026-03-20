package runner

import (
	"testing"
	"time"
)

func TestEffectiveTimeout(t *testing.T) {
	tests := []struct {
		name     string
		asset    time.Duration
		runner   time.Duration
		expected time.Duration
	}{
		{"asset overrides runner", 30 * time.Minute, 5 * time.Minute, 30 * time.Minute},
		{"runner used when no asset timeout", 0, 10 * time.Minute, 10 * time.Minute},
		{"default when both zero", 0, 0, DefaultTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := effectiveTimeout(tt.asset, tt.runner)
			if got != tt.expected {
				t.Errorf("effectiveTimeout(%v, %v) = %v, want %v", tt.asset, tt.runner, got, tt.expected)
			}
		})
	}
}
