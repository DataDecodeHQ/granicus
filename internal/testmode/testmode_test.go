package testmode

import "testing"

func TestTestDatasetName(t *testing.T) {
	tests := []struct {
		base   string
		runID  string
		expect string
	}{
		{"my_dataset", "run-20260225-abcd", "my_dataset__test_abcd"},
		{"ds", "xy", "ds__test_xy"},
		{"ds", "a", "ds__test_a"},
		{"analytics", "run-20260101-1234", "analytics__test_1234"},
	}

	for _, tt := range tests {
		got := TestDatasetName(tt.base, tt.runID)
		if got != tt.expect {
			t.Errorf("TestDatasetName(%q, %q) = %q, want %q", tt.base, tt.runID, got, tt.expect)
		}
	}
}

func TestSanitizeLabel(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		{"run-20260225-abcd", "run_20260225_abcd"},
		{"UPPERCASE", "uppercase"},
		{"short", "short"},
	}

	for _, tt := range tests {
		got := sanitizeLabel(tt.input)
		if got != tt.expect {
			t.Errorf("sanitizeLabel(%q) = %q, want %q", tt.input, got, tt.expect)
		}
	}

	// Test truncation at 63 chars
	long := ""
	for i := 0; i < 70; i++ {
		long += "a"
	}
	got := sanitizeLabel(long)
	if len(got) != 63 {
		t.Errorf("expected 63 chars, got %d", len(got))
	}
}
