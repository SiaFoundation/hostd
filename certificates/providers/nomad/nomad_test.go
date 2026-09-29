package nomad

import (
	"math"
	"testing"
	"time"
)

// TestBackoff is a regression test for the exponential backoff overflowing a
// time.Duration after 28 consecutive failures, which retried immediately
// instead of waiting.
func TestBackoff(t *testing.T) {
	tests := []struct {
		failures int
		expected time.Duration
	}{
		{0, 0},
		{1, 2 * time.Minute},
		{8, 256 * time.Minute},
		{9, maxBackoff},
		{27, maxBackoff},
		{28, maxBackoff},
		{63, maxBackoff},
		{64, maxBackoff},
		{1000, maxBackoff},
		{math.MaxInt, maxBackoff},
		{-1, 0},
	}
	for _, test := range tests {
		if got := backoff(test.failures); got != test.expected {
			t.Fatalf("%d failures: expected %v, got %v", test.failures, test.expected, got)
		}
	}
}
