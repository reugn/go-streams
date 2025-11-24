//go:build linux

package sysmonitor

import (
	"testing"
)

// TestProcessSampler_ClockTicks tests that clock ticks are properly initialized
func TestProcessSampler_ClockTicks(t *testing.T) {
	sampler, err := NewProcessSampler()
	if err != nil {
		t.Fatalf("NewProcessSampler failed: %v", err)
	}

	ps := sampler.(*ProcessSampler)
	if ps.clockTicks <= 0 {
		t.Errorf("clockTicks should be positive, got %d", ps.clockTicks)
	}

	// Clock ticks should typically be 100 on Linux systems
	// but can vary, so we just check it's reasonable
	if ps.clockTicks > 10000 {
		t.Errorf("clockTicks seems unreasonably high: %d", ps.clockTicks)
	}
}
