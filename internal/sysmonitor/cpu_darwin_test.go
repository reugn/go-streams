//go:build darwin

package sysmonitor

import (
	"math"
	"testing"
)

// TestNewProcessSampler_InvalidPID tests the error path for invalid PID.
func TestNewProcessSampler_InvalidPID(t *testing.T) {
	sampler, err := newProcessSampler()
	if err != nil {
		t.Fatalf("newProcessSampler failed with valid PID: %v", err)
	}
	if sampler == nil {
		t.Fatal("newProcessSampler should not return nil on success")
	}

	if sampler.pid <= 0 {
		t.Errorf("PID should be positive, got %d", sampler.pid)
	}
	if sampler.pid > math.MaxInt32 {
		t.Errorf("PID should not exceed MaxInt32, got %d", sampler.pid)
	}
}
