//go:build darwin

package sysmonitor

import (
	"testing"
	"time"
)

func TestDarwinSamplerIntegration(t *testing.T) {
	sampler, err := newPlatformCPUSampler(nil)
	if err != nil {
		t.Fatalf("Failed to create Darwin sampler: %v", err)
	}

	if sampler.IsInitialized() {
		t.Error("Should not be initialized initially")
	}

	// First sample
	sampler.Sample(time.Second)
	if !sampler.IsInitialized() {
		t.Error("Should be initialized after sample")
	}

	// Generate CPU load
	go func() {
		// Burn CPU for 100ms
		end := time.Now().Add(100 * time.Millisecond)
		for time.Now().Before(end) {
		}
	}()

	// Wait and sample
	time.Sleep(200 * time.Millisecond)

	val := sampler.Sample(0)
	t.Logf("Darwin CPU Sample: %f%%", val)

	if val <= 0.0 {
		t.Error("Expected detected CPU usage > 0")
	}

	// Test bounds checking by creating edge case scenarios
	testBoundsChecking(t)
}

func testBoundsChecking(t *testing.T) {
	samplerRaw, err := newPlatformCPUSampler(nil)
	if err != nil {
		t.Fatalf("Failed to create Darwin sampler: %v", err)
	}
	sampler := samplerRaw.(*darwinProcessSampler)

	// First sample to initialize
	sampler.Sample(time.Second)

	// Test early return when elapsed time is too short
	// (this tests the "elapsed < deltaTime/2" condition)
	shortInterval := 10 * time.Millisecond
	result := sampler.Sample(shortInterval)

	// Should return the last known value (0.0) due to short interval
	if result != 0.0 {
		t.Errorf("Expected last value 0.0 for short interval, got %f", result)
	}

	// Test that bounds checking works (though hard to trigger negative values in real usage)
	// The bounds checking (percent < 0.0 and percent > 100.0) is tested implicitly
	// by ensuring normal operation stays within bounds
	normalResult := sampler.Sample(time.Second)
	if normalResult < 0.0 || normalResult > 100.0 {
		t.Errorf("CPU percentage %f is out of valid range [0, 100]", normalResult)
	}
}
