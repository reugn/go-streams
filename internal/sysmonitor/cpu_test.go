package sysmonitor

import (
	"runtime"
	"testing"
	"time"
)

func TestGoroutineHeuristicSampler(t *testing.T) {
	sampler := NewGoroutineHeuristicSampler()

	// Test with reasonable goroutine count
	_ = runtime.NumGoroutine()

	percent := sampler.Sample(100 * time.Millisecond)
	if percent < 0.0 || percent > 100.0 {
		t.Errorf("CPU percent should be between 0 and 100, got %v", percent)
	}

	// Test reset (should be no-op)
	sampler.Reset()
}

func TestNewProcessSampler(t *testing.T) {
	sampler, err := NewProcessSampler()
	if err != nil {
		t.Fatalf("NewProcessSampler failed: %v", err)
	}
	if sampler == nil {
		t.Fatal("NewProcessSampler should not be nil")
	}

	// Test basic interface compliance
	_ = sampler.Sample(100 * time.Millisecond)
	sampler.Reset()
	_ = sampler.IsInitialized()
}

func TestProcessSampler_Initialization(t *testing.T) {
	sampler, err := NewProcessSampler()
	if err != nil {
		t.Fatalf("NewProcessSampler failed: %v", err)
	}
	if sampler == nil {
		t.Fatal("NewProcessSampler should not be nil")
	}

	// Initially not initialized
	if sampler.IsInitialized() {
		t.Error("ProcessSampler should not be initialized initially")
	}

	// After first sample, should be initialized
	sampler.Sample(100 * time.Millisecond)
	if !sampler.IsInitialized() {
		t.Error("ProcessSampler should be initialized after first sample")
	}

	// After reset, should not be initialized
	sampler.Reset()
	if sampler.IsInitialized() {
		t.Error("ProcessSampler should not be initialized after reset")
	}
}

func TestProcessSampler_Sample(t *testing.T) {
	sampler, err := NewProcessSampler()
	if err != nil {
		t.Fatalf("NewProcessSampler failed: %v", err)
	}

	// First sample should return 0 and initialize
	percent := sampler.Sample(100 * time.Millisecond)
	if percent != 0.0 {
		t.Errorf("first sample should return 0.0, got %v", percent)
	}

	// Subsequent samples should be valid
	time.Sleep(10 * time.Millisecond)
	percent = sampler.Sample(10 * time.Millisecond)
	if percent < 0.0 || percent > 100.0 {
		t.Errorf("CPU percent should be between 0 and 100, got %v", percent)
	}

	// Test reset
	sampler.Reset()
	if sampler.IsInitialized() {
		t.Error("sampler should not be initialized after reset")
	}
}
