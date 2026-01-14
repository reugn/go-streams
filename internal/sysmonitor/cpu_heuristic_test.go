package sysmonitor

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestGoroutineHeuristicSampler(t *testing.T) {
	sampler := NewGoroutineHeuristicSampler()

	// Test 1: Initialization
	if !sampler.IsInitialized() {
		t.Error("Heuristic sampler should always be initialized")
	}

	// Test 2: Reset (should be no-op)
	sampler.Reset()
	if !sampler.IsInitialized() {
		t.Error("Heuristic sampler should verify initialized after reset")
	}

	// Test 3: Baseline Logic
	// We expect at least the baseline CPU usage
	val := sampler.Sample(time.Second)
	if val < CPUHeuristicBaselineCPU {
		t.Errorf("Expected baseline CPU >= %f, got %f", CPUHeuristicBaselineCPU, val)
	}

	// Test 4: Scaling Logic
	// We spawn a significant number of goroutines to force the count up
	// and verify the "CPU usage" increases.
	baseCount := runtime.NumGoroutine()
	targetIncrease := 50 // Add 50 goroutines to trigger scaling

	var wg sync.WaitGroup
	wg.Add(targetIncrease)

	// Channel to keep goroutines alive
	hold := make(chan struct{})

	for i := 0; i < targetIncrease; i++ {
		go func() {
			wg.Done()
			<-hold
		}()
	}

	// Wait for all to start
	wg.Wait()

	// Sample again with higher load
	highLoadVal := sampler.Sample(time.Second)

	// Cleanup
	close(hold)

	if highLoadVal <= val && baseCount < CPUHeuristicMaxGoroutinesForLinear {
		t.Errorf("Expected higher CPU estimate with more goroutines. Low: %f, High: %f", val, highLoadVal)
	}

	if highLoadVal > CPUHeuristicMaxCPU {
		t.Errorf("CPU estimate %f exceeded max cap %f", highLoadVal, CPUHeuristicMaxCPU)
	}

	// Test CPU cap is enforced - spawn many goroutines to trigger cap
	manyGoroutines := 1000 // This should trigger logarithmic scaling and cap
	var wg2 sync.WaitGroup
	wg2.Add(manyGoroutines)

	// Channel to keep goroutines alive
	hold2 := make(chan struct{})

	for i := 0; i < manyGoroutines; i++ {
		go func() {
			wg2.Done()
			<-hold2
		}()
	}

	// Wait for all to start
	wg2.Wait()

	// Sample with many goroutines - should be capped at CPUHeuristicMaxCPU
	cappedVal := sampler.Sample(time.Second)
	t.Logf("CPU estimate with %d goroutines: %f (max allowed: %f)",
		manyGoroutines, cappedVal, CPUHeuristicMaxCPU)
	if cappedVal > CPUHeuristicMaxCPU {
		t.Errorf("CPU estimate %f exceeded max cap %f even with %d goroutines",
			cappedVal, CPUHeuristicMaxCPU, manyGoroutines)
	}

	// Cleanup
	close(hold2)
}
