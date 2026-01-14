//go:build windows

package sysmonitor

import (
	"testing"
	"time"
)

// TestWindowsSamplerIntegration runs real calls against the OS.
func TestWindowsSamplerIntegration(t *testing.T) {
	sampler, err := newPlatformCPUSampler(nil)
	if err != nil {
		t.Fatalf("Failed to create Windows sampler: %v", err)
	}

	// 1. Initialize
	val := sampler.Sample(time.Second)
	if val != 0.0 {
		t.Errorf("Expected initial sample 0.0, got %f", val)
	}

	// 2. Generate Load (Busy Loop) to ensure non-zero CPU
	done := make(chan struct{})
	go func() {
		end := time.Now().Add(100 * time.Millisecond)
		for time.Now().Before(end) {
		}
		close(done)
	}()
	<-done

	// Sleep a tiny bit to ensure total elapsed > busy loop time
	time.Sleep(50 * time.Millisecond)

	// 3. Measure
	val = sampler.Sample(0)

	// Log result (useful for verification)
	t.Logf("Measured CPU Load: %f%%", val)

	if val <= 0.0 {
		t.Error("Expected non-zero CPU usage after busy loop")
	}
	if val > 100.0 {
		t.Errorf("CPU usage %f exceeds 100%%", val)
	}
}

// TestWindowsSamplerReset tests the Reset functionality
func TestWindowsSamplerReset(t *testing.T) {
	sampler, err := newPlatformCPUSampler(nil)
	if err != nil {
		t.Fatalf("Failed to create Windows sampler: %v", err)
	}

	// Take an initial sample to initialize state
	val1 := sampler.Sample(time.Second)
	if val1 != 0.0 {
		t.Errorf("Expected initial sample 0.0, got %f", val1)
	}

	// Verify sampler is initialized
	if !sampler.IsInitialized() {
		t.Error("Expected sampler to be initialized after first sample")
	}

	// Reset the sampler
	sampler.Reset()

	// Verify sampler is no longer initialized
	if sampler.IsInitialized() {
		t.Error("Expected sampler to be uninitialized after reset")
	}

	// Take another sample - should return 0.0 again (like first sample)
	val2 := sampler.Sample(time.Second)
	if val2 != 0.0 {
		t.Errorf("Expected sample after reset to be 0.0, got %f", val2)
	}

	// Verify sampler is now initialized again
	if !sampler.IsInitialized() {
		t.Error("Expected sampler to be initialized after second sample")
	}
}

func TestWindowsSamplerIsInitialized(t *testing.T) {
	sampler, err := newPlatformCPUSampler(nil)
	if err != nil {
		t.Fatalf("Failed to create Windows sampler: %v", err)
	}

	// Initially should not be initialized
	if sampler.IsInitialized() {
		t.Error("Expected new sampler to be uninitialized")
	}

	// After first sample, should be initialized
	sampler.Sample(time.Second)
	if !sampler.IsInitialized() {
		t.Error("Expected sampler to be initialized after first sample")
	}

	// After reset, should be uninitialized again
	sampler.Reset()
	if sampler.IsInitialized() {
		t.Error("Expected sampler to be uninitialized after reset")
	}
}

func TestWindowsSamplerShortInterval(t *testing.T) {
	sampler, err := newPlatformCPUSampler(nil)
	if err != nil {
		t.Fatalf("Failed to create Windows sampler: %v", err)
	}

	// Initialize with first sample
	val1 := sampler.Sample(time.Second)
	if val1 != 0.0 {
		t.Errorf("Expected initial sample 0.0, got %f", val1)
	}

	// Generate some CPU load and take a normal sample
	done := make(chan struct{})
	go func() {
		end := time.Now().Add(50 * time.Millisecond)
		for time.Now().Before(end) {
		}
		close(done)
	}()
	<-done

	time.Sleep(10 * time.Millisecond)              // Ensure some elapsed time
	val2 := sampler.Sample(100 * time.Millisecond) // Normal sample
	t.Logf("Normal sample after load: %f", val2)

	// Now test short interval behavior - sample immediately again with very short delta
	val3 := sampler.Sample(10 * time.Millisecond) // Very short interval (< deltaTime/2 = 5ms)

	// Should return the previous value due to short interval
	if val3 != val2 {
		t.Errorf("Expected same value %f due to short interval (10ms < 50ms), got %f", val2, val3)
	} else {
		t.Logf("Short interval correctly returned cached value: %f", val3)
	}
}

func TestWindowsSamplerConsistency(t *testing.T) {
	sampler, err := newPlatformCPUSampler(nil)
	if err != nil {
		t.Fatalf("Failed to create Windows sampler: %v", err)
	}

	// Initialize
	sampler.Sample(time.Second)

	// Take multiple samples over time
	var samples []float64
	for i := 0; i < 3; i++ {
		time.Sleep(100 * time.Millisecond)
		val := sampler.Sample(50 * time.Millisecond)
		samples = append(samples, val)

		if val < 0.0 || val > 100.0 {
			t.Errorf("Sample %d out of valid range: %f", i, val)
		}
	}

	t.Logf("Consistency test samples: %v", samples)
}

// TestWindowsSamplerBoundaryCalculations tests edge cases in CPU calculations
func TestWindowsSamplerBoundaryCalculations(t *testing.T) {
	sampler, err := newPlatformCPUSampler(nil)
	if err != nil {
		t.Fatalf("Failed to create Windows sampler: %v", err)
	}

	// Test many rapid samples to ensure bounds checking works
	for i := 0; i < 20; i++ {
		val := sampler.Sample(time.Millisecond)
		if val < 0.0 {
			t.Errorf("Sample %d returned negative value: %f", i, val)
		}
		if val > 100.0 {
			t.Errorf("Sample %d exceeded 100%%: %f", i, val)
		}
	}

	// Test with very small delta times
	val := sampler.Sample(time.Nanosecond)
	if val < 0.0 || val > 100.0 {
		t.Errorf("Nanosecond delta sample out of bounds: %f", val)
	}

	// Test reset and immediate sampling
	sampler.Reset()
	val = sampler.Sample(0)
	if val != 0.0 {
		t.Errorf("Expected 0.0 after reset, got %f", val)
	}
}

// TestWindowsSamplerLoadScenarios tests various CPU load scenarios
func TestWindowsSamplerLoadScenarios(t *testing.T) {
	sampler, err := newPlatformCPUSampler(nil)
	if err != nil {
		t.Fatalf("Failed to create Windows sampler: %v", err)
	}

	// Initialize
	sampler.Sample(time.Second)

	// Test 1: No load scenario
	time.Sleep(50 * time.Millisecond)
	val1 := sampler.Sample(100 * time.Millisecond)
	t.Logf("No load CPU: %f%%", val1)

	// Test 2: Light load scenario
	done := make(chan struct{})
	go func() {
		end := time.Now().Add(30 * time.Millisecond)
		for time.Now().Before(end) {
			// Light CPU work
			_ = time.Now().UnixNano()
		}
		close(done)
	}()
	<-done

	time.Sleep(10 * time.Millisecond)
	val2 := sampler.Sample(100 * time.Millisecond)
	t.Logf("Light load CPU: %f%%", val2)

	// Test 3: Moderate load scenario
	done = make(chan struct{})
	go func() {
		end := time.Now().Add(50 * time.Millisecond)
		for time.Now().Before(end) {
			// Moderate CPU work
			for j := 0; j < 1000; j++ {
				_ = j * j
			}
		}
		close(done)
	}()
	<-done

	time.Sleep(10 * time.Millisecond)
	val3 := sampler.Sample(100 * time.Millisecond)
	t.Logf("Moderate load CPU: %f%%", val3)

	// All values should be valid
	for i, val := range []float64{val1, val2, val3} {
		if val < 0.0 || val > 100.0 {
			t.Errorf("Load scenario %d out of bounds: %f", i+1, val)
		}
	}
}

// TestWindowsSamplerTimingBehavior tests timing-related behavior
func TestWindowsSamplerTimingBehavior(t *testing.T) {
	sampler, err := newPlatformCPUSampler(nil)
	if err != nil {
		t.Fatalf("Failed to create Windows sampler: %v", err)
	}

	// Initialize
	start := time.Now()
	sampler.Sample(time.Second)
	initTime := time.Since(start)

	// Test rapid sampling behavior
	for i := 0; i < 5; i++ {
		start := time.Now()
		val := sampler.Sample(50 * time.Millisecond)
		elapsed := time.Since(start)

		// Should complete quickly (less than 10ms typically)
		if elapsed > 100*time.Millisecond {
			t.Errorf("Sample %d took too long: %v", i, elapsed)
		}

		if val < 0.0 || val > 100.0 {
			t.Errorf("Sample %d value out of bounds: %f", i, val)
		}
	}

	t.Logf("Initialization took: %v", initTime)
}
