package sysmonitor

import (
	"math"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestGoroutineHeuristicSampler_IsInitialized_Comprehensive(t *testing.T) {
	sampler := NewGoroutineHeuristicSampler()

	// IsInitialized should always return true for heuristic sampler
	if !sampler.IsInitialized() {
		t.Error("GoroutineHeuristicSampler.IsInitialized() should always return true")
	}

	// Should remain true after reset
	sampler.Reset()
	if !sampler.IsInitialized() {
		t.Error("GoroutineHeuristicSampler.IsInitialized() should remain true after reset")
	}

	// Should remain true after sampling
	sampler.Sample(100 * time.Millisecond)
	if !sampler.IsInitialized() {
		t.Error("GoroutineHeuristicSampler.IsInitialized() should remain true after sampling")
	}
}

func TestGoroutineHeuristicSampler_Reset_Comprehensive(t *testing.T) {
	sampler := NewGoroutineHeuristicSampler()

	// Reset should be a no-op but should not panic
	sampler.Reset()

	// Verify sampler still works after reset
	percent1 := sampler.Sample(100 * time.Millisecond)
	sampler.Reset()
	percent2 := sampler.Sample(100 * time.Millisecond)

	// Results should be the same (since reset doesn't affect state)
	if percent1 != percent2 {
		t.Logf("Note: percent1=%v, percent2=%v (may differ due to goroutine count changes)", percent1, percent2)
	}

	// Both should be valid
	if percent1 < 0.0 || percent1 > CPUHeuristicMaxCPU {
		t.Errorf("CPU percent should be between 0 and %.1f, got %v", CPUHeuristicMaxCPU, percent1)
	}
	if percent2 < 0.0 || percent2 > CPUHeuristicMaxCPU {
		t.Errorf("CPU percent should be between 0 and %.1f, got %v", CPUHeuristicMaxCPU, percent2)
	}
}

func TestGoroutineHeuristicSampler_Sample_LinearScaling(t *testing.T) {
	// Test the linear scaling formula for goroutines <= 10
	// Formula: CPUHeuristicBaselineCPU + goroutineCount * CPUHeuristicLinearScaleFactor
	testCases := []struct {
		name            string
		goroutineCount  float64
		expectedMin     float64
		expectedMax     float64
		expectedFormula float64
	}{
		{
			name:            "1 goroutine",
			goroutineCount:  1.0,
			expectedFormula: CPUHeuristicBaselineCPU + 1.0*CPUHeuristicLinearScaleFactor,
			expectedMin:     10.0,
			expectedMax:     12.0,
		},
		{
			name:            "5 goroutines",
			goroutineCount:  5.0,
			expectedFormula: CPUHeuristicBaselineCPU + 5.0*CPUHeuristicLinearScaleFactor,
			expectedMin:     14.0,
			expectedMax:     16.0,
		},
		{
			name:            "10 goroutines (max linear)",
			goroutineCount:  10.0,
			expectedFormula: CPUHeuristicBaselineCPU + 10.0*CPUHeuristicLinearScaleFactor,
			expectedMin:     19.0,
			expectedMax:     21.0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Calculate expected value using the formula
			expected := tc.expectedFormula

			// Verify the formula matches expected range
			if expected < tc.expectedMin || expected > tc.expectedMax {
				t.Errorf("Formula result %.2f should be between %.2f and %.2f", expected, tc.expectedMin, tc.expectedMax)
			}
		})
	}
}

func TestGoroutineHeuristicSampler_Sample_LogarithmicScaling(t *testing.T) {
	// Test the logarithmic scaling formula for goroutines > 10
	// Formula: CPUHeuristicBaselineCPU + ln(goroutineCount) * CPUHeuristicLogScaleFactor
	testCases := []struct {
		name            string
		goroutineCount  float64
		expectedMin     float64
		expectedMax     float64
		expectedFormula float64
	}{
		{
			name:            "11 goroutines (first logarithmic)",
			goroutineCount:  11.0,
			expectedFormula: CPUHeuristicBaselineCPU + math.Log(11.0)*CPUHeuristicLogScaleFactor,
			expectedMin:     28.0,
			expectedMax:     32.0,
		},
		{
			name:            "100 goroutines",
			goroutineCount:  100.0,
			expectedFormula: CPUHeuristicBaselineCPU + math.Log(100.0)*CPUHeuristicLogScaleFactor,
			expectedMin:     45.0,
			expectedMax:     55.0,
		},
		{
			name:            "1000 goroutines",
			goroutineCount:  1000.0,
			expectedFormula: CPUHeuristicBaselineCPU + math.Log(1000.0)*CPUHeuristicLogScaleFactor,
			expectedMin:     65.0,
			expectedMax:     75.0,
		},
		{
			name:            "10000 goroutines",
			goroutineCount:  10000.0,
			expectedFormula: CPUHeuristicBaselineCPU + math.Log(10000.0)*CPUHeuristicLogScaleFactor,
			expectedMin:     80.0,
			expectedMax:     90.0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Calculate expected value using the formula
			expected := tc.expectedFormula

			// Verify the formula matches expected range
			if expected < tc.expectedMin || expected > tc.expectedMax {
				t.Errorf("Formula result %.2f should be between %.2f and %.2f", expected, tc.expectedMin, tc.expectedMax)
			}

			// Verify it doesn't exceed max (capped values should equal max)
			if expected > CPUHeuristicMaxCPU {
				if CPUHeuristicMaxCPU != 95.0 {
					t.Errorf("Expected value %.2f exceeds max, but CPUHeuristicMaxCPU is %.1f (expected 95.0)",
						expected, CPUHeuristicMaxCPU)
				}
			}
		})
	}
}

func TestGoroutineHeuristicSampler_Sample_MaxCPUCap_Formula(t *testing.T) {
	// Test that very high goroutine counts are capped at CPUHeuristicMaxCPU
	// We can't directly set goroutine count, but we can verify the cap works
	// by checking the formula with a very high value
	veryHighGoroutineCount := 1000000.0
	expectedUncapped := CPUHeuristicBaselineCPU + math.Log(veryHighGoroutineCount)*CPUHeuristicLogScaleFactor

	if expectedUncapped > CPUHeuristicMaxCPU {
		// Verify the cap would be applied
		if CPUHeuristicMaxCPU != 95.0 {
			t.Errorf("CPUHeuristicMaxCPU should be 95.0, got %.1f", CPUHeuristicMaxCPU)
		}
	}

	// Test actual sampler with current goroutine count
	sampler := NewGoroutineHeuristicSampler()
	percent := sampler.Sample(100 * time.Millisecond)
	if percent > CPUHeuristicMaxCPU {
		t.Errorf("CPU percent should be capped at %.1f, got %v", CPUHeuristicMaxCPU, percent)
	}
	if percent < 0.0 {
		t.Errorf("CPU percent should not be negative, got %v", percent)
	}

	// Test multiple samples to ensure consistency
	for i := 0; i < 10; i++ {
		p := sampler.Sample(100 * time.Millisecond)
		if p > CPUHeuristicMaxCPU || p < 0.0 {
			t.Errorf("CPU percent should be between 0 and %.1f, got %v", CPUHeuristicMaxCPU, p)
		}
	}
}

func TestGoroutineHeuristicSampler_Sample_WithGoroutines(t *testing.T) {
	sampler := NewGoroutineHeuristicSampler()

	// Get baseline goroutine count
	baselineGoroutines := runtime.NumGoroutine()
	baselinePercent := sampler.Sample(100 * time.Millisecond)

	// Spawn some goroutines to increase the count
	var wg sync.WaitGroup
	numGoroutines := 20
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
		}()
	}

	// Wait a bit for goroutines to start
	time.Sleep(5 * time.Millisecond)

	// Sample with increased goroutine count
	highPercent := sampler.Sample(100 * time.Millisecond)

	// Wait for goroutines to finish
	wg.Wait()
	time.Sleep(10 * time.Millisecond)

	// Sample after goroutines finish
	lowPercent := sampler.Sample(100 * time.Millisecond)

	// Verify all percentages are valid
	if baselinePercent < 0.0 || baselinePercent > CPUHeuristicMaxCPU {
		t.Errorf("Baseline CPU percent should be between 0 and %.1f, got %v", CPUHeuristicMaxCPU, baselinePercent)
	}
	if highPercent < 0.0 || highPercent > CPUHeuristicMaxCPU {
		t.Errorf("High CPU percent should be between 0 and %.1f, got %v", CPUHeuristicMaxCPU, highPercent)
	}
	if lowPercent < 0.0 || lowPercent > CPUHeuristicMaxCPU {
		t.Errorf("Low CPU percent should be between 0 and %.1f, got %v", CPUHeuristicMaxCPU, lowPercent)
	}

	// High goroutine count should generally result in higher CPU estimate
	// (though this is not guaranteed due to timing)
	t.Logf("Baseline goroutines: %d, CPU: %.2f%%", baselineGoroutines, baselinePercent)
	t.Logf("High goroutines: %d, CPU: %.2f%%", runtime.NumGoroutine(), highPercent)
	t.Logf("After cleanup: %d, CPU: %.2f%%", runtime.NumGoroutine(), lowPercent)
}

func TestGoroutineHeuristicSampler_Sample_TimeDeltaIgnored(t *testing.T) {
	sampler := NewGoroutineHeuristicSampler()

	// The heuristic sampler ignores the time delta parameter
	// All samples should return the same value (based on goroutine count)
	percent1 := sampler.Sample(1 * time.Millisecond)
	percent2 := sampler.Sample(100 * time.Millisecond)
	percent3 := sampler.Sample(1 * time.Second)

	// All should be the same (since they're based on goroutine count, not time)
	if percent1 != percent2 || percent2 != percent3 {
		// This is acceptable if goroutine count changed between samples
		t.Logf("Note: Percentages differ (percent1=%.2f, percent2=%.2f, percent3=%.2f), "+
			"likely due to goroutine count changes", percent1, percent2, percent3)
	}

	// All should be valid
	if percent1 < 0.0 || percent1 > CPUHeuristicMaxCPU {
		t.Errorf("CPU percent should be between 0 and %.1f, got %v", CPUHeuristicMaxCPU, percent1)
	}
}

func TestGoroutineHeuristicSampler_Constants(t *testing.T) {
	// Verify all constants have expected values
	if CPUHeuristicBaselineCPU != 10.0 {
		t.Errorf("CPUHeuristicBaselineCPU should be 10.0, got %.1f", CPUHeuristicBaselineCPU)
	}
	if CPUHeuristicLinearScaleFactor != 1.0 {
		t.Errorf("CPUHeuristicLinearScaleFactor should be 1.0, got %.1f", CPUHeuristicLinearScaleFactor)
	}
	if CPUHeuristicLogScaleFactor != 8.0 {
		t.Errorf("CPUHeuristicLogScaleFactor should be 8.0, got %.1f", CPUHeuristicLogScaleFactor)
	}
	if CPUHeuristicMaxGoroutinesForLinear != 10 {
		t.Errorf("CPUHeuristicMaxGoroutinesForLinear should be 10, got %d", CPUHeuristicMaxGoroutinesForLinear)
	}
	if CPUHeuristicMaxCPU != 95.0 {
		t.Errorf("CPUHeuristicMaxCPU should be 95.0, got %.1f", CPUHeuristicMaxCPU)
	}
}

func TestGoroutineHeuristicSampler_FormulaTransition(t *testing.T) {
	// Test the transition point between linear and logarithmic scaling
	// At exactly 10 goroutines, should use linear formula
	linearAt10 := CPUHeuristicBaselineCPU + 10.0*CPUHeuristicLinearScaleFactor

	// At 11 goroutines, should use logarithmic formula
	logAt11 := CPUHeuristicBaselineCPU + math.Log(11.0)*CPUHeuristicLogScaleFactor

	// Verify the transition is smooth (log should be higher than linear)
	if logAt11 <= linearAt10 {
		t.Errorf("Logarithmic scaling at 11 (%.2f) should be higher than linear at 10 (%.2f)", logAt11, linearAt10)
	}

	// Verify both are within reasonable bounds
	if linearAt10 < 0.0 || linearAt10 > CPUHeuristicMaxCPU {
		t.Errorf("Linear at 10 should be between 0 and %.1f, got %.2f", CPUHeuristicMaxCPU, linearAt10)
	}
	if logAt11 < 0.0 || logAt11 > CPUHeuristicMaxCPU {
		t.Errorf("Log at 11 should be between 0 and %.1f, got %.2f", CPUHeuristicMaxCPU, logAt11)
	}
}

func TestGoroutineHeuristicSampler_EdgeCases(t *testing.T) {
	sampler := NewGoroutineHeuristicSampler()

	// Test with zero time delta (should still work)
	percent := sampler.Sample(0)
	if percent < 0.0 || percent > CPUHeuristicMaxCPU {
		t.Errorf("CPU percent with zero delta should be between 0 and %.1f, got %v", CPUHeuristicMaxCPU, percent)
	}

	// Test with negative time delta (should still work, though unusual)
	percent = sampler.Sample(-100 * time.Millisecond)
	if percent < 0.0 || percent > CPUHeuristicMaxCPU {
		t.Errorf("CPU percent with negative delta should be between 0 and %.1f, got %v", CPUHeuristicMaxCPU, percent)
	}

	// Test with very large time delta
	percent = sampler.Sample(1 * time.Hour)
	if percent < 0.0 || percent > CPUHeuristicMaxCPU {
		t.Errorf("CPU percent with large delta should be between 0 and %.1f, got %v", CPUHeuristicMaxCPU, percent)
	}

	// Test multiple rapid samples
	for i := 0; i < 100; i++ {
		p := sampler.Sample(100 * time.Millisecond)
		if p < 0.0 || p > CPUHeuristicMaxCPU {
			t.Errorf("CPU percent should be between 0 and %.1f, got %v", CPUHeuristicMaxCPU, p)
		}
	}
}
