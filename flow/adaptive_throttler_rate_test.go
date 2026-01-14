package flow

import (
	"math"
	"testing"
	"time"

	"github.com/reugn/go-streams/internal/assert"
)

// createThrottlerForRateTesting creates a throttler with mock monitor for rate adjustment testing.
func createThrottlerForRateTesting(
	config *AdaptiveThrottlerConfig,
	initialRate float64,
) (*AdaptiveThrottler, *MockMonitor) {
	mockMonitor := &MockMonitor{}
	throttler := &AdaptiveThrottler{
		config:          *config,
		monitor:         mockMonitor,
		currentRateBits: math.Float64bits(initialRate),
	}
	return throttler, mockMonitor
}

// TestAdaptiveThrottler_AdjustRate_Logic tests rate adjustment algorithm with high/low resource usage.
func TestAdaptiveThrottler_AdjustRate_Logic(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	config.MinRate = 10
	config.MaxRate = 100
	config.InitialRate = 50
	config.BackoffFactor = 0.5
	config.RecoveryFactor = 2.0
	config.MaxCPUPercent = 50.0
	config.MaxMemoryPercent = 50.0
	config.RecoveryCPUThreshold = 40.0
	config.RecoveryMemoryThreshold = 40.0

	at, mockMonitor := createThrottlerForRateTesting(config, float64(config.InitialRate))

	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   60.0,
		MemoryUsedPercent: 30.0,
	})
	at.adjustRate()
	assert.InDelta(t, 42.5, at.GetCurrentRate(), 0.01)

	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   40.0,
		MemoryUsedPercent: 60.0,
	})
	at.adjustRate()
	assert.InDelta(t, 36.125, at.GetCurrentRate(), 0.01)

	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   10.0,
		MemoryUsedPercent: 10.0,
	})
	at.adjustRate()
	assert.InDelta(t, 46.9625, at.GetCurrentRate(), 0.01)
}

// TestAdaptiveThrottler_Hysteresis tests hysteresis behavior with enabled/disabled modes.
func TestAdaptiveThrottler_Hysteresis(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	config.MinRate = 10
	config.MaxRate = 100
	config.InitialRate = 50
	config.BackoffFactor = 0.8
	config.RecoveryFactor = 1.2
	config.MaxCPUPercent = 80.0
	config.MaxMemoryPercent = 85.0
	config.RecoveryCPUThreshold = 70.0
	config.RecoveryMemoryThreshold = 75.0

	// Test with hysteresis enabled (default)
	config.EnableHysteresis = true
	at, mockMonitor := createThrottlerForRateTesting(config, float64(config.InitialRate))

	// CPU at 75% (above recovery threshold) - should not increase with hysteresis
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   75.0,
		MemoryUsedPercent: 40.0,
	})
	at.adjustRate()
	assert.InDelta(t, 50.0, at.GetCurrentRate(), 0.01)

	// CPU at 65% (below recovery threshold) - should increase
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   65.0,
		MemoryUsedPercent: 40.0,
	})
	at.adjustRate()
	assert.InDelta(t, 53.0, at.GetCurrentRate(), 0.01)

	// Test with hysteresis disabled
	config.EnableHysteresis = false
	at2, mockMonitor2 := createThrottlerForRateTesting(config, 50.0)

	// CPU at 75% (below max threshold) - should increase immediately
	mockMonitor2.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   75.0,
		MemoryUsedPercent: 40.0,
	})
	at2.adjustRate()
	assert.InDelta(t, 53.0, at2.GetCurrentRate(), 0.01) // Should increase immediately
}

// TestAdaptiveThrottler_Limits tests that rate stays within min/max bounds.
func TestAdaptiveThrottler_Limits(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	config.MinRate = 10
	config.MaxRate = 20
	config.InitialRate = 10
	config.RecoveryFactor = 10.0
	config.RecoveryCPUThreshold = 70.0
	config.RecoveryMemoryThreshold = 75.0

	at, mockMonitor := createThrottlerForRateTesting(config, 10.0)

	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   0,
		MemoryUsedPercent: 0,
		Timestamp:         time.Now(),
	})
	at.adjustRate()
	assert.InDelta(t, 13.0, at.GetCurrentRate(), 0.01)
}

// TestAdaptiveThrottler_AdjustRate_EdgeCases tests edge cases for adjustRate.
func TestAdaptiveThrottler_AdjustRate_EdgeCases(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	config.MinRate = 10
	config.MaxRate = 100
	config.InitialRate = 50
	config.BackoffFactor = 0.7
	config.RecoveryFactor = 1.3
	config.MaxCPUPercent = 80.0
	config.MaxMemoryPercent = 85.0
	config.RecoveryCPUThreshold = 70.0
	config.RecoveryMemoryThreshold = 75.0

	at, mockMonitor := createThrottlerForRateTesting(config, 50.0)

	// Both CPU and memory constrained
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   90.0,
		MemoryUsedPercent: 90.0,
	})
	at.adjustRate()
	assert.InDelta(t, 45.5, at.GetCurrentRate(), 0.1)

	// Rate at max, should not exceed
	at.setRate(100.0)
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   10.0,
		MemoryUsedPercent: 10.0,
	})
	at.adjustRate()
	rate := at.GetCurrentRate()
	if rate > 100.0 {
		t.Errorf("Rate should not exceed MaxRate, got %f", rate)
	}

	// Rate at min, constrained
	at.setRate(10.0)
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   90.0,
		MemoryUsedPercent: 90.0,
	})
	at.adjustRate()
	rate = at.GetCurrentRate()
	if rate < 0 {
		t.Errorf("Rate should not be negative, got %f", rate)
	}

	// Exactly at thresholds
	at.setRate(50.0)
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   80.0, // Exactly at max
		MemoryUsedPercent: 70.0,
	})
	at.adjustRate()
	rate = at.GetCurrentRate()
	if rate < 0 {
		t.Errorf("Rate should not be negative, got %f", rate)
	}

	// Exactly at recovery thresholds with hysteresis
	at.setRate(50.0)
	config.EnableHysteresis = true
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   70.0, // Exactly at recovery threshold
		MemoryUsedPercent: 75.0,
	})
	at.adjustRate()
	rate = at.GetCurrentRate()
	if rate > 50.0 {
		t.Errorf("With hysteresis, rate should not increase at threshold, got %f", rate)
	}
}

// TestAdaptiveThrottler_DisableCPUThrottling ensures MaxCPUPercent=0 disables CPU-based constraints.
func TestAdaptiveThrottler_DisableCPUThrottling(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	config.MinRate = 10
	config.MaxRate = 100
	config.InitialRate = 50
	config.BackoffFactor = 0.7
	config.RecoveryFactor = 1.3
	config.MaxCPUPercent = 0
	config.RecoveryCPUThreshold = 0
	config.MaxMemoryPercent = 85.0
	config.RecoveryMemoryThreshold = 65.0

	at, mockMonitor := createThrottlerForRateTesting(config, float64(config.InitialRate))

	// High CPU alone should not constrain when CPU limit disabled; expect increase.
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   95.0,
		MemoryUsedPercent: 40.0,
	})
	at.adjustRate()
	assert.InDelta(t, 54.5, at.GetCurrentRate(), 0.01)

	// Memory pressure should still constrain even if CPU is high.
	rateBefore := at.GetCurrentRate()
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   95.0,
		MemoryUsedPercent: 90.0,
	})
	at.adjustRate()
	rateAfter := at.GetCurrentRate()
	if rateAfter >= rateBefore {
		t.Errorf("Rate should decrease under memory pressure, got before %.2f after %.2f", rateBefore, rateAfter)
	}
}

// TestAdaptiveThrottler_AdjustRate_IgnoresMissingStats ensures missing stats do not cause runaway increases.
func TestAdaptiveThrottler_AdjustRate_IgnoresMissingStats(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	config.InitialRate = 50
	config.MinRate = 10
	config.MaxRate = 100

	at, mockMonitor := createThrottlerForRateTesting(config, float64(config.InitialRate))

	mockMonitor.ExpectGetStats(ResourceStats{}) // Simulate missing/invalid stats
	at.adjustRate()

	if got := at.GetCurrentRate(); got != float64(config.InitialRate) {
		t.Errorf("Rate unchanged when stats missing: want %.2f got %.2f", float64(config.InitialRate), got)
	}
}
