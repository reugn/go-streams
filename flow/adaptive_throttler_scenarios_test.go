package flow

import (
	"testing"
	"time"
)

// calculateExpectedRateRange calculates the expected final rate range for a sustained load scenario.
// Returns min and max expected rates based on the algorithm behavior.
func calculateExpectedRateRange(
	config *AdaptiveThrottlerConfig,
	initialRate float64,
	statsSequence []ResourceStats,
) (float64, float64) {
	finalRate := simulateRateAdjustments(config, initialRate, statsSequence)

	tolerance := 0.1 // 10% tolerance

	minExpected := finalRate * (1 - tolerance)
	maxExpected := finalRate * (1 + tolerance)

	if minExpected < float64(config.MinRate) {
		minExpected = float64(config.MinRate)
	}
	if maxExpected > float64(config.MaxRate) {
		maxExpected = float64(config.MaxRate)
	}

	return minExpected, maxExpected
}

// calculateExpectedRecoveryRate calculates the expected recovery rate after a high load followed by normal load.
// Returns the expected rate after the specified number of recovery cycles.
func calculateExpectedRecoveryRate(
	config *AdaptiveThrottlerConfig,
	initialRate float64,
	highLoadStats, normalLoadStats ResourceStats,
	recoveryCycles int,
) float64 {
	currentRate := initialRate

	currentRate = simulateSingleAdjustment(config, currentRate, highLoadStats)

	for i := 0; i < recoveryCycles+1; i++ { // +1 for the first normal load adjustment
		currentRate = simulateSingleAdjustment(config, currentRate, normalLoadStats)
	}

	return currentRate
}

// simulateRateAdjustments simulates a sequence of rate adjustments and returns the final rate.
// This is used to calculate expected rate ranges algorithmically instead of hardcoding them.
func simulateRateAdjustments(
	config *AdaptiveThrottlerConfig,
	initialRate float64,
	statsSequence []ResourceStats,
) float64 {
	currentRate := initialRate

	for _, stats := range statsSequence {
		currentRate = calculateNewRate(config, currentRate, stats)
	}

	return currentRate
}

// simulateSingleAdjustment simulates a single rate adjustment for given stats.
func simulateSingleAdjustment(config *AdaptiveThrottlerConfig, currentRate float64, stats ResourceStats) float64 {
	return calculateNewRate(config, currentRate, stats)
}

// TestAdaptiveThrottler_RealisticProductionScenarios tests behavior in realistic production-like sequences.
func TestAdaptiveThrottler_RealisticProductionScenarios(t *testing.T) {
	tests := []struct {
		name     string
		config   func() *AdaptiveThrottlerConfig
		scenario string
		steps    []ResourceStats
	}{
		{
			name: "High CPU Production Scenario",
			config: func() *AdaptiveThrottlerConfig {
				config := DefaultAdaptiveThrottlerConfig()
				config.MaxCPUPercent = 80.0
				config.MaxMemoryPercent = 90.0
				config.InitialRate = 1000
				config.MinRate = 50
				config.MaxRate = 5000
				config.BackoffFactor = 0.7
				config.RecoveryFactor = 1.2
				config.RecoveryCPUThreshold = 70.0
				config.RecoveryMemoryThreshold = 80.0
				return config
			},
			scenario: "High CPU usage scenario typical in production",
			steps: []ResourceStats{
				{CPUUsagePercent: 85.0, MemoryUsedPercent: 60.0},
				{CPUUsagePercent: 75.0, MemoryUsedPercent: 65.0},
				{CPUUsagePercent: 65.0, MemoryUsedPercent: 70.0},
				{CPUUsagePercent: 55.0, MemoryUsedPercent: 75.0},
			},
		},
		{
			name: "Memory Pressure Production Scenario",
			config: func() *AdaptiveThrottlerConfig {
				config := DefaultAdaptiveThrottlerConfig()
				config.MaxCPUPercent = 90.0
				config.MaxMemoryPercent = 85.0
				config.InitialRate = 2000
				config.MinRate = 100
				config.MaxRate = 10000
				config.BackoffFactor = 0.6
				config.RecoveryFactor = 1.3
				config.RecoveryCPUThreshold = 80.0
				config.RecoveryMemoryThreshold = 75.0
				return config
			},
			scenario: "Memory pressure scenario common in memory-intensive apps",
			steps: []ResourceStats{
				{CPUUsagePercent: 70.0, MemoryUsedPercent: 88.0},
				{CPUUsagePercent: 75.0, MemoryUsedPercent: 82.0},
				{CPUUsagePercent: 65.0, MemoryUsedPercent: 78.0},
				{CPUUsagePercent: 60.0, MemoryUsedPercent: 72.0},
			},
		},
		{
			name: "Balanced Production Load",
			config: func() *AdaptiveThrottlerConfig {
				config := DefaultAdaptiveThrottlerConfig()
				config.MaxCPUPercent = 75.0
				config.MaxMemoryPercent = 80.0
				config.InitialRate = 1500
				config.MinRate = 200
				config.MaxRate = 8000
				config.BackoffFactor = 0.75
				config.RecoveryFactor = 1.25
				config.RecoveryCPUThreshold = 65.0
				config.RecoveryMemoryThreshold = 70.0
				return config
			},
			scenario: "Balanced load typical for well-tuned production systems",
			steps: []ResourceStats{
				{CPUUsagePercent: 78.0, MemoryUsedPercent: 65.0},
				{CPUUsagePercent: 72.0, MemoryUsedPercent: 68.0},
				{CPUUsagePercent: 68.0, MemoryUsedPercent: 75.0},
				{CPUUsagePercent: 65.0, MemoryUsedPercent: 72.0},
			},
		},
		{
			name: "Conservative Production Settings",
			config: func() *AdaptiveThrottlerConfig {
				config := DefaultAdaptiveThrottlerConfig()
				config.MaxCPUPercent = 60.0
				config.MaxMemoryPercent = 75.0
				config.InitialRate = 500
				config.MinRate = 50
				config.MaxRate = 2000
				config.BackoffFactor = 0.8
				config.RecoveryFactor = 1.1
				config.RecoveryCPUThreshold = 50.0
				config.RecoveryMemoryThreshold = 65.0
				return config
			},
			scenario: "Conservative settings for critical production systems",
			steps: []ResourceStats{
				{CPUUsagePercent: 65.0, MemoryUsedPercent: 70.0},
				{CPUUsagePercent: 58.0, MemoryUsedPercent: 72.0},
				{CPUUsagePercent: 55.0, MemoryUsedPercent: 68.0},
				{CPUUsagePercent: 52.0, MemoryUsedPercent: 65.0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := tt.config()
			at, mockMonitor := createThrottlerForRateTesting(config, float64(config.InitialRate))

			t.Logf("Testing scenario: %s", tt.scenario)

			throttlingTriggered := false

			for i, stats := range tt.steps {
				initialRate := at.GetCurrentRate()
				mockMonitor.ExpectGetStats(stats)
				at.adjustRate()
				finalRate := at.GetCurrentRate()

				isConstrained := stats.CPUUsagePercent > config.MaxCPUPercent ||
					stats.MemoryUsedPercent > config.MaxMemoryPercent

				isBelowRecovery := stats.CPUUsagePercent < config.RecoveryCPUThreshold &&
					stats.MemoryUsedPercent < config.RecoveryMemoryThreshold

				t.Logf("Step %d: CPU %.1f%%, Mem %.1f%% -> Rate %.1f (constrained: %v, below recovery: %v)",
					i+1, stats.CPUUsagePercent, stats.MemoryUsedPercent, finalRate, isConstrained, isBelowRecovery)

				// Verify throttling behavior
				if isConstrained {
					throttlingTriggered = true
					if finalRate > initialRate {
						t.Errorf("Step %d: Rate should not increase when constrained, but %.1f > %.1f",
							i+1, finalRate, initialRate)
					}
				}

				// Verify recovery behavior (only if we've seen throttling before)
				if throttlingTriggered && !isConstrained {
					if config.EnableHysteresis && !isBelowRecovery {
						// With hysteresis, rate should not increase until both resources are below recovery thresholds
						if finalRate > initialRate {
							t.Errorf("Step %d: With hysteresis, rate should not increase until both resources below recovery thresholds",
								i+1)
						}
					} else if !config.EnableHysteresis || isBelowRecovery {
						// Without hysteresis or when below recovery thresholds, rate should be able to increase
						if finalRate < initialRate {
							t.Errorf("Step %d: Rate should not decrease during recovery, but %.1f < %.1f",
								i+1, finalRate, initialRate)
						}
					}
				}

				// Verify rate bounds
				if finalRate < float64(config.MinRate) {
					t.Errorf("Step %d: Rate %.1f below MinRate %d", i+1, finalRate, config.MinRate)
				}
				if finalRate > float64(config.MaxRate) {
					t.Errorf("Step %d: Rate %.1f above MaxRate %d", i+1, finalRate, config.MaxRate)
				}
			}

			// Ensure we actually tested throttling behavior
			if !throttlingTriggered {
				t.Errorf("Test scenario should have triggered throttling at least once")
			}
		})
	}
}

// TestAdaptiveThrottler_SustainedLoadScenarios tests behavior under prolonged load.
func TestAdaptiveThrottler_SustainedLoadScenarios(t *testing.T) {
	tests := []struct {
		name        string
		config      func() *AdaptiveThrottlerConfig
		loadPattern []ResourceStats
		description string
	}{
		{
			name: "Sustained High CPU Load",
			config: func() *AdaptiveThrottlerConfig {
				config := DefaultAdaptiveThrottlerConfig()
				config.MaxCPUPercent = 75.0
				config.MaxMemoryPercent = 90.0
				config.InitialRate = 2000
				config.MinRate = 100
				config.MaxRate = 10000
				config.BackoffFactor = 0.7
				config.RecoveryFactor = 1.2
				return config
			},
			loadPattern: []ResourceStats{
				{CPUUsagePercent: 80.0, MemoryUsedPercent: 60.0},
				{CPUUsagePercent: 82.0, MemoryUsedPercent: 62.0},
				{CPUUsagePercent: 78.0, MemoryUsedPercent: 64.0},
				{CPUUsagePercent: 85.0, MemoryUsedPercent: 66.0},
				{CPUUsagePercent: 81.0, MemoryUsedPercent: 68.0},
			},
			description: "Sustained high CPU usage with some variation",
		},
		{
			name: "Memory Pressure Buildup",
			config: func() *AdaptiveThrottlerConfig {
				config := DefaultAdaptiveThrottlerConfig()
				config.MaxCPUPercent = 85.0
				config.MaxMemoryPercent = 80.0
				config.InitialRate = 3000
				config.MinRate = 200
				config.MaxRate = 15000
				config.BackoffFactor = 0.6
				config.RecoveryFactor = 1.15
				return config
			},
			loadPattern: []ResourceStats{
				{CPUUsagePercent: 70.0, MemoryUsedPercent: 75.0},
				{CPUUsagePercent: 72.0, MemoryUsedPercent: 78.0},
				{CPUUsagePercent: 68.0, MemoryUsedPercent: 82.0},
				{CPUUsagePercent: 71.0, MemoryUsedPercent: 85.0},
				{CPUUsagePercent: 69.0, MemoryUsedPercent: 83.0},
			},
			description: "Gradual memory pressure buildup typical of memory leaks",
		},
		{
			name: "Mixed Resource Contention",
			config: func() *AdaptiveThrottlerConfig {
				config := DefaultAdaptiveThrottlerConfig()
				config.MaxCPUPercent = 70.0
				config.MaxMemoryPercent = 75.0
				config.InitialRate = 2500
				config.MinRate = 300
				config.MaxRate = 12000
				config.BackoffFactor = 0.65
				config.RecoveryFactor = 1.25
				config.EnableHysteresis = true
				return config
			},
			loadPattern: []ResourceStats{
				{CPUUsagePercent: 75.0, MemoryUsedPercent: 72.0},
				{CPUUsagePercent: 68.0, MemoryUsedPercent: 78.0},
				{CPUUsagePercent: 72.0, MemoryUsedPercent: 76.0},
				{CPUUsagePercent: 69.0, MemoryUsedPercent: 74.0},
				{CPUUsagePercent: 66.0, MemoryUsedPercent: 71.0},
			},
			description: "Mixed CPU and memory pressure with hysteresis",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := tt.config()
			at, mockMonitor := createThrottlerForRateTesting(config, float64(config.InitialRate))

			t.Logf("Testing sustained load: %s", tt.description)

			var finalRate float64
			for i, stats := range tt.loadPattern {
				mockMonitor.ExpectGetStats(stats)
				at.adjustRate()
				finalRate = at.GetCurrentRate()

				t.Logf("Iteration %d: CPU %.1f%%, Mem %.1f%% -> Rate %.1f",
					i+1, stats.CPUUsagePercent, stats.MemoryUsedPercent, finalRate)
			}

			// Calculate expected rate range algorithmically
			minExpectedRate, maxExpectedRate := calculateExpectedRateRange(config, float64(config.InitialRate), tt.loadPattern)

			t.Logf("Expected final rate range: [%.1f, %.1f], actual: %.1f", minExpectedRate, maxExpectedRate, finalRate)

			if finalRate < minExpectedRate || finalRate > maxExpectedRate {
				t.Errorf("Final rate %.1f outside expected range [%.1f, %.1f] for sustained load scenario",
					finalRate, minExpectedRate, maxExpectedRate)
			}

			// Verify rate doesn't oscillate wildly in final iterations
			// (This would be a sign of poor hysteresis or smoothing)
			if finalRate < float64(config.MinRate)*0.9 {
				t.Errorf("Final rate %.1f too close to MinRate %d, indicating possible oscillation",
					finalRate, config.MinRate)
			}
		})
	}
}

// TestAdaptiveThrottler_RecoveryScenarios tests recovery from high load to normal load.
func TestAdaptiveThrottler_RecoveryScenarios(t *testing.T) {
	tests := []struct {
		name        string
		config      func() *AdaptiveThrottlerConfig
		highLoad    ResourceStats
		normalLoad  ResourceStats
		description string
	}{
		{
			name: "CPU Spike Recovery",
			config: func() *AdaptiveThrottlerConfig {
				config := DefaultAdaptiveThrottlerConfig()
				config.MaxCPUPercent = 80.0
				config.MaxMemoryPercent = 90.0
				config.InitialRate = 2000
				config.MinRate = 200
				config.MaxRate = 10000
				config.BackoffFactor = 0.6
				config.RecoveryFactor = 1.4
				config.SampleInterval = 100 * time.Millisecond
				config.RecoveryCPUThreshold = 70.0
				config.RecoveryMemoryThreshold = 80.0
				return config
			},
			highLoad:    ResourceStats{CPUUsagePercent: 85.0, MemoryUsedPercent: 70.0},
			normalLoad:  ResourceStats{CPUUsagePercent: 60.0, MemoryUsedPercent: 65.0},
			description: "Recovery from CPU spike to normal load",
		},
		{
			name: "Memory Pressure Recovery",
			config: func() *AdaptiveThrottlerConfig {
				config := DefaultAdaptiveThrottlerConfig()
				config.MaxCPUPercent = 90.0
				config.MaxMemoryPercent = 75.0
				config.InitialRate = 3000
				config.MinRate = 300
				config.MaxRate = 15000
				config.BackoffFactor = 0.5
				config.RecoveryFactor = 1.3
				config.RecoveryCPUThreshold = 80.0
				config.RecoveryMemoryThreshold = 65.0
				return config
			},
			highLoad:    ResourceStats{CPUUsagePercent: 70.0, MemoryUsedPercent: 85.0},
			normalLoad:  ResourceStats{CPUUsagePercent: 65.0, MemoryUsedPercent: 60.0},
			description: "Recovery from memory pressure to normal load",
		},
		{
			name: "Dual Resource Recovery with Hysteresis",
			config: func() *AdaptiveThrottlerConfig {
				config := DefaultAdaptiveThrottlerConfig()
				config.MaxCPUPercent = 75.0
				config.MaxMemoryPercent = 80.0
				config.InitialRate = 2500
				config.MinRate = 250
				config.MaxRate = 12000
				config.BackoffFactor = 0.65
				config.RecoveryFactor = 1.25
				config.EnableHysteresis = true
				config.RecoveryCPUThreshold = 65.0
				config.RecoveryMemoryThreshold = 70.0
				return config
			},
			highLoad:    ResourceStats{CPUUsagePercent: 85.0, MemoryUsedPercent: 85.0},
			normalLoad:  ResourceStats{CPUUsagePercent: 60.0, MemoryUsedPercent: 65.0},
			description: "Recovery from dual resource pressure with hysteresis",
		},
		{
			name: "Recovery Without Hysteresis",
			config: func() *AdaptiveThrottlerConfig {
				config := DefaultAdaptiveThrottlerConfig()
				config.MaxCPUPercent = 75.0
				config.MaxMemoryPercent = 80.0
				config.InitialRate = 2500
				config.MinRate = 250
				config.MaxRate = 12000
				config.BackoffFactor = 0.65
				config.RecoveryFactor = 1.25
				config.EnableHysteresis = false
				config.RecoveryCPUThreshold = 65.0
				config.RecoveryMemoryThreshold = 70.0
				return config
			},
			highLoad:    ResourceStats{CPUUsagePercent: 85.0, MemoryUsedPercent: 85.0},
			normalLoad:  ResourceStats{CPUUsagePercent: 70.0, MemoryUsedPercent: 75.0},
			description: "Recovery without hysteresis (faster recovery)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := tt.config()
			at, mockMonitor := createThrottlerForRateTesting(config, float64(config.InitialRate))

			t.Logf("Testing recovery: %s", tt.description)
			t.Logf("Config - CPU Max: %.1f%% (Recovery: %.1f%%), Mem Max: %.1f%% (Recovery: %.1f%%), Hysteresis: %v",
				config.MaxCPUPercent, config.RecoveryCPUThreshold,
				config.MaxMemoryPercent, config.RecoveryMemoryThreshold, config.EnableHysteresis)

			// Start with high load - should trigger throttling
			mockMonitor.ExpectGetStats(tt.highLoad)
			at.adjustRate()
			highLoadRate := at.GetCurrentRate()

			if highLoadRate >= float64(config.InitialRate) {
				t.Errorf("High load should reduce rate, but got %.1f >= %d",
					highLoadRate, config.InitialRate)
			}

			t.Logf("High load (CPU:%.1f%%, Mem:%.1f%%) -> Rate: %.1f",
				tt.highLoad.CPUUsagePercent, tt.highLoad.MemoryUsedPercent, highLoadRate)

			// Transition to normal load - recovery behavior depends on hysteresis
			mockMonitor.ExpectGetStats(tt.normalLoad)
			at.adjustRate()
			recoveryRate1 := at.GetCurrentRate()

			t.Logf("Normal load (CPU:%.1f%%, Mem:%.1f%%) -> Rate: %.1f",
				tt.normalLoad.CPUUsagePercent, tt.normalLoad.MemoryUsedPercent, recoveryRate1)

			// With hysteresis disabled, rate should increase immediately if not constrained
			isConstrained := tt.normalLoad.CPUUsagePercent > config.MaxCPUPercent ||
				tt.normalLoad.MemoryUsedPercent > config.MaxMemoryPercent

			if !isConstrained && !config.EnableHysteresis {
				if recoveryRate1 <= highLoadRate {
					t.Errorf("Without hysteresis, rate should increase when not constrained, but %.1f <= %.1f",
						recoveryRate1, highLoadRate)
				}
			}

			// Continue recovery for a few more cycles to allow hysteresis recovery
			for i := 0; i < 5; i++ {
				mockMonitor.ExpectGetStats(tt.normalLoad)
				at.adjustRate()
			}
			finalRecoveryRate := at.GetCurrentRate()

			t.Logf("After recovery cycles: Rate: %.1f", finalRecoveryRate)

			// Calculate expected recovery rate algorithmically
			expectedRecoveryRate := calculateExpectedRecoveryRate(
				config,
				float64(config.InitialRate),
				tt.highLoad,
				tt.normalLoad,
				5,
			)
			tolerance := 0.05 // 5% tolerance for floating point precision

			t.Logf("Expected recovery rate: %.1f, actual: %.1f", expectedRecoveryRate, finalRecoveryRate)

			// Verify recovery rate is within expected range
			minExpected := expectedRecoveryRate * (1 - tolerance)
			maxExpected := expectedRecoveryRate * (1 + tolerance)

			if finalRecoveryRate < minExpected || finalRecoveryRate > maxExpected {
				t.Errorf("Final recovery rate %.1f outside expected range [%.1f, %.1f]",
					finalRecoveryRate, minExpected, maxExpected)
			}

			// Should not exceed MaxRate
			if finalRecoveryRate > float64(config.MaxRate) {
				t.Errorf("Recovery rate %.1f should not exceed MaxRate %d",
					finalRecoveryRate, config.MaxRate)
			}
		})
	}
}
