package flow

import (
	"fmt"
	"math"
	"runtime"
	"testing"
	"time"

	"github.com/reugn/go-streams/internal/sysmonitor"
)

func TestNewResourceMonitor_InvalidSampleInterval(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for invalid sample interval")
		}
	}()

	NewResourceMonitor(0, 80.0, 70.0, CPUUsageModeHeuristic, nil)
}

func TestNewResourceMonitor_InvalidMemoryThreshold(t *testing.T) {
	t.Run("negative memory threshold", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic for negative memory threshold")
			}
		}()

		NewResourceMonitor(100*time.Millisecond, -1.0, 70.0, CPUUsageModeHeuristic, nil)
	})

	t.Run("memory threshold > 100", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic for memory threshold > 100")
			}
		}()

		NewResourceMonitor(100*time.Millisecond, 150.0, 70.0, CPUUsageModeHeuristic, nil)
	})
}

func TestNewResourceMonitor_InvalidCPUThreshold(t *testing.T) {
	t.Run("negative CPU threshold", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic for negative CPU threshold")
			}
		}()

		NewResourceMonitor(100*time.Millisecond, 80.0, -1.0, CPUUsageModeHeuristic, nil)
	})

	t.Run("CPU threshold > 100", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic for CPU threshold > 100")
			}
		}()

		NewResourceMonitor(100*time.Millisecond, 80.0, 150.0, CPUUsageModeHeuristic, nil)
	})
}

func TestResourceMonitor_GetStats(t *testing.T) {
	rm := NewResourceMonitor(50*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
	defer rm.Close()

	timeout := time.After(500 * time.Millisecond)
	var stats ResourceStats
	statsCollected := false
	for !statsCollected {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for stats collection")
		default:
			stats = rm.GetStats()
			if !stats.Timestamp.IsZero() {
				statsCollected = true
				break
			}
			time.Sleep(10 * time.Millisecond) // Brief pause before polling again
		}
	}

	if stats.MemoryUsedPercent < 0.0 || stats.MemoryUsedPercent > 100.0 {
		t.Errorf("memory percent should be between 0 and 100, got %v", stats.MemoryUsedPercent)
	}
	if stats.CPUUsagePercent < 0.0 || stats.CPUUsagePercent > 100.0 {
		t.Errorf("CPU percent should be between 0 and 100, got %v", stats.CPUUsagePercent)
	}
	if stats.GoroutineCount <= 0 {
		t.Errorf("goroutine count should be > 0, got %d", stats.GoroutineCount)
	}
}

func TestResourceMonitor_IsResourceConstrained(t *testing.T) {
	tests := []struct {
		name                string
		memoryThreshold     float64
		cpuThreshold        float64
		memoryPercent       float64
		cpuPercent          float64
		expectedConstrained bool
	}{
		{
			name:                "not constrained",
			memoryThreshold:     80.0,
			cpuThreshold:        70.0,
			memoryPercent:       50.0,
			cpuPercent:          40.0,
			expectedConstrained: false,
		},
		{
			name:                "memory constrained",
			memoryThreshold:     80.0,
			cpuThreshold:        70.0,
			memoryPercent:       85.0,
			cpuPercent:          40.0,
			expectedConstrained: true,
		},
		{
			name:                "CPU constrained",
			memoryThreshold:     80.0,
			cpuThreshold:        70.0,
			memoryPercent:       50.0,
			cpuPercent:          75.0,
			expectedConstrained: true,
		},
		{
			name:                "both constrained",
			memoryThreshold:     80.0,
			cpuThreshold:        70.0,
			memoryPercent:       85.0,
			cpuPercent:          75.0,
			expectedConstrained: true,
		},
		{
			name:                "at threshold not constrained",
			memoryThreshold:     80.0,
			cpuThreshold:        70.0,
			memoryPercent:       80.0,
			cpuPercent:          70.0,
			expectedConstrained: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rm := NewResourceMonitor(100*time.Millisecond, tt.memoryThreshold, tt.cpuThreshold, CPUUsageModeHeuristic, nil)
			defer rm.Close()

			// Manually set stats for testing
			testStats := &ResourceStats{
				MemoryUsedPercent: tt.memoryPercent,
				CPUUsagePercent:   tt.cpuPercent,
				GoroutineCount:    10,
				Timestamp:         time.Now(),
			}
			rm.stats.Store(testStats)

			if rm.IsResourceConstrained() != tt.expectedConstrained {
				t.Errorf("expected constrained %v, got %v", tt.expectedConstrained, rm.IsResourceConstrained())
			}
		})
	}
}

func TestResourceMonitor_CPUUsageModeHeuristic(t *testing.T) {
	rm := NewResourceMonitor(100*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
	defer rm.Close()

	// Verify mode is set correctly
	if rm.cpuMode != CPUUsageModeHeuristic {
		t.Errorf("expected cpuMode %v, got %v", CPUUsageModeHeuristic, rm.cpuMode)
	}

	// Verify sampler is initialized and functional
	if rm.sampler == nil {
		t.Fatal("sampler should not be nil")
	}

	// Test that sampling produces reasonable values
	percent := rm.sampler.Sample(100 * time.Millisecond)
	if percent < 0.0 {
		t.Errorf("CPU percent should not be negative, got %v", percent)
	}
}

func TestResourceMonitor_CPUUsageModeMeasured(t *testing.T) {
	rm := NewResourceMonitor(100*time.Millisecond, 80.0, 70.0, CPUUsageModeMeasured, nil)
	defer rm.Close()

	// Should use measured mode
	if rm.cpuMode != CPUUsageModeMeasured && rm.cpuMode != CPUUsageModeHeuristic {
		t.Errorf("expected cpuMode Measured or Heuristic (fallback), got %v", rm.cpuMode)
	}

	// Verify sampler is initialized and functional
	if rm.sampler == nil {
		t.Fatal("sampler should not be nil")
	}

	// Test that sampling produces reasonable values (normalized to 0-100%)
	percent := rm.sampler.Sample(100 * time.Millisecond)
	if percent < 0.0 || percent > 100.0 {
		t.Errorf("CPU percent should be between 0 and 100, got %v", percent)
	}
}

func TestResourceMonitor_Close(t *testing.T) {
	rm := NewResourceMonitor(100*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)

	rm.Close()
	rm.Close()

	// Should be able to close again without panic
	select {
	case <-rm.done:
		// Expected - channel should be closed
	default:
		t.Error("done channel should be closed after Close()")
	}
}

// TestResourceMonitor_MonitorLoop is an integration test that verifies
// the monitoring loop updates statistics over time
func TestResourceMonitor_MonitorLoop(t *testing.T) {
	rm := NewResourceMonitor(50*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
	defer rm.Close()

	initialStats := waitForStats(t, rm, 500*time.Millisecond, func(stats ResourceStats) bool {
		return !stats.Timestamp.IsZero()
	})

	waitForStats(t, rm, 500*time.Millisecond, func(stats ResourceStats) bool {
		return stats.Timestamp.After(initialStats.Timestamp)
	})
}

// waitForStats polls GetStats until the condition function returns true or timeout is reached
func waitForStats(
	t *testing.T,
	rm *ResourceMonitor,
	timeout time.Duration,
	condition func(ResourceStats) bool,
) ResourceStats {
	t.Helper()

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		stats := rm.GetStats()
		if condition(stats) {
			return stats
		}

		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for stats condition (timeout: %v)", timeout)
		}

		<-ticker.C
	}
}

func TestResourceMonitor_UsesSystemMemoryStats(t *testing.T) {
	restore := sysmonitor.SetMemoryReader(func() (sysmonitor.SystemMemory, error) {
		return sysmonitor.SystemMemory{
			Total:     100 * 1024 * 1024,
			Available: 25 * 1024 * 1024,
		}, nil
	})
	defer restore()

	rm := NewResourceMonitor(50*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
	defer rm.Close()

	stats := rm.collectStats()
	if diff := math.Abs(stats.MemoryUsedPercent - 75.0); diff > 0.01 {
		t.Fatalf("expected memory percent ~75, diff %v", diff)
	}
}

// TestResourceStats_MemoryCalculation is an integration test that verifies
// memory statistics calculation with real system memory
func TestResourceStats_MemoryCalculation(t *testing.T) {
	rm := NewResourceMonitor(50*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
	defer rm.Close()

	timeout := time.After(500 * time.Millisecond)
	statsReady := false
	for !statsReady {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for stats")
		default:
			if !rm.GetStats().Timestamp.IsZero() {
				statsReady = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	runtime.GC()
	time.Sleep(20 * time.Millisecond)

	stats := rm.GetStats()

	if stats.MemoryUsedPercent < 0.0 || stats.MemoryUsedPercent > 100.0 {
		t.Errorf("memory percent should be between 0 and 100, got %v", stats.MemoryUsedPercent)
	}

	// Goroutine count should be at least 2 (main + monitor goroutine)
	if stats.GoroutineCount < 2 {
		t.Errorf("goroutine count should be at least 2, got %d", stats.GoroutineCount)
	}
}

func TestResourceMonitor_GetStatsNilStats(t *testing.T) {
	rm := NewResourceMonitor(50*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
	defer rm.Close()

	// Manually set stats to nil
	rm.stats.Store(nil)

	stats := rm.GetStats()
	if stats.Timestamp.IsZero() {
		// Should return empty ResourceStats when stats is nil
		if stats.MemoryUsedPercent != 0 || stats.CPUUsagePercent != 0 || stats.GoroutineCount != 0 {
			t.Errorf("expected empty stats when nil, got %+v", stats)
		}
	}
}

func TestResourceMonitor_MemoryUsagePercentEdgeCases(t *testing.T) {
	t.Run("available > total", func(t *testing.T) {
		restore := sysmonitor.SetMemoryReader(func() (sysmonitor.SystemMemory, error) {
			return sysmonitor.SystemMemory{
				Total:     100 * 1024 * 1024,
				Available: 150 * 1024 * 1024, // Available > Total (invalid but should handle)
			}, nil
		})
		defer restore()

		rm := NewResourceMonitor(50*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
		defer rm.Close()

		stats := rm.collectStats()
		// Should clamp available to total, so used = 0, percent = 0
		if stats.MemoryUsedPercent < 0 || stats.MemoryUsedPercent > 100 {
			t.Errorf("memory percent should be valid, got %f", stats.MemoryUsedPercent)
		}
	})

	t.Run("total == 0", func(t *testing.T) {
		restore := sysmonitor.SetMemoryReader(func() (sysmonitor.SystemMemory, error) {
			return sysmonitor.SystemMemory{
				Total:     0,
				Available: 0,
			}, nil
		})
		defer restore()

		rm := NewResourceMonitor(50*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
		defer rm.Close()

		stats := rm.collectStats()
		// When total is 0, tryGetSystemMemory() returns hasSystemStats=false
		// This causes collectStats() to fall back to procStats (runtime.MemStats)
		if stats.MemoryUsedPercent < 0 || stats.MemoryUsedPercent > 100 {
			t.Errorf("memory percent should be valid when total is 0 (falls back to procStats), got %f", stats.MemoryUsedPercent)
		}
	})

	t.Run("procStats fallback", func(t *testing.T) {
		// Set memory reader to return error to trigger procStats fallback
		restore := sysmonitor.SetMemoryReader(func() (sysmonitor.SystemMemory, error) {
			return sysmonitor.SystemMemory{}, fmt.Errorf("memory read failed")
		})
		defer restore()

		rm := NewResourceMonitor(50*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
		defer rm.Close()

		stats := rm.collectStats()
		// Should use procStats fallback
		if stats.MemoryUsedPercent < 0 || stats.MemoryUsedPercent > 100 {
			t.Errorf("memory percent should be valid with procStats fallback, got %f", stats.MemoryUsedPercent)
		}
	})

	t.Run("procStats nil or Sys == 0", func(t *testing.T) {
		// This is hard to test directly as it requires system memory to fail
		// and procStats to be nil or have Sys == 0, which is unlikely in practice
		// but the code path exists for safety
		restore := sysmonitor.SetMemoryReader(func() (sysmonitor.SystemMemory, error) {
			return sysmonitor.SystemMemory{}, fmt.Errorf("memory read failed")
		})
		defer restore()

		rm := NewResourceMonitor(50*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
		defer rm.Close()

		// Force hasSystemStats to false by making GetSystemMemory fail
		stats := rm.collectStats()
		// Should handle gracefully
		if stats.MemoryUsedPercent < 0 || stats.MemoryUsedPercent > 100 {
			t.Errorf("memory percent should be valid, got %f", stats.MemoryUsedPercent)
		}
	})
}

func TestResourceMonitor_CustomMemoryReaderError(t *testing.T) {
	errorCount := 0
	rm := NewResourceMonitor(50*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, func() (float64, error) {
		errorCount++
		return 0, fmt.Errorf("custom reader error") // Return error to trigger fallback
	})
	defer rm.Close()

	stats := rm.collectStats()
	// Should fall back to system memory when custom reader fails
	if stats.MemoryUsedPercent < 0 || stats.MemoryUsedPercent > 100 {
		t.Errorf("memory percent should be valid after fallback, got %f", stats.MemoryUsedPercent)
	}
	if errorCount == 0 {
		t.Error("custom memory reader should have been called")
	}
}

func TestResourceMonitor_ValidateResourceStats(t *testing.T) {
	rm := NewResourceMonitor(50*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
	defer rm.Close()

	t.Run("negative goroutine count", func(t *testing.T) {
		stats := &ResourceStats{
			MemoryUsedPercent: 50.0,
			CPUUsagePercent:   40.0,
			GoroutineCount:    -1, // Invalid
			Timestamp:         time.Now(),
		}
		// Store invalid stats and retrieve them - validation happens in collectStats
		// We test indirectly by ensuring GetStats returns valid values
		rm.stats.Store(stats)
		// Wait for next collection which will validate
		time.Sleep(60 * time.Millisecond)
		retrievedStats := rm.GetStats()
		// Validation should clamp negative goroutine count to 0
		if retrievedStats.GoroutineCount < 0 {
			t.Errorf("goroutine count should not be negative, got %d", retrievedStats.GoroutineCount)
		}
	})

	t.Run("zero timestamp", func(t *testing.T) {
		stats := &ResourceStats{
			MemoryUsedPercent: 50.0,
			CPUUsagePercent:   40.0,
			GoroutineCount:    10,
			Timestamp:         time.Time{}, // Zero timestamp
		}
		rm.stats.Store(stats)
		// Validation should set timestamp if zero
		// Note: GetStats doesn't validate, but collectStats does
		// We test by triggering a new collection
		time.Sleep(60 * time.Millisecond) // Wait for next collection
		newStats := rm.GetStats()
		if newStats.Timestamp.IsZero() {
			t.Error("timestamp should not be zero after collection")
		}
	})

	t.Run("old timestamp refresh", func(t *testing.T) {
		oldTime := time.Now().Add(-2 * time.Minute) // More than 1 minute ago
		stats := &ResourceStats{
			MemoryUsedPercent: 50.0,
			CPUUsagePercent:   40.0,
			GoroutineCount:    10,
			Timestamp:         oldTime,
		}
		rm.stats.Store(stats)
		// Wait for next collection which will validate and refresh timestamp
		time.Sleep(60 * time.Millisecond)
		newStats := rm.GetStats()
		if newStats.Timestamp.Equal(oldTime) {
			t.Error("timestamp should be refreshed when older than 1 minute")
		}
		if time.Since(newStats.Timestamp) > time.Second {
			t.Error("timestamp should be recent after refresh")
		}
	})
}

func BenchmarkResourceMonitor_GetStats(b *testing.B) {
	rm := NewResourceMonitor(100*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
	defer rm.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rm.GetStats()
	}
}

func BenchmarkResourceMonitor_IsResourceConstrained(b *testing.B) {
	rm := NewResourceMonitor(100*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
	defer rm.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rm.IsResourceConstrained()
	}
}
