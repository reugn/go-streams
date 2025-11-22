package flow

import (
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
