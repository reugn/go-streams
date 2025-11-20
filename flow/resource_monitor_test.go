package flow

import (
	"math"
	"runtime"
	"testing"
	"time"
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

	// Wait for initial stats to be collected (poll instead of fixed sleep)
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for stats collection")
		default:
			stats := rm.GetStats()
			if !stats.Timestamp.IsZero() {
				// Stats have been collected, continue with validation
				goto statsCollected
			}
			time.Sleep(10 * time.Millisecond) // Brief pause before polling again
		}
	}

statsCollected:
	stats := rm.GetStats()
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

func TestGoroutineHeuristicSampler(t *testing.T) {
	sampler := &goroutineHeuristicSampler{}

	// Test with reasonable goroutine count
	_ = runtime.NumGoroutine()

	percent := sampler.Sample(100 * time.Millisecond)
	if percent < 0.0 || percent > 100.0 {
		t.Errorf("CPU percent should be between 0 and 100, got %v", percent)
	}

	// Test reset (should be no-op)
	sampler.Reset()
}

// TestGopsutilProcessSampler is an integration test that verifies
// gopsutil-based CPU sampling (may be skipped if gopsutil unavailable)
func TestGopsutilProcessSampler(t *testing.T) {
	sampler, err := newGopsutilProcessSampler()
	if err != nil {
		t.Fatalf("newGopsutilProcessSampler failed: %v", err)
	}
	if sampler == nil {
		t.Fatal("gopsutilProcessSampler should not be nil")
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

func TestResourceMonitor_CPUUsageModeHeuristic(t *testing.T) {
	rm := NewResourceMonitor(100*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)
	defer rm.Close()

	if rm.cpuMode != CPUUsageModeHeuristic {
		t.Errorf("expected cpuMode %v, got %v", CPUUsageModeHeuristic, rm.cpuMode)
	}
	_, ok := rm.sampler.(*goroutineHeuristicSampler)
	if !ok {
		t.Error("expected goroutineHeuristicSampler")
	}
}

func TestResourceMonitor_CPUUsageModeMeasured(t *testing.T) {
	rm := NewResourceMonitor(100*time.Millisecond, 80.0, 70.0, CPUUsageModeMeasured, nil)
	defer rm.Close()

	// Should use gopsutil sampler or fallback to heuristic
	if _, ok := rm.sampler.(*gopsutilProcessSampler); ok {
		if rm.cpuMode != CPUUsageModeMeasured {
			t.Errorf("expected cpuMode %v, got %v", CPUUsageModeMeasured, rm.cpuMode)
		}
	} else {
		if rm.cpuMode != CPUUsageModeHeuristic {
			t.Errorf("expected fallback cpuMode %v, got %v", CPUUsageModeHeuristic, rm.cpuMode)
		}
		_, ok := rm.sampler.(*goroutineHeuristicSampler)
		if !ok {
			t.Error("expected fallback to goroutineHeuristicSampler")
		}
	}
}

func TestResourceMonitor_Close(t *testing.T) {
	rm := NewResourceMonitor(100*time.Millisecond, 80.0, 70.0, CPUUsageModeHeuristic, nil)

	// Close should be idempotent
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

	// Wait for initial stats collection
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for initial stats collection")
		default:
			if !rm.GetStats().Timestamp.IsZero() {
				goto initialStatsCollected
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

initialStatsCollected:
	// Wait for at least one more update
	stats1 := rm.GetStats()
	timeout = time.After(500 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for stats update")
		default:
			stats2 := rm.GetStats()
			if stats2.Timestamp.After(stats1.Timestamp) {
				// Stats have been updated
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestResourceMonitor_UsesSystemMemoryStats(t *testing.T) {
	t.Helper()

	restore := setSystemMemoryReader(func() (systemMemory, error) {
		return systemMemory{
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

	// Wait for initial stats
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for stats")
		default:
			if !rm.GetStats().Timestamp.IsZero() {
				goto statsReady
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

statsReady:
	// Force garbage collection to get more stable memory stats
	runtime.GC()
	time.Sleep(20 * time.Millisecond) // Reduced sleep time

	stats := rm.GetStats()

	// Memory percentage should be reasonable
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

// fakeSampler tracks Sample calls to test regression for double sampling bug
type fakeSampler struct {
	sampleCount   int
	lastDelta     time.Duration
	isInitialized bool
}

func (f *fakeSampler) Sample(deltaTime time.Duration) float64 {
	f.sampleCount++
	f.lastDelta = deltaTime
	return 50.0 // dummy value
}

func (f *fakeSampler) Reset() {
	f.sampleCount = 0
	f.lastDelta = 0
	f.isInitialized = false
}

func (f *fakeSampler) IsInitialized() bool {
	return f.isInitialized
}

// TestResourceMonitor_SingleSamplePerTick verifies the fix for the double sampling bug
// where collectStats was calling Sample twice per tick, overwriting the first measurement
func TestResourceMonitor_SingleSamplePerTick(t *testing.T) {
	fakeSampler := &fakeSampler{isInitialized: true}

	// Create a monitor with our fake sampler
	rm := &ResourceMonitor{
		sampleInterval:  100 * time.Millisecond,
		memoryThreshold: 80.0,
		cpuThreshold:    70.0,
		cpuMode:         CPUUsageModeHeuristic,
		sampler:         fakeSampler,
		done:            make(chan struct{}),
	}

	// Call collectStats once
	rm.collectStats()

	// Verify Sample was called exactly once
	if fakeSampler.sampleCount != 1 {
		t.Errorf("expected Sample to be called once per collectStats, got %d calls", fakeSampler.sampleCount)
	}

	// Verify the delta passed is the configured interval
	if fakeSampler.lastDelta != rm.sampleInterval {
		t.Errorf("expected delta %v, got %v", rm.sampleInterval, fakeSampler.lastDelta)
	}
}

// TestGopsutilProcessSampler_Initialization verifies that gopsutilProcessSampler
// properly tracks initialization state
func TestGopsutilProcessSampler_Initialization(t *testing.T) {
	sampler, err := newGopsutilProcessSampler()
	if err != nil {
		t.Fatalf("newGopsutilProcessSampler failed: %v", err)
	}
	if sampler == nil {
		t.Fatal("gopsutilProcessSampler should not be nil")
	}

	// Initially not initialized
	if sampler.IsInitialized() {
		t.Error("gopsutilProcessSampler should not be initialized initially")
	}

	// After first sample, should be initialized
	sampler.Sample(100 * time.Millisecond)
	if !sampler.IsInitialized() {
		t.Error("gopsutilProcessSampler should be initialized after first sample")
	}

	// After reset, should not be initialized
	sampler.Reset()
	if sampler.IsInitialized() {
		t.Error("gopsutilProcessSampler should not be initialized after reset")
	}
}
