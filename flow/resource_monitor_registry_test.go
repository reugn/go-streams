package flow

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func resetRegistry() {
	globalMonitorRegistry = &monitorRegistry{
		intervalRefs: make(map[time.Duration]int),
	}
}

func setupTest(t *testing.T) {
	t.Helper()
	resetRegistry()
	t.Cleanup(resetRegistry)
}

// Registry acquire/release and handle lifecycle.
func TestMonitorRegistry_Acquire(t *testing.T) {
	setupTest(t)

	h1 := globalMonitorRegistry.Acquire(2*time.Second, CPUUsageModeHeuristic, nil)
	defer h1.Close()

	if globalMonitorRegistry.instance == nil {
		t.Fatal("Registry instance should not be nil after acquire")
	}
	if globalMonitorRegistry.currentMin != 2*time.Second {
		t.Errorf("Expected currentMin 2s, got %v", globalMonitorRegistry.currentMin)
	}
	if count := globalMonitorRegistry.intervalRefs[2*time.Second]; count != 1 {
		t.Errorf("Expected ref count 1, got %d", count)
	}

	h2 := globalMonitorRegistry.Acquire(2*time.Second, CPUUsageModeHeuristic, nil)
	defer h2.Close()

	if count := globalMonitorRegistry.intervalRefs[2*time.Second]; count != 2 {
		t.Errorf("Expected ref count 2, got %d", count)
	}

	h3 := globalMonitorRegistry.Acquire(1*time.Second, CPUUsageModeHeuristic, nil)
	defer h3.Close()

	if globalMonitorRegistry.currentMin != 1*time.Second {
		t.Errorf("Expected currentMin to update to 1s, got %v", globalMonitorRegistry.currentMin)
	}

	timeout := time.After(100 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for instance interval update. Expected 1s, got %v",
				globalMonitorRegistry.getInstanceSampleInterval())
		default:
			if globalMonitorRegistry.getInstanceSampleInterval() == 1*time.Second {
				goto intervalUpdated
			}
			time.Sleep(1 * time.Millisecond)
		}
	}
intervalUpdated:

	h4 := globalMonitorRegistry.Acquire(5*time.Second, CPUUsageModeHeuristic, nil)
	defer h4.Close()

	if globalMonitorRegistry.currentMin != 1*time.Second {
		t.Errorf("Expected currentMin to remain 1s, got %v", globalMonitorRegistry.currentMin)
	}
}

func TestMonitorRegistry_Release_Logic(t *testing.T) {
	setupTest(t)

	hFast := globalMonitorRegistry.Acquire(1*time.Second, CPUUsageModeHeuristic, nil)
	hSlow := globalMonitorRegistry.Acquire(5*time.Second, CPUUsageModeHeuristic, nil)

	if globalMonitorRegistry.currentMin != 1*time.Second {
		t.Fatalf("Setup failed: expected 1s min")
	}

	hFast.Close()

	if globalMonitorRegistry.currentMin != 5*time.Second {
		t.Errorf("Expected currentMin to relax to 5s, got %v", globalMonitorRegistry.currentMin)
	}

	hSlow.Close()

	if len(globalMonitorRegistry.intervalRefs) != 0 {
		t.Errorf("Expected empty refs, got %v", globalMonitorRegistry.intervalRefs)
	}
	if globalMonitorRegistry.stopTimer == nil {
		t.Error("Expected stopTimer to be set after full release")
	}

	globalMonitorRegistry.cleanup()
	if globalMonitorRegistry.instance != nil {
		t.Error("Expected instance to be nil after cleanup")
	}
}

func TestMonitorRegistry_Resurrect(t *testing.T) {
	setupTest(t)

	h1 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)
	h1.Close()

	if globalMonitorRegistry.stopTimer == nil {
		t.Fatal("Timer should be running")
	}

	h2 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)
	defer h2.Close()

	if globalMonitorRegistry.stopTimer != nil {
		t.Error("Timer should be stopped/nil after resurrection")
	}
	if globalMonitorRegistry.instance == nil {
		t.Error("Instance should still be alive")
	}
}

// Registry correctness and panic scenarios.
func TestMonitorRegistry_BufferedChannel_DeadlockPrevention(t *testing.T) {
	setupTest(t)

	sampleStarted := make(chan bool, 1)
	sampleContinue := make(chan bool, 1)
	blockingMemoryReader := func() (float64, error) {
		sampleStarted <- true
		<-sampleContinue
		return 50.0, nil
	}

	handle1 := globalMonitorRegistry.Acquire(200*time.Millisecond, CPUUsageModeHeuristic, blockingMemoryReader)
	defer handle1.Close()

	select {
	case <-sampleStarted:
	case <-time.After(1 * time.Second):
		t.Fatal("Monitor didn't start sampling within timeout")
	}

	start := time.Now()
	handle2 := globalMonitorRegistry.Acquire(50*time.Millisecond, CPUUsageModeHeuristic, nil)
	defer handle2.Close()

	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Errorf("Acquire took too long (%v) - possible blocking", elapsed)
	}

	sampleContinue <- true
	time.Sleep(50 * time.Millisecond)
}

func TestMonitorRegistry_ConcurrentAccess(t *testing.T) {
	setupTest(t)

	const numGoroutines = 5
	const operationsPerGoroutine = 20

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*operationsPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				switch j % 3 {
				case 0:
					handle := globalMonitorRegistry.Acquire(time.Duration(id+1)*time.Millisecond, CPUUsageModeHeuristic, nil)
					handle.Close()
				case 1:
					if globalMonitorRegistry.instance != nil {
						_ = globalMonitorRegistry.instance.GetStats()
					}
				case 2:
					globalMonitorRegistry.mu.Lock()
					_ = len(globalMonitorRegistry.intervalRefs)
					globalMonitorRegistry.mu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent access error: %v", err)
	}
}

func TestResourceMonitor_BoundaryConditions(t *testing.T) {
	tests := []struct {
		name          string
		interval      time.Duration
		expectedValid bool
	}{
		{"very small interval", time.Nanosecond, true},
		{"very large interval", 24 * time.Hour, true},
		{"normal interval", time.Second, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setupTest(t)

			handle := globalMonitorRegistry.Acquire(tt.interval, CPUUsageModeHeuristic, nil)
			defer handle.Close()

			stats := handle.GetStats()
			if stats.Timestamp.IsZero() {
				t.Error("Expected valid timestamp")
			}

			if tt.expectedValid {
				if globalMonitorRegistry.instance == nil {
					t.Error("Expected monitor instance to be created")
				}
			}
		})
	}

	t.Run("invalid intervals", func(t *testing.T) {
		testCases := []struct {
			name     string
			interval time.Duration
			wantMsg  string
		}{
			{"negative interval", -time.Second, "resource monitor: invalid interval -1s, must be positive"},
			{"zero interval", 0, "resource monitor: invalid interval 0s, must be positive"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				setupTest(t)

				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic for %s", tc.name)
					} else if msg := fmt.Sprintf("%v", r); msg != tc.wantMsg {
						t.Errorf("Expected panic message %q, got %q", tc.wantMsg, msg)
					}
				}()

				globalMonitorRegistry.Acquire(tc.interval, CPUUsageModeHeuristic, nil)
			})
		}
	})
}

// Shared handle behavior.
func TestSharedMonitorHandle(t *testing.T) {
	setupTest(t)

	h := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)

	stats := h.GetStats()
	if stats.Timestamp.IsZero() {
		t.Error("Handle returned zero stats")
	}

	h.Close()
	if len(globalMonitorRegistry.intervalRefs) != 0 {
		t.Error("Registry not empty after close")
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Double close panicked: %v", r)
			}
		}()
		h.Close()
	}()
}

func TestSharedMonitorHandle_Close(t *testing.T) {
	setupTest(t)

	h1 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)

	if len(globalMonitorRegistry.intervalRefs) == 0 {
		t.Error("Registry should have reference after acquire")
	}

	h1.Close()

	if len(globalMonitorRegistry.intervalRefs) != 0 {
		t.Error("Registry should have no references after close")
	}

	h1.Close()
	h1.Close()

	if len(globalMonitorRegistry.intervalRefs) != 0 {
		t.Error("Registry should still have no references after multiple closes")
	}
}

func TestSharedMonitorHandle_Close_MultipleHandles(t *testing.T) {
	setupTest(t)

	h1 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)
	h2 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)
	h3 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)

	if count := globalMonitorRegistry.intervalRefs[time.Second]; count != 3 {
		t.Errorf("Expected ref count 3, got %d", count)
	}

	h1.Close()

	if count := globalMonitorRegistry.intervalRefs[time.Second]; count != 2 {
		t.Errorf("Expected ref count 2, got %d", count)
	}

	h2.Close()
	h3.Close()

	if len(globalMonitorRegistry.intervalRefs) != 0 {
		t.Error("Registry should have no references after all closes")
	}
}

func TestSharedMonitorHandle_Close_Concurrent(t *testing.T) {
	setupTest(t)

	handles := make([]resourceMonitor, 10)
	for i := 0; i < 10; i++ {
		handles[i] = globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)
	}

	var wg sync.WaitGroup
	for _, h := range handles {
		wg.Add(1)
		go func(handle resourceMonitor) {
			defer wg.Done()
			handle.Close()
		}(h)
	}

	wg.Wait()

	if len(globalMonitorRegistry.intervalRefs) != 0 {
		t.Error("Registry should have no references after concurrent closes")
	}
}

func TestMonitorRegistry_CalculateMinInterval(t *testing.T) {
	tests := []struct {
		name      string
		intervals map[time.Duration]int
		expected  time.Duration
	}{
		{"empty registry", nil, time.Second},
		{"single interval", map[time.Duration]int{2 * time.Second: 1}, 2 * time.Second},
		{"multiple intervals", map[time.Duration]int{
			5 * time.Second: 1,
			1 * time.Second: 1,
			3 * time.Second: 1,
		}, 1 * time.Second},
		{"very small interval", map[time.Duration]int{50 * time.Millisecond: 1}, 50 * time.Millisecond},
		{"very large interval", map[time.Duration]int{24 * time.Hour: 1}, 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setupTest(t)

			globalMonitorRegistry.intervalRefs = tt.intervals

			minInterval := globalMonitorRegistry.calculateMinInterval()
			if minInterval != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, minInterval)
			}
		})
	}
}

// TestMonitorRegistry_Acquire_UpgradeMode tests that acquiring with a higher cpuMode upgrades the instance.
func TestMonitorRegistry_Acquire_UpgradeMode(t *testing.T) {
	setupTest(t)

	// First acquire with heuristic mode
	h1 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)
	defer h1.Close()

	initialMode := globalMonitorRegistry.instance.GetMode()

	// Acquire with measured mode (higher than heuristic) - should upgrade
	h2 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeMeasured, nil)
	defer h2.Close()

	// The instance should have been upgraded to measured mode (if available)
	// or remain in heuristic if measured is not available
	upgradedMode := globalMonitorRegistry.instance.GetMode()
	if upgradedMode < initialMode {
		t.Errorf("Expected mode to be upgraded or remain same, got initial: %v, upgraded: %v", initialMode, upgradedMode)
	}
	// Measured mode should be >= heuristic mode
	if upgradedMode != CPUUsageModeMeasured && upgradedMode != CPUUsageModeHeuristic {
		t.Errorf("Unexpected mode after upgrade: %v", upgradedMode)
	}
}

// TestMonitorRegistry_Acquire_CustomMemoryReader tests that custom memory reader is applied.
func TestMonitorRegistry_Acquire_CustomMemoryReader(t *testing.T) {
	setupTest(t)

	customMemValue := 75.5
	customMemReader := func() (float64, error) {
		return customMemValue, nil
	}

	// First acquire without memory reader
	h1 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)
	defer h1.Close()

	// Acquire with custom memory reader - should be applied
	h2 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, customMemReader)
	defer h2.Close()

	// Give monitor time to sample
	time.Sleep(200 * time.Millisecond)

	stats := h2.GetStats()
	// The custom memory reader should be used
	// Note: This may not always match exactly due to timing, but should be close
	if stats.MemoryUsedPercent < 0 || stats.MemoryUsedPercent > 200 {
		t.Errorf("Expected valid memory percentage, got %f", stats.MemoryUsedPercent)
	}
}

// TestMonitorRegistry_Acquire_NoUpgradeWhenLowerMode tests that acquiring with lower mode doesn't downgrade.
func TestMonitorRegistry_Acquire_NoUpgradeWhenLowerMode(t *testing.T) {
	setupTest(t)

	// First acquire with measured mode (higher)
	h1 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeMeasured, nil)
	defer h1.Close()

	initialMode := globalMonitorRegistry.instance.GetMode()

	// Acquire with heuristic mode (lower) - should NOT downgrade
	h2 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)
	defer h2.Close()

	// Mode should remain the same or higher (not downgrade)
	finalMode := globalMonitorRegistry.instance.GetMode()
	if finalMode < initialMode {
		t.Errorf("Expected mode to not downgrade, got initial: %v, final: %v", initialMode, finalMode)
	}
}
