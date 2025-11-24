package flow

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/reugn/go-streams/internal/sysmonitor"
)

const (
	testSampleInterval    = 10 * time.Millisecond
	testStopTimeout       = 50 * time.Millisecond
	testStatsUpdateMargin = 20 * time.Millisecond
)

type mockCPUSampler struct {
	val float64
}

func (m *mockCPUSampler) Sample(_ time.Duration) float64 {
	return m.val
}

func (m *mockCPUSampler) Reset() {
	// No-op for mock
}

func (m *mockCPUSampler) IsInitialized() bool {
	return true // Mock is always initialized
}

type assertError string

func (e assertError) Error() string { return string(e) }

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

// assertValidStats checks that resource stats contain reasonable values
func assertValidStats(t *testing.T, stats ResourceStats) {
	t.Helper()
	if stats.Timestamp.IsZero() {
		t.Error("Timestamp should not be zero")
	}
	if stats.GoroutineCount < 0 {
		t.Errorf("GoroutineCount should not be negative, got %d", stats.GoroutineCount)
	}
	if stats.MemoryUsedPercent < 0 || stats.MemoryUsedPercent > 200 { // Allow >100% for some systems
		t.Errorf("MemoryUsedPercent should be between 0-200, got %f", stats.MemoryUsedPercent)
	}
	if stats.CPUUsagePercent < 0 {
		t.Errorf("CPUUsagePercent should not be negative, got %f", stats.CPUUsagePercent)
	}
}

// TestResourceMonitor_Initialization tests monitor creation with different CPU modes
func TestResourceMonitor_Initialization(t *testing.T) {
	setupTest(t)

	// Test with Heuristic preference - should use best available (measured if possible)
	rm := newResourceMonitor(time.Second, CPUUsageModeHeuristic, nil)
	if rm.sampler == nil {
		t.Error("Expected sampler to be initialized, got nil")
	}
	// Should use measured mode if available, regardless of preference
	if rm.cpuMode != CPUUsageModeMeasured && rm.cpuMode != CPUUsageModeHeuristic {
		t.Errorf("Unexpected CPU mode: %v", rm.cpuMode)
	}
	rm.stop()

	// Test with Measured preference - should use measured if available
	rm2 := newResourceMonitor(time.Second, CPUUsageModeMeasured, nil)
	if rm2.sampler == nil {
		t.Error("Expected sampler to be initialized, got nil")
	}
	// Should use measured mode if available
	if rm2.cpuMode != CPUUsageModeMeasured && rm2.cpuMode != CPUUsageModeHeuristic {
		t.Errorf("Unexpected CPU mode: %v", rm2.cpuMode)
	}
	rm2.stop()
}

// TestResourceMonitor_Sample_MemoryReader tests memory sampling with custom reader
func TestResourceMonitor_Sample_MemoryReader(t *testing.T) {
	setupTest(t)

	expectedMem := 42.5
	mockMemReader := func() (float64, error) {
		return expectedMem, nil
	}

	// Create monitor with long interval to prevent auto-sampling during test
	rm := newResourceMonitor(time.Hour, CPUUsageModeHeuristic, mockMemReader)
	defer rm.stop()

	// Get initial stats before manual sample
	initialStats := rm.GetStats()

	// Trigger sample manually
	rm.sample()

	stats := rm.GetStats()

	// Verify Memory
	if stats.MemoryUsedPercent != expectedMem {
		t.Errorf("Expected memory %f, got %f", expectedMem, stats.MemoryUsedPercent)
	}

	// Verify stats are valid and updated
	assertValidStats(t, stats)
	if !stats.Timestamp.After(initialStats.Timestamp) {
		t.Error("Timestamp should be updated after sampling")
	}
}

// TestResourceMonitor_Sample_CPUMock tests CPU sampling with mock sampler
func TestResourceMonitor_Sample_CPUMock(t *testing.T) {
	setupTest(t)

	expectedCPU := 12.34
	mockSampler := &mockCPUSampler{val: expectedCPU}

	// Initialize with Measured mode
	rm := newResourceMonitor(time.Hour, CPUUsageModeMeasured, nil)
	defer rm.stop()

	// Inject mock sampler manually (overwriting the real one)
	rm.sampler = mockSampler

	rm.sample()
	stats := rm.GetStats()

	if stats.CPUUsagePercent != expectedCPU {
		t.Errorf("Expected CPU %f, got %f", expectedCPU, stats.CPUUsagePercent)
	}
}

// TestResourceMonitor_Sample_HeuristicMode tests heuristic CPU sampling
func TestResourceMonitor_Sample_HeuristicMode(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(time.Hour, CPUUsageModeHeuristic, nil)
	defer rm.stop()

	// Force heuristic mode by setting the sampler directly
	rm.sampler = sysmonitor.NewGoroutineHeuristicSampler()
	rm.cpuMode = CPUUsageModeHeuristic

	rm.sample()
	stats := rm.GetStats()

	// Heuristic mode should use the GoroutineHeuristicSampler
	// The sampler returns a sophisticated calculation based on goroutine count
	if stats.CPUUsagePercent <= 0 {
		t.Errorf("Expected positive CPU usage in heuristic mode, got %f", stats.CPUUsagePercent)
	}
	if stats.CPUUsagePercent > 100 {
		t.Errorf("Expected CPU usage <= 100%%, got %f", stats.CPUUsagePercent)
	}
}

// TestMonitorRegistry_Acquire tests registry acquire logic with different intervals
func TestMonitorRegistry_Acquire(t *testing.T) {
	setupTest(t)

	// 1. First Acquire
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

	// 2. Second Acquire (Same interval) - should just increment ref
	h2 := globalMonitorRegistry.Acquire(2*time.Second, CPUUsageModeHeuristic, nil)
	defer h2.Close()

	if count := globalMonitorRegistry.intervalRefs[2*time.Second]; count != 2 {
		t.Errorf("Expected ref count 2, got %d", count)
	}

	// 3. Third Acquire (Faster interval) - should update monitor
	h3 := globalMonitorRegistry.Acquire(1*time.Second, CPUUsageModeHeuristic, nil)
	defer h3.Close()

	if globalMonitorRegistry.currentMin != 1*time.Second {
		t.Errorf("Expected currentMin to update to 1s, got %v", globalMonitorRegistry.currentMin)
	}

	// Wait for the monitor goroutine to process the interval update (buffered channel makes this async)
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

	// 4. Fourth Acquire (Slower interval) - should NOT update monitor
	h4 := globalMonitorRegistry.Acquire(5*time.Second, CPUUsageModeHeuristic, nil)
	defer h4.Close()

	if globalMonitorRegistry.currentMin != 1*time.Second {
		t.Errorf("Expected currentMin to remain 1s, got %v", globalMonitorRegistry.currentMin)
	}
}

// TestMonitorRegistry_Release_Logic tests registry release and cleanup logic
func TestMonitorRegistry_Release_Logic(t *testing.T) {
	setupTest(t)

	// Acquire 1s and 5s
	hFast := globalMonitorRegistry.Acquire(1*time.Second, CPUUsageModeHeuristic, nil)
	hSlow := globalMonitorRegistry.Acquire(5*time.Second, CPUUsageModeHeuristic, nil)

	// Initial State
	if globalMonitorRegistry.currentMin != 1*time.Second {
		t.Fatalf("Setup failed: expected 1s min")
	}

	// Release Fast Handle
	hFast.Close()

	// Registry should check if it can slow down
	// Now only 5s is left, so min should become 5s
	if globalMonitorRegistry.currentMin != 5*time.Second {
		t.Errorf("Expected currentMin to relax to 5s, got %v", globalMonitorRegistry.currentMin)
	}

	// Release Slow Handle
	hSlow.Close()

	// Check that ref count is zero
	if len(globalMonitorRegistry.intervalRefs) != 0 {
		t.Errorf("Expected empty refs, got %v", globalMonitorRegistry.intervalRefs)
	}

	// Verify cleanup timer is started
	if globalMonitorRegistry.stopTimer == nil {
		t.Error("Expected stopTimer to be set after full release")
	}

	// Force cleanup manually to verify cleanup logic works (instead of waiting 5s)
	globalMonitorRegistry.cleanup()
	if globalMonitorRegistry.instance != nil {
		t.Error("Expected instance to be nil after cleanup")
	}
}

// TestMonitorRegistry_Resurrect tests registry resurrection after cleanup timer starts
func TestMonitorRegistry_Resurrect(t *testing.T) {
	setupTest(t)

	// Acquire and Release to trigger timer
	h1 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)
	h1.Close()

	if globalMonitorRegistry.stopTimer == nil {
		t.Fatal("Timer should be running")
	}

	// Acquire again BEFORE timer fires
	h2 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)
	defer h2.Close()

	if globalMonitorRegistry.stopTimer != nil {
		t.Error("Timer should be stopped/nil after resurrection")
	}
	if globalMonitorRegistry.instance == nil {
		t.Error("Instance should still be alive")
	}
}

// TestResourceMonitor_SetInterval_Dynamic tests dynamic interval changes
func TestResourceMonitor_SetInterval_Dynamic(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(10*time.Minute, CPUUsageModeHeuristic, nil)
	defer rm.stop()

	// Since we can't easily hook into the loop, we observe the effect.
	// We change interval to something very short, which should trigger 'sample()'

	// Current state
	initialStats := rm.GetStats()

	// Change interval
	newDuration := time.Millisecond
	rm.setInterval(newDuration)

	// Wait for update
	// The loop: case d := <-rm.updateIntervalCh -> sets rm.sampleInterval -> calls rm.sample()

	// We poll briefly for the change
	deadline := time.Now().Add(1 * time.Second)
	updated := false
	for time.Now().Before(deadline) {
		if rm.getSampleInterval() == newDuration {
			updated = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !updated {
		t.Error("Failed to update interval dynamically")
	}

	// Ensure stats timestamp updated (proof that sample() was called on switch)
	time.Sleep(time.Millisecond)
	currentStats := rm.GetStats()
	if !currentStats.Timestamp.After(initialStats.Timestamp) {
		t.Error("Expected immediate sample on interval switch")
	}
}

// TestSharedMonitorHandle tests handle functionality and idempotent close
func TestSharedMonitorHandle(t *testing.T) {
	setupTest(t)

	h := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)

	// Test GetStats delegates correctly
	stats := h.GetStats()
	if stats.Timestamp.IsZero() {
		t.Error("Handle returned zero stats")
	}

	// Test Idempotent Close
	h.Close()
	// Access internal registry to verify release happened
	if len(globalMonitorRegistry.intervalRefs) != 0 {
		t.Error("Registry not empty after close")
	}

	// Second close should not panic and not change refs (already 0)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Double close panicked: %v", r)
			}
		}()
		h.Close()
	}()
}

// TestMemoryFallback tests fallback to runtime memory stats when reader is nil
func TestMemoryFallback(t *testing.T) {
	setupTest(t)

	// Test fallback when memoryReader is nil
	rm := newResourceMonitor(time.Hour, CPUUsageModeHeuristic, nil)
	defer rm.stop()

	rm.sample()
	stats := rm.GetStats()

	// Memory percentage should be a valid value (0-100)
	if stats.MemoryUsedPercent < 0 || stats.MemoryUsedPercent > 100 {
		t.Errorf("Expected memory percentage in range [0,100], got %f", stats.MemoryUsedPercent)
	}

	// Should have other stats populated
	if stats.GoroutineCount <= 0 {
		t.Errorf("Expected goroutine count > 0, got %d", stats.GoroutineCount)
	}
	if stats.CPUUsagePercent < 0 {
		t.Errorf("Expected CPU usage >= 0, got %f", stats.CPUUsagePercent)
	}
	if stats.Timestamp.IsZero() {
		t.Error("Expected non-zero timestamp")
	}
}

// TestMemoryReaderError tests error handling in memory reader with fallback
func TestMemoryReaderError(t *testing.T) {
	setupTest(t)

	// Reader that returns error
	errReader := func() (float64, error) {
		return 0, assertError("fail")
	}

	rm := newResourceMonitor(time.Hour, CPUUsageModeHeuristic, errReader)
	defer rm.stop()

	initialStats := rm.GetStats()
	rm.sample()
	stats := rm.GetStats()

	// Should fall back to runtime stats
	if stats.MemoryUsedPercent < 0 {
		t.Errorf("Expected memory percentage >= 0, got %f", stats.MemoryUsedPercent)
	}
	if stats.MemoryUsedPercent > 100 {
		t.Errorf("Expected memory percentage <= 100, got %f", stats.MemoryUsedPercent)
	}

	// Other stats should still be valid
	if stats.GoroutineCount <= 0 {
		t.Errorf("Expected goroutine count > 0, got %d", stats.GoroutineCount)
	}
	if stats.CPUUsagePercent < 0 {
		t.Errorf("Expected CPU usage >= 0, got %f", stats.CPUUsagePercent)
	}
	if !stats.Timestamp.After(initialStats.Timestamp) {
		t.Error("Expected timestamp to be updated")
	}
}

// TestResourceMonitor_CPU_SamplerError tests CPU sampler error handling
func TestResourceMonitor_CPU_SamplerError(t *testing.T) {
	setupTest(t)

	// Create monitor and inject a failing sampler
	rm := newResourceMonitor(time.Hour, CPUUsageModeMeasured, nil)
	defer rm.stop()

	// Create a mock sampler that returns negative values (error condition)
	failingSampler := &mockCPUSampler{val: -1.0}
	rm.sampler = failingSampler

	rm.sample()
	stats := rm.GetStats()

	// CPU sampler can return negative values, so we just check it's a valid float
	// The implementation doesn't clamp negative values, it passes them through
	if math.IsNaN(stats.CPUUsagePercent) {
		t.Error("Expected valid CPU usage value, got NaN")
	}

	// Other stats should still be valid
	if stats.GoroutineCount <= 0 {
		t.Errorf("Expected goroutine count > 0, got %d", stats.GoroutineCount)
	}
	if stats.MemoryUsedPercent < 0 {
		t.Errorf("Expected memory percentage >= 0, got %f", stats.MemoryUsedPercent)
	}
}

// TestResourceMonitor_SetMode tests CPU monitoring mode switching
func TestResourceMonitor_SetMode(t *testing.T) {
	setupTest(t)

	// Create monitor - should try measured mode first
	rm := newResourceMonitor(time.Second, CPUUsageModeHeuristic, nil)
	defer rm.stop()

	// Should use measured mode if available (new behavior)
	initialMode := rm.GetMode()
	if initialMode != CPUUsageModeMeasured && initialMode != CPUUsageModeHeuristic {
		t.Errorf("Unexpected initial mode: %v", initialMode)
	}

	// Test switching to Heuristic mode
	rm.SetMode(CPUUsageModeHeuristic)
	if rm.GetMode() != CPUUsageModeHeuristic {
		t.Errorf("Expected mode Heuristic after switching, got %v", rm.GetMode())
	}

	// Test switching back to Measured mode
	rm.SetMode(CPUUsageModeMeasured)
	if rm.GetMode() != CPUUsageModeMeasured {
		t.Errorf("Expected mode Measured after switching back, got %v", rm.GetMode())
	}

	// Test switching to the same mode (should be no-op)
	rm.SetMode(CPUUsageModeMeasured)
	if rm.GetMode() != CPUUsageModeMeasured {
		t.Errorf("Expected mode to remain Measured, got %v", rm.GetMode())
	}
}

// TestMonitorRegistry_BufferedChannel_DeadlockPrevention
func TestMonitorRegistry_BufferedChannel_DeadlockPrevention(t *testing.T) {
	setupTest(t)

	// Create a blocking memory reader to simulate slow I/O
	sampleStarted := make(chan bool, 1)
	sampleContinue := make(chan bool, 1)
	blockingMemoryReader := func() (float64, error) {
		sampleStarted <- true
		<-sampleContinue // Block until signaled to continue
		return 50.0, nil
	}

	// Start first acquire with blocking memory reader
	handle1 := globalMonitorRegistry.Acquire(200*time.Millisecond, CPUUsageModeHeuristic, blockingMemoryReader)
	defer handle1.Close()

	// Wait for monitor to start sampling and block
	select {
	case <-sampleStarted:
		// Monitor is now blocked in memoryReader
	case <-time.After(1 * time.Second):
		t.Fatal("Monitor didn't start sampling within timeout")
	}

	// Now try to acquire with different interval - this triggers setInterval
	// With unbuffered channel, this would deadlock holding registry lock
	start := time.Now()
	handle2 := globalMonitorRegistry.Acquire(50*time.Millisecond, CPUUsageModeHeuristic, nil)
	defer handle2.Close()

	// The acquire should complete quickly with buffered channel
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Errorf("Acquire took too long (%v) - possible blocking", elapsed)
	}

	// Unblock the memory reader so monitor can process the update
	sampleContinue <- true

	// Give monitor time to process the update
	time.Sleep(50 * time.Millisecond)
}

// TestMonitorRegistry_ConcurrentAccess tests concurrent access to the registry
func TestMonitorRegistry_ConcurrentAccess(t *testing.T) {
	setupTest(t)

	const numGoroutines = 5
	const operationsPerGoroutine = 20

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*operationsPerGoroutine)

	// Launch multiple goroutines performing concurrent operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				switch j % 3 {
				case 0: // Acquire and release
					handle := globalMonitorRegistry.Acquire(time.Duration(id+1)*time.Millisecond, CPUUsageModeHeuristic, nil)
					handle.Close()
				case 1: // Get stats if available
					if globalMonitorRegistry.instance != nil {
						_ = globalMonitorRegistry.instance.GetStats()
					}
				case 2: // Registry inspection
					globalMonitorRegistry.mu.Lock()
					_ = len(globalMonitorRegistry.intervalRefs)
					globalMonitorRegistry.mu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for any errors (panics would be caught here)
	for err := range errors {
		t.Errorf("Concurrent access error: %v", err)
	}
}

// TestResourceMonitor_BoundaryConditions tests edge cases and boundary conditions
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

			// Test registry acquire with boundary intervals
			handle := globalMonitorRegistry.Acquire(tt.interval, CPUUsageModeHeuristic, nil)
			defer handle.Close()

			// Should not panic and should return valid stats
			stats := handle.GetStats()
			if stats.Timestamp.IsZero() {
				t.Error("Expected valid timestamp")
			}

			// For valid intervals, check that monitor was created
			if tt.expectedValid {
				if globalMonitorRegistry.instance == nil {
					t.Error("Expected monitor instance to be created")
				}
			}
		})
	}

	// Test invalid intervals cause validation panic
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

				// This should panic with validation error
				globalMonitorRegistry.Acquire(tc.interval, CPUUsageModeHeuristic, nil)
			})
		}
	})
}

// TestResourceMonitor_ExtremeMemoryValues tests handling of extreme memory values
func TestResourceMonitor_ExtremeMemoryValues(t *testing.T) {
	setupTest(t)

	testCases := []struct {
		name     string
		memValue float64
	}{
		{"zero memory", 0.0},
		{"negative memory", -50.0},
		{"normal memory", 75.5},
		{"max memory", 100.0},
		{"over max memory", 150.0},
		{"very large memory", 1e6},
		{"NaN memory", math.NaN()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockReader := func() (float64, error) {
				return tc.memValue, nil
			}

			rm := newResourceMonitor(time.Hour, CPUUsageModeHeuristic, mockReader)
			defer rm.stop()

			rm.sample()
			stats := rm.GetStats()

			// The implementation currently passes through any value from the custom reader
			// without validation. This documents the current behavior.
			if stats.MemoryUsedPercent != tc.memValue && !math.IsNaN(tc.memValue) {
				t.Errorf("Expected memory %f, got %f", tc.memValue, stats.MemoryUsedPercent)
			}
			if math.IsNaN(tc.memValue) && !math.IsNaN(stats.MemoryUsedPercent) {
				t.Errorf("Expected NaN memory, got %f", stats.MemoryUsedPercent)
			}

			// Always check other stats are reasonable
			if stats.GoroutineCount <= 0 {
				t.Errorf("Expected goroutine count > 0, got %d", stats.GoroutineCount)
			}
		})
	}
}

// TestResourceMonitor_Stop tests the stop method for cleanup and shutdown
func TestResourceMonitor_Stop(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(testSampleInterval, CPUUsageModeHeuristic, nil)

	// Verify monitor is running
	initialStats := rm.GetStats()
	if initialStats.Timestamp.IsZero() {
		t.Fatal("Monitor should be running and producing stats")
	}

	// Wait for sampling to occur
	time.Sleep(testStopTimeout)
	statsBeforeStop := rm.GetStats()
	if !statsBeforeStop.Timestamp.After(initialStats.Timestamp) {
		t.Fatal("Monitor should have sampled at least once")
	}

	// Stop the monitor
	rm.stop()

	// Verify monitor has stopped by checking no more updates occur
	time.Sleep(testStopTimeout)
	statsAfterStop := rm.GetStats()

	// Allow small timing variations but ensure no significant updates
	if statsAfterStop.Timestamp.Sub(statsBeforeStop.Timestamp) > testStatsUpdateMargin {
		t.Error("Monitor should have stopped, stats should not update significantly")
	}

	// Test idempotent stop
	rm.stop() // Should not panic
	rm.stop() // Should not panic
}

// TestResourceMonitor_Stop_Concurrent tests concurrent stop calls
func TestResourceMonitor_Stop_Concurrent(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(100*time.Millisecond, CPUUsageModeHeuristic, nil)

	// Call stop concurrently
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rm.stop()
		}()
	}

	wg.Wait()

	// Should not panic and monitor should be stopped
	time.Sleep(200 * time.Millisecond)
	stats1 := rm.GetStats()
	time.Sleep(200 * time.Millisecond)
	stats2 := rm.GetStats()

	// Stats should not update after stop
	if stats2.Timestamp.After(stats1.Timestamp.Add(50 * time.Millisecond)) {
		t.Error("Monitor should be stopped, stats should not update")
	}
}

// TestResourceMonitor_GetStats_Concurrent tests concurrent GetStats calls
func TestResourceMonitor_GetStats_Concurrent(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(100*time.Millisecond, CPUUsageModeHeuristic, nil)
	defer rm.stop()

	// Call GetStats concurrently
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats := rm.GetStats()
			// Verify stats are valid
			if stats.Timestamp.IsZero() {
				errors <- fmt.Errorf("got zero timestamp")
			}
			if stats.GoroutineCount < 0 {
				errors <- fmt.Errorf("got invalid goroutine count: %d", stats.GoroutineCount)
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}
}

// TestResourceMonitor_GetStats_NilPointer tests GetStats with nil pointer
func TestResourceMonitor_GetStats_NilPointer(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(time.Hour, CPUUsageModeHeuristic, nil)
	defer rm.stop()

	// Manually set stats to nil (simulating edge case)
	rm.stats.Store(nil)

	// GetStats should return empty ResourceStats, not panic
	stats := rm.GetStats()
	if !stats.Timestamp.IsZero() {
		t.Error("Expected zero timestamp for nil stats")
	}
}

// TestSharedMonitorHandle_Close tests Close method for cleanup
func TestSharedMonitorHandle_Close(t *testing.T) {
	setupTest(t)

	// Acquire a handle
	h1 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)

	// Verify registry has reference
	if len(globalMonitorRegistry.intervalRefs) == 0 {
		t.Error("Registry should have reference after acquire")
	}

	// Close the handle
	h1.Close()

	// Verify reference is released
	if len(globalMonitorRegistry.intervalRefs) != 0 {
		t.Error("Registry should have no references after close")
	}

	// Test idempotent close
	h1.Close() // Should not panic
	h1.Close() // Should not panic

	// Verify still no references
	if len(globalMonitorRegistry.intervalRefs) != 0 {
		t.Error("Registry should still have no references after multiple closes")
	}
}

// TestSharedMonitorHandle_Close_MultipleHandles tests Close with multiple handles
func TestSharedMonitorHandle_Close_MultipleHandles(t *testing.T) {
	setupTest(t)

	// Acquire multiple handles with same interval
	h1 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)
	h2 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)
	h3 := globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)

	// Verify ref count is 3
	if count := globalMonitorRegistry.intervalRefs[time.Second]; count != 3 {
		t.Errorf("Expected ref count 3, got %d", count)
	}

	// Close one handle
	h1.Close()

	// Verify ref count is 2
	if count := globalMonitorRegistry.intervalRefs[time.Second]; count != 2 {
		t.Errorf("Expected ref count 2, got %d", count)
	}

	// Close remaining handles
	h2.Close()
	h3.Close()

	// Verify no references
	if len(globalMonitorRegistry.intervalRefs) != 0 {
		t.Error("Registry should have no references after all closes")
	}
}

// TestSharedMonitorHandle_Close_Concurrent tests concurrent Close calls
func TestSharedMonitorHandle_Close_Concurrent(t *testing.T) {
	setupTest(t)

	// Acquire multiple handles
	handles := make([]resourceMonitor, 10)
	for i := 0; i < 10; i++ {
		handles[i] = globalMonitorRegistry.Acquire(time.Second, CPUUsageModeHeuristic, nil)
	}

	// Close all handles concurrently
	var wg sync.WaitGroup
	for _, h := range handles {
		wg.Add(1)
		go func(handle resourceMonitor) {
			defer wg.Done()
			handle.Close()
		}(h)
	}

	wg.Wait()

	// Verify all references are released
	if len(globalMonitorRegistry.intervalRefs) != 0 {
		t.Error("Registry should have no references after concurrent closes")
	}
}

// TestMonitorRegistry_CalculateMinInterval tests calculateMinInterval function
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
			setupTest(t) // Ensure clean state

			// Set up test intervals
			globalMonitorRegistry.intervalRefs = tt.intervals

			minInterval := globalMonitorRegistry.calculateMinInterval()
			if minInterval != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, minInterval)
			}
		})
	}
}

// TestMonitorRegistry_CalculateMinInterval_EdgeCases tests edge cases for calculateMinInterval
func TestMonitorRegistry_CalculateMinInterval_EdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		intervals map[time.Duration]int
		expected  time.Duration
	}{
		{"single nanosecond", map[time.Duration]int{time.Nanosecond: 1}, time.Nanosecond},
		{"multiple same intervals", map[time.Duration]int{time.Second: 5}, time.Second},
		{"mixed with duplicates", map[time.Duration]int{
			100 * time.Millisecond: 2,
			200 * time.Millisecond: 1,
		}, 100 * time.Millisecond},
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
