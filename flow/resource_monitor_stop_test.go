package flow

import (
	"sync"
	"testing"
	"time"

	"github.com/reugn/go-streams/internal/testutil"
)

func TestResourceMonitor_Stop(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(testutil.TestSampleInterval, CPUUsageModeHeuristic, nil)

	initialStats := rm.GetStats()
	if initialStats.Timestamp.IsZero() {
		t.Fatal("Monitor should be running and producing stats")
	}

	time.Sleep(testutil.TestStopTimeout)
	statsBeforeStop := rm.GetStats()
	if !statsBeforeStop.Timestamp.After(initialStats.Timestamp) {
		t.Fatal("Monitor should have sampled at least once")
	}

	rm.stop()

	time.Sleep(testutil.TestStopTimeout)
	statsAfterStop := rm.GetStats()

	if statsAfterStop.Timestamp.Sub(statsBeforeStop.Timestamp) > testutil.TestStatsUpdateMargin {
		t.Error("Monitor should have stopped, stats should not update significantly")
	}

	rm.stop()
	rm.stop()
}

func TestResourceMonitor_Stop_Concurrent(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(100*time.Millisecond, CPUUsageModeHeuristic, nil)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rm.stop()
		}()
	}

	wg.Wait()

	time.Sleep(200 * time.Millisecond)
	stats1 := rm.GetStats()
	time.Sleep(200 * time.Millisecond)
	stats2 := rm.GetStats()

	if stats2.Timestamp.After(stats1.Timestamp.Add(50 * time.Millisecond)) {
		t.Error("Monitor should be stopped, stats should not update")
	}
}

func TestResourceMonitor_GetStats_Concurrent(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(100*time.Millisecond, CPUUsageModeHeuristic, nil)
	defer rm.stop()

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats := rm.GetStats()
			if stats.Timestamp.IsZero() {
				errors <- errInvalidStatsError("got zero timestamp")
			}
			if stats.GoroutineCount < 0 {
				errors <- errInvalidStatsError("got invalid goroutine count")
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

func TestResourceMonitor_GetStats_NilStats(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(time.Hour, CPUUsageModeHeuristic, nil)
	defer rm.stop()

	rm.stats.Store(nil)

	stats := rm.GetStats()

	if !stats.Timestamp.IsZero() {
		t.Error("Expected zero timestamp for nil stats")
	}
	if stats.CPUUsagePercent != 0 {
		t.Errorf("Expected zero CPU usage, got %f", stats.CPUUsagePercent)
	}
	if stats.MemoryUsedPercent != 0 {
		t.Errorf("Expected zero memory usage, got %f", stats.MemoryUsedPercent)
	}
	if stats.GoroutineCount != 0 {
		t.Errorf("Expected zero goroutine count, got %d", stats.GoroutineCount)
	}
}

// errInvalidStatsError is a simple error type for testing invalid stats.
type errInvalidStatsError string

func (e errInvalidStatsError) Error() string { return string(e) }
