package flow

import (
	"fmt"
	"log/slog"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/reugn/go-streams/internal/sysmonitor"
)

// CPUUsageMode defines strategies for CPU usage monitoring.
type CPUUsageMode int

const (
	// CPUUsageModeHeuristic uses goroutine count as a lightweight CPU usage proxy.
	CPUUsageModeHeuristic CPUUsageMode = iota
	// CPUUsageModeMeasured provides accurate CPU usage measurement via system calls.
	CPUUsageModeMeasured
)

// ResourceStats holds current system resource utilization metrics.
type ResourceStats struct {
	MemoryUsedPercent float64   // Memory usage as a percentage (0-100).
	CPUUsagePercent   float64   // CPU usage as a percentage (0-100).
	GoroutineCount    int       // Number of active goroutines.
	Timestamp         time.Time // Time when these stats were collected.
}

// resourceMonitor defines the interface for resource monitoring.
type resourceMonitor interface {
	GetStats() ResourceStats
	Close()
}

// ResourceMonitor collects and provides system resource usage statistics.
type ResourceMonitor struct {
	sampleInterval time.Duration
	mu             sync.Mutex              // Protects configuration changes.
	cpuMode        CPUUsageMode            // Current CPU monitoring strategy.
	memoryReader   func() (float64, error) // Custom memory usage reader.

	// Runtime state
	stats            atomic.Pointer[ResourceStats] // Latest resource statistics.
	sampler          sysmonitor.ProcessCPUSampler  // CPU usage sampler implementation.
	updateIntervalCh chan time.Duration            // Channel for dynamic interval updates.
	done             chan struct{}                 // Signals monitoring loop termination.
	closeOnce        sync.Once                     // Ensures clean shutdown.
}

// newResourceMonitor creates a new resource monitor instance.
// This constructor is private and should only be called by the registry.
func newResourceMonitor(
	sampleInterval time.Duration,
	cpuMode CPUUsageMode,
	memoryReader func() (float64, error),
) *ResourceMonitor {
	rm := &ResourceMonitor{
		sampleInterval:   sampleInterval,
		cpuMode:          cpuMode,
		memoryReader:     memoryReader,
		updateIntervalCh: make(chan time.Duration, 1),
		done:             make(chan struct{}),
	}

	// Initialize with empty stats
	rm.stats.Store(&ResourceStats{
		Timestamp: time.Now(),
	})

	rm.initSampler()

	go rm.monitor()
	return rm
}

// GetStats returns the most recent resource usage statistics.
// The returned data is thread-safe and represents a consistent snapshot.
func (rm *ResourceMonitor) GetStats() ResourceStats {
	val := rm.stats.Load()
	if val == nil {
		return ResourceStats{}
	}
	return *val
}

// GetMode returns the current CPU monitoring strategy.
func (rm *ResourceMonitor) GetMode() CPUUsageMode {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.cpuMode
}

// SetMode changes the CPU monitoring strategy if possible.
// Allows switching between heuristic and measured modes.
func (rm *ResourceMonitor) SetMode(newMode CPUUsageMode) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// No change needed
	if newMode == rm.cpuMode {
		return
	}

	switch newMode {
	case CPUUsageModeMeasured:
		// Try to switch to measured mode
		if sampler, err := sysmonitor.NewProcessSampler(); err == nil {
			rm.sampler = sampler
			rm.cpuMode = CPUUsageModeMeasured
		} else {
			slog.Error("failed to switch to measured mode", "error", err)
		}
	case CPUUsageModeHeuristic:
		rm.sampler = sysmonitor.NewGoroutineHeuristicSampler()
		rm.cpuMode = CPUUsageModeHeuristic
	}
}

// initSampler initializes the appropriate CPU usage sampler.
// Uses measured mode by default if available
func (rm *ResourceMonitor) initSampler() {
	if sampler, err := sysmonitor.NewProcessSampler(); err == nil {
		rm.sampler = sampler
		rm.cpuMode = CPUUsageModeMeasured
	} else {
		// Fallback to heuristic
		rm.sampler = sysmonitor.NewGoroutineHeuristicSampler()
		rm.cpuMode = CPUUsageModeHeuristic
	}
}

// monitor runs the continuous resource sampling loop.
// Handles dynamic interval changes and graceful shutdown.
func (rm *ResourceMonitor) monitor() {
	ticker := time.NewTicker(rm.sampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rm.done:
			return
		case d := <-rm.updateIntervalCh:
			rm.mu.Lock()
			if d != rm.sampleInterval {
				rm.sampleInterval = d
				ticker.Stop()
				ticker = time.NewTicker(rm.sampleInterval)
				rm.sample()
			}
			rm.mu.Unlock()
		case <-ticker.C:
			rm.sample()
		}
	}
}

// sample collects current CPU, memory, and goroutine statistics.
// Updates the atomic stats pointer with the latest measurements.
func (rm *ResourceMonitor) sample() {
	stats := &ResourceStats{
		Timestamp:      time.Now(),
		GoroutineCount: runtime.NumGoroutine(),
	}

	// Memory Usage
	// Check if a custom memory reader is provided
	if rm.memoryReader != nil {
		if mem, err := rm.memoryReader(); err == nil {
			stats.MemoryUsedPercent = mem
		}
		// If not, use system memory stats
	} else if memStats, err := sysmonitor.GetSystemMemory(); err == nil && memStats.Total > 0 {
		used := memStats.Total - memStats.Available
		stats.MemoryUsedPercent = float64(used) / float64(memStats.Total) * 100
	} else {
		// Fallback to runtime memory stats
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if m.Sys > 0 {
			stats.MemoryUsedPercent = float64(m.Alloc) / float64(m.Sys) * 100
		}
	}

	// CPU Usage
	cpu := rm.sampler.Sample(rm.sampleInterval)
	stats.CPUUsagePercent = cpu

	rm.stats.Store(stats)
}

// getSampleInterval thread-safe getter of the current sample interval.
func (rm *ResourceMonitor) getSampleInterval() time.Duration {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.sampleInterval
}

// setInterval updates the sampling frequency dynamically.
func (rm *ResourceMonitor) setInterval(d time.Duration) {
	select {
	case rm.updateIntervalCh <- d:
	case <-rm.done:
	}
}

// stop terminates the monitoring goroutine gracefully.
func (rm *ResourceMonitor) stop() {
	rm.closeOnce.Do(func() {
		close(rm.done)
	})
}

// globalMonitorRegistry manages the singleton ResourceMonitor instance.
// Provides shared access with reference counting and automatic cleanup.
var globalMonitorRegistry = &monitorRegistry{
	intervalRefs: make(map[time.Duration]int),
}

// monitorIdleTimeout defines how long to wait before stopping an unused monitor.
// Prevents unnecessary recreation of the monitor instance when new throttler is added.
const monitorIdleTimeout = 5 * time.Second

// monitorRegistry coordinates shared access to a ResourceMonitor instance.
// Manages multiple consumers with different sampling requirements efficiently.
type monitorRegistry struct {
	mu           sync.Mutex            // Protects registry state.
	instance     *ResourceMonitor      // The shared monitor instance.
	intervalRefs map[time.Duration]int // Reference counts per sampling interval.
	currentMin   time.Duration         // Current minimum sampling interval.
	stopTimer    *time.Timer           // Timer for delayed cleanup.
}

// Acquire obtains a handle to the shared resource monitor.
// Manages reference counting and may create or reconfigure the monitor as needed.
func (r *monitorRegistry) Acquire(
	requestedInterval time.Duration,
	cpuMode CPUUsageMode,
	memReader func() (float64, error),
) resourceMonitor {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Validate the requested interval
	if requestedInterval <= 0 {
		panic(fmt.Sprintf("resource monitor: invalid interval %v, must be positive", requestedInterval))
	}

	// Cancel pending stop if we are resurrecting within the grace period
	if r.stopTimer != nil {
		r.stopTimer.Stop()
		r.stopTimer = nil
	}

	// Register the requested interval
	r.intervalRefs[requestedInterval]++

	// Calculate global minimum
	requiredMin := r.calculateMinInterval()

	if r.instance == nil {
		r.instance = newResourceMonitor(requiredMin, cpuMode, memReader)
		r.currentMin = requiredMin
	} else {
		// Check if we need to upgrade the existing instance
		if cpuMode > r.instance.GetMode() {
			r.instance.SetMode(cpuMode)
		}

		// Adjust interval if this new user needs it faster
		if requiredMin != r.currentMin {
			r.instance.setInterval(requiredMin)
			r.currentMin = requiredMin
		}
	}

	return &sharedMonitorHandle{
		monitor:  r.instance,
		interval: requestedInterval,
		registry: r,
	}
}

// release decrements the reference count for a sampling interval.
// Initiates cleanup when no consumers remain.
func (r *monitorRegistry) release(interval time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.intervalRefs[interval]--
	if r.intervalRefs[interval] <= 0 {
		delete(r.intervalRefs, interval)
	}

	if len(r.intervalRefs) == 0 {
		// Start grace period timer
		if r.stopTimer == nil {
			r.stopTimer = time.AfterFunc(monitorIdleTimeout, func() {
				r.cleanup()
			})
		}
		return
	}

	// If we still have users, check if we can slow down (release pressure)
	newMin := r.calculateMinInterval()
	if newMin != r.currentMin && r.instance != nil {
		r.instance.setInterval(newMin)
		r.currentMin = newMin
	}
}

// cleanup stops and destroys the monitor instance.
// Called after the idle timeout when no consumers remain.
func (r *monitorRegistry) cleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double check we are still empty
	if len(r.intervalRefs) == 0 && r.instance != nil {
		r.instance.stop()
		r.instance = nil
	}
	r.stopTimer = nil
}

// calculateMinInterval finds the fastest sampling rate required by any consumer.
// Returns a default interval when no consumers are registered.
func (r *monitorRegistry) calculateMinInterval() time.Duration {
	if len(r.intervalRefs) == 0 {
		return time.Second
	}
	minInterval := time.Duration(math.MaxInt64)
	for d := range r.intervalRefs {
		if d < minInterval {
			minInterval = d
		}
	}
	return minInterval
}

// getInstanceSampleInterval returns the current instance's sample interval in a thread-safe manner.
// Returns 0 if no instance exists.
func (r *monitorRegistry) getInstanceSampleInterval() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.instance == nil {
		return 0
	}
	return r.instance.getSampleInterval()
}

// sharedMonitorHandle provides consumers with access to the shared monitor.
// Ensures proper reference counting and cleanup when no longer needed.
type sharedMonitorHandle struct {
	monitor  *ResourceMonitor // Reference to the shared monitor.
	interval time.Duration    // Sampling interval requested by this consumer.
	registry *monitorRegistry // Registry managing this handle.
	once     sync.Once        // Ensures Close is called only once.
}

// GetStats returns resource statistics from the shared monitor.
func (h *sharedMonitorHandle) GetStats() ResourceStats {
	return h.monitor.GetStats()
}

// Close releases this consumer's reference to the shared monitor.
func (h *sharedMonitorHandle) Close() {
	h.once.Do(func() {
		h.registry.release(h.interval)
	})
}
