package flow

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/reugn/go-streams/internal/sysmonitor"
)

// CPUUsageMode defines the strategy for sampling CPU usage
type CPUUsageMode int

const (
	// CPUUsageModeHeuristic uses goroutine count as a simple CPU usage proxy
	CPUUsageModeHeuristic CPUUsageMode = iota
	// CPUUsageModeMeasured attempts to measure actual process CPU usage via gopsutil
	CPUUsageModeMeasured
)

// ResourceStats represents current system resource statistics
type ResourceStats struct {
	MemoryUsedPercent float64
	CPUUsagePercent   float64
	GoroutineCount    int
	Timestamp         time.Time
}

// ResourceMonitor monitors system resources and provides current statistics
type ResourceMonitor struct {
	sampleInterval  time.Duration
	memoryThreshold float64
	cpuThreshold    float64
	cpuMode         CPUUsageMode

	// Current stats (atomic for thread-safe reads)
	stats atomic.Value // *ResourceStats

	// CPU sampling
	sampler sysmonitor.ProcessCPUSampler

	// Reusable buffer for memory stats
	memStats runtime.MemStats

	// Memory reader for containerized deployments
	memoryReader func() (float64, error)

	mu   sync.RWMutex
	done chan struct{}
}

// NewResourceMonitor creates a new resource monitor.
//
// Panics if:
//
// - sampleInterval <= 0
//
// - memoryThreshold < 0 or memoryThreshold > 100
//
// - cpuThreshold < 0 or cpuThreshold > 100
func NewResourceMonitor(
	sampleInterval time.Duration,
	memoryThreshold, cpuThreshold float64,
	cpuMode CPUUsageMode,
	memoryReader func() (float64, error),
) *ResourceMonitor {
	if sampleInterval <= 0 {
		// sampleInterval must be greater than 0
		panic(fmt.Sprintf("invalid sampleInterval: %v", sampleInterval))
	}
	if memoryThreshold < 0 || memoryThreshold > 100 {
		// memoryThreshold must be between 0 and 100
		panic(fmt.Sprintf("invalid memoryThreshold: %f, must be between 0 and 100", memoryThreshold))
	}
	if cpuThreshold < 0 || cpuThreshold > 100 {
		// cpuThreshold must be between 0 and 100
		panic(fmt.Sprintf("invalid cpuThreshold: %f, must be between 0 and 100", cpuThreshold))
	}

	rm := &ResourceMonitor{
		sampleInterval:  sampleInterval,
		memoryThreshold: memoryThreshold,
		cpuThreshold:    cpuThreshold,
		cpuMode:         cpuMode,
		memoryReader:    memoryReader,
		done:            make(chan struct{}),
	}

	rm.initSampler()

    // start periodically collecting resource statistics
	go rm.monitor()

	return rm
}

// initSampler initializes the appropriate CPU sampler based on mode and platform support
func (rm *ResourceMonitor) initSampler() {
	switch rm.cpuMode {
	case CPUUsageModeMeasured:
		// Try native sampler first, fallback to heuristic
		if sampler, err := sysmonitor.NewProcessSampler(); err == nil {
			rm.sampler = sampler
		} else {
			rm.sampler = sysmonitor.NewGoroutineHeuristicSampler()
			rm.cpuMode = CPUUsageModeHeuristic
		}
	default: // CPUUsageModeHeuristic
		rm.sampler = sysmonitor.NewGoroutineHeuristicSampler()
	}

	rm.stats.Store(rm.collectStats())
}

// GetStats returns the current resource statistics
func (rm *ResourceMonitor) GetStats() ResourceStats {
	stats := rm.stats.Load()
	if stats == nil {
		return ResourceStats{}
	}
	return *stats.(*ResourceStats)
}

// IsResourceConstrained returns true if resources are above thresholds
func (rm *ResourceMonitor) IsResourceConstrained() bool {
	stats := rm.GetStats()
	return stats.MemoryUsedPercent > rm.memoryThreshold ||
		stats.CPUUsagePercent > rm.cpuThreshold
}

// collectStats collects current system resource statistics
func (rm *ResourceMonitor) collectStats() *ResourceStats {
	sysStats, hasSystemStats := rm.tryGetSystemMemory()

	var procStats *runtime.MemStats
	if !hasSystemStats {
		// Reuse existing memStats buffer instead of allocating new one
		runtime.ReadMemStats(&rm.memStats)
		procStats = &rm.memStats
	}

	// Calculate memory usage percentage
	memoryPercent := rm.memoryUsagePercent(hasSystemStats, sysStats, procStats)

	// Get goroutine count
	goroutineCount := runtime.NumGoroutine()

	// Sample CPU usage and validate
	cpuPercent := rm.sampler.Sample(rm.sampleInterval)
	cpuPercent = validatePercent(cpuPercent)

	stats := &ResourceStats{
		MemoryUsedPercent: memoryPercent,
		CPUUsagePercent:   cpuPercent,
		GoroutineCount:    goroutineCount,
		Timestamp:         time.Now(),
	}

	// Validate the complete stats object
	validateResourceStats(stats)

	return stats
}

func (rm *ResourceMonitor) tryGetSystemMemory() (sysmonitor.SystemMemory, bool) {
	stats, err := sysmonitor.GetSystemMemory()
	if err != nil || stats.Total == 0 {
		return sysmonitor.SystemMemory{}, false
	}
	return stats, true
}

func (rm *ResourceMonitor) memoryUsagePercent(
	hasSystemStats bool,
	sysStats sysmonitor.SystemMemory,
	procStats *runtime.MemStats,
) float64 {
	// Use custom memory reader if provided (for containerized deployments)
	if rm.memoryReader != nil {
		if percent, err := rm.memoryReader(); err == nil {
			return clampPercent(percent)
		}
		// Fall back to system memory if custom reader fails
	}

	if hasSystemStats {
		available := sysStats.Available
		if available > sysStats.Total {
			available = sysStats.Total
		}

		// avoid division by zero
		if sysStats.Total == 0 {
			return 0
		}

		used := sysStats.Total - available
		percent := float64(used) / float64(sysStats.Total) * 100

		return clampPercent(percent)
	}

	if procStats == nil || procStats.Sys == 0 {
		return 0
	}

	percent := float64(procStats.Alloc) / float64(procStats.Sys) * 100
	return clampPercent(percent)
}

// validateResourceStats sanitizes ResourceStats to ensure valid values
func validateResourceStats(stats *ResourceStats) {
	if stats == nil {
		panic("ResourceStats cannot be nil")
	}

	// Validate memory percent
	stats.MemoryUsedPercent = validatePercent(stats.MemoryUsedPercent)

	// Validate CPU percent
	stats.CPUUsagePercent = validatePercent(stats.CPUUsagePercent)

	// Validate goroutine count
	if stats.GoroutineCount < 0 {
		stats.GoroutineCount = 0
	}

	// Validate timestamp (should be recent)
	if stats.Timestamp.IsZero() {
		stats.Timestamp = time.Now()
	} else if time.Since(stats.Timestamp) > time.Minute {
		// Stats are too old, refresh timestamp
		stats.Timestamp = time.Now()
	}
}

// monitor periodically collects resource statistics
func (rm *ResourceMonitor) monitor() {
	ticker := time.NewTicker(rm.sampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			newStats := rm.collectStats()
			rm.stats.Store(newStats)
		case <-rm.done:
			return
		}
	}
}

// Close stops the resource monitor
func (rm *ResourceMonitor) Close() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	select {
	case <-rm.done:
		return
	default:
		close(rm.done)
	}
}
