package flow

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// CPUUsageMode defines the strategy for sampling CPU usage
type CPUUsageMode int

const (
	// CPUUsageModeHeuristic uses goroutine count as a simple CPU usage proxy
	CPUUsageModeHeuristic CPUUsageMode = iota
	// CPUUsageModeReal attempts to measure actual process CPU usage via gopsutil
	CPUUsageModeReal
)

// ResourceStats represents current system resource statistics
type ResourceStats struct {
	MemoryUsedPercent float64
	CPUUsagePercent   float64
	GoroutineCount    int
	Timestamp         time.Time
}

// cpuUsageSampler defines the interface for CPU sampling strategies
type cpuUsageSampler interface {
	// Sample returns CPU usage percentage for the given time delta
	Sample(deltaTime time.Duration) float64
	// Reset prepares the sampler for a new sampling session
	Reset()
	// IsInitialized returns true if the sampler has been initialized with at least one sample
	IsInitialized() bool
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
	sampler cpuUsageSampler

	memStats runtime.MemStats // Reusable buffer for memory stats

	// Lifecycle
	mu   sync.RWMutex
	done chan struct{}
}

// NewResourceMonitor creates a new resource monitor
func NewResourceMonitor(
	sampleInterval time.Duration,
	memoryThreshold, cpuThreshold float64,
	cpuMode CPUUsageMode,
) *ResourceMonitor {
	if sampleInterval <= 0 {
		panic(fmt.Sprintf("invalid sampleInterval: %v", sampleInterval))
	}
	if memoryThreshold < 0 || memoryThreshold > 100 {
		panic(fmt.Sprintf("invalid memoryThreshold: %f, must be between 0 and 100", memoryThreshold))
	}
	if cpuThreshold < 0 || cpuThreshold > 100 {
		panic(fmt.Sprintf("invalid cpuThreshold: %f, must be between 0 and 100", cpuThreshold))
	}

	rm := &ResourceMonitor{
		sampleInterval:  sampleInterval,
		memoryThreshold: memoryThreshold,
		cpuThreshold:    cpuThreshold,
		cpuMode:         cpuMode,
		done:            make(chan struct{}),
	}

	// Initialize CPU sampler
	rm.initSampler()

	// Initialize with current stats
	rm.stats.Store(rm.collectStats())

	// Start monitoring goroutine
	go rm.monitor()

	return rm
}

// initSampler initializes the appropriate CPU sampler based on mode and platform support
func (rm *ResourceMonitor) initSampler() {
	switch rm.cpuMode {
	case CPUUsageModeReal:
		// Try gopsutil first, fallback to heuristic
		if sampler, err := newGopsutilProcessSampler(); err == nil {
			rm.sampler = sampler
		} else {
			rm.sampler = &goroutineHeuristicSampler{}
			rm.cpuMode = CPUUsageModeHeuristic
		}
	default: // CPUUsageModeHeuristic
		rm.sampler = &goroutineHeuristicSampler{}
	}
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
	if math.IsNaN(cpuPercent) || math.IsInf(cpuPercent, 0) || cpuPercent < 0 {
		cpuPercent = 0
	} else if cpuPercent > 100 {
		cpuPercent = 100
	}

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

func (rm *ResourceMonitor) tryGetSystemMemory() (systemMemory, bool) {
	stats, err := getSystemMemory()
	if err != nil || stats.Total == 0 {
		return systemMemory{}, false
	}
	return stats, true
}

func (rm *ResourceMonitor) memoryUsagePercent(
	hasSystemStats bool,
	sysStats systemMemory,
	procStats *runtime.MemStats,
) float64 {
	if hasSystemStats {
		available := sysStats.Available
		if available > sysStats.Total {
			available = sysStats.Total
		}

		// Defensive programming: avoid division by zero
		if sysStats.Total == 0 {
			return 0
		}

		used := sysStats.Total - available
		percent := float64(used) / float64(sysStats.Total) * 100

		if percent < 0 {
			return 0
		}
		if percent > 100 {
			return 100
		}
		return percent
	}

	if procStats == nil || procStats.Sys == 0 {
		return 0
	}

	percent := float64(procStats.Alloc) / float64(procStats.Sys) * 100
	if percent < 0 {
		return 0
	}
	if percent > 100 {
		return 100
	}
	return percent
}

type systemMemory struct {
	Total     uint64
	Available uint64
}

// validateResourceStats sanitizes ResourceStats to ensure valid values
func validateResourceStats(stats *ResourceStats) {
	if stats == nil {
		panic("ResourceStats cannot be nil")
	}

	// Validate memory percent
	switch {
	case math.IsNaN(stats.MemoryUsedPercent) || math.IsInf(stats.MemoryUsedPercent, 0):
		stats.MemoryUsedPercent = 0
	case stats.MemoryUsedPercent < 0:
		stats.MemoryUsedPercent = 0
	case stats.MemoryUsedPercent > 100:
		stats.MemoryUsedPercent = 100
	}

	// Validate CPU percent
	switch {
	case math.IsNaN(stats.CPUUsagePercent) || math.IsInf(stats.CPUUsagePercent, 0):
		stats.CPUUsagePercent = 0
	case stats.CPUUsagePercent < 0:
		stats.CPUUsagePercent = 0
	case stats.CPUUsagePercent > 100:
		stats.CPUUsagePercent = 100
	}

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
		// Already closed
		return
	default:
		close(rm.done)
	}
}
