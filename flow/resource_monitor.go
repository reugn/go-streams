package flow

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// CPUHeuristicBaselineCPU provides minimum CPU usage estimate for any number of goroutines
	CPUHeuristicBaselineCPU = 10.0
	// CPUHeuristicLinearScaleFactor determines CPU increase per goroutine for low counts (1-10)
	CPUHeuristicLinearScaleFactor = 1.0
	// CPUHeuristicLogScaleFactor determines logarithmic CPU scaling for higher goroutine counts
	CPUHeuristicLogScaleFactor = 8.0
	// CPUHeuristicMaxGoroutinesForLinear switches from linear to logarithmic scaling
	CPUHeuristicMaxGoroutinesForLinear = 10
	// CPUHeuristicMaxCPU caps the CPU estimate to leave room for system processes
	CPUHeuristicMaxCPU = 95.0
)

// CPUUsageMode defines the strategy for sampling CPU usage
type CPUUsageMode int

const (
	// CPUUsageModeHeuristic uses goroutine count as a simple CPU usage proxy
	CPUUsageModeHeuristic CPUUsageMode = iota
	// CPUUsageModeRusage samples actual CPU time using syscall.Getrusage
	CPUUsageModeRusage
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

// goroutineHeuristicSampler uses goroutine count as a CPU usage proxy
type goroutineHeuristicSampler struct{}

func (s *goroutineHeuristicSampler) Sample(deltaTime time.Duration) float64 {
	// Improved heuristic: uses logarithmic scaling for more realistic CPU estimation
	// Base level: 1-10 goroutines = baseline CPU usage (10-20%)
	// Scaling: logarithmic growth to avoid overestimation at high goroutine counts
	goroutineCount := float64(runtime.NumGoroutine())

	// Baseline CPU usage for minimal goroutines
	if goroutineCount <= CPUHeuristicMaxGoroutinesForLinear {
		return CPUHeuristicBaselineCPU + goroutineCount*CPUHeuristicLinearScaleFactor
	}

	// Logarithmic scaling: ln(goroutines) * scaling factor
	// At ~100 goroutines: ~50% CPU
	// At ~1000 goroutines: ~70% CPU
	// At ~10000 goroutines: ~85% CPU
	// Caps at 95% to leave room for system processes
	logScaling := math.Log(goroutineCount) * CPUHeuristicLogScaleFactor
	estimatedCPU := CPUHeuristicBaselineCPU + logScaling

	// Cap at maximum to be conservative
	if estimatedCPU > CPUHeuristicMaxCPU {
		return CPUHeuristicMaxCPU
	}
	return estimatedCPU
}

func (s *goroutineHeuristicSampler) Reset() {
	// No state to reset for heuristic sampler
}

func (s *goroutineHeuristicSampler) IsInitialized() bool {
	// Heuristic sampler is always "initialized" as it doesn't need state
	return true
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
	case CPUUsageModeRusage:
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

func (rm *ResourceMonitor) memoryUsagePercent(hasSystemStats bool, sysStats systemMemory, procStats *runtime.MemStats) float64 {
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
	if math.IsNaN(stats.MemoryUsedPercent) || math.IsInf(stats.MemoryUsedPercent, 0) {
		stats.MemoryUsedPercent = 0
	} else if stats.MemoryUsedPercent < 0 {
		stats.MemoryUsedPercent = 0
	} else if stats.MemoryUsedPercent > 100 {
		stats.MemoryUsedPercent = 100
	}

	// Validate CPU percent
	if math.IsNaN(stats.CPUUsagePercent) || math.IsInf(stats.CPUUsagePercent, 0) {
		stats.CPUUsagePercent = 0
	} else if stats.CPUUsagePercent < 0 {
		stats.CPUUsagePercent = 0
	} else if stats.CPUUsagePercent > 100 {
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
