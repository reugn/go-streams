package flow

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/reugn/go-streams"
)

const (
	// minSampleInterval is the minimum allowed sampling interval to prevent
	// excessive CPU overhead from too-frequent system resource polling.
	minSampleInterval = 10 * time.Millisecond
)

// AdaptiveThrottlerConfig configures the adaptive throttler behavior
type AdaptiveThrottlerConfig struct {
	// Resource thresholds (0-100 percentage)
	MaxMemoryPercent float64
	MaxCPUPercent    float64

	// Throughput bounds (elements per second)
	MinThroughput int
	MaxThroughput int

	// How often to sample resources
	SampleInterval time.Duration

	// Buffer configuration
	BufferSize int

	// Adaptation parameters (How aggressively to adapt. 0.1 = slow, 0.5 = fast).
	//
	// Allowed values: 0.0 to 1.0
	AdaptationFactor float64

	// Rate transition smoothing.
	//
	// If true, the throughput rate will be smoothed over time to avoid abrupt changes.
	SmoothTransitions bool

	// CPU usage sampling mode.
	//
	// CPUUsageModeHeuristic: Estimates CPU usage using a simple heuristic (goroutine count),
	// suitable for platforms where accurate process CPU measurement is not supported.
	//
	// CPUUsageModeMeasured: Attempts to measure actual process CPU usage natively
	// (when supported), providing more accurate CPU usage readings.
	CPUUsageMode CPUUsageMode

	// Hysteresis buffer to prevent rapid state changes (percentage points).
	// Requires this much additional headroom before increasing rate.
	// Default: 5.0
	HysteresisBuffer float64

	// Maximum rate change factor per adaptation cycle (0.0-1.0).
	// Limits how much the rate can change in a single step to prevent instability.
	// Default: 0.3 (max 30% change per cycle)
	MaxRateChangeFactor float64

	// MemoryReader provides memory usage percentage for containerized deployments.
	// If nil, system memory will be read via mem.VirtualMemory().
	// Must return memory used percentage (0-100).
	MemoryReader func() (float64, error)
}

// DefaultAdaptiveThrottlerConfig returns sensible defaults for most use cases
func DefaultAdaptiveThrottlerConfig() AdaptiveThrottlerConfig {
	return AdaptiveThrottlerConfig{
		MaxMemoryPercent:    80.0,                   // Conservative memory threshold
		MaxCPUPercent:       70.0,                   // Conservative CPU threshold
		MinThroughput:       10,                     // Reasonable minimum throughput
		MaxThroughput:       500,                    // More conservative maximum
		SampleInterval:      200 * time.Millisecond, // Less frequent sampling
		BufferSize:          500,                    // Match max throughput for 1 second buffer at max rate
		AdaptationFactor:    0.15,                   // Slightly more conservative adaptation
		SmoothTransitions:   true,                   // Keep smooth transitions enabled by default
		CPUUsageMode:        CPUUsageModeMeasured,   // Use actual process CPU usage natively
		HysteresisBuffer:    5.0,                    // Prevent oscillations around threshold
		MaxRateChangeFactor: 0.3,                    // More conservative rate changes
	}
}

// ResourceMonitor defines the interface for resource monitoring
type resourceMonitor interface {
	// GetStats returns the current resource statistics
	GetStats() ResourceStats
	// IsResourceConstrained returns true if resources are above thresholds
	IsResourceConstrained() bool
	// Close closes the resource monitor
	Close()
}

// AdaptiveThrottler implements a feedback control system that:
// - Monitors CPU and memory usage at regular intervals
//
// - Reduces throughput when resources exceed thresholds (with severity-based scaling)
//
// - Gradually increases throughput when resources are available (with hysteresis)
//
// - Applies smoothing to prevent abrupt rate changes
//
// - Enforces minimum and maximum throughput bounds
type AdaptiveThrottler struct {
	config  AdaptiveThrottlerConfig
	monitor resourceMonitor

	// Current rate (elements per second)
	currentRate atomic.Int64

	// Rate control
	period      time.Duration // Calculated from currentRate
	maxElements atomic.Int64  // Elements per period
	counter     atomic.Int64

	// Channels
	in          chan any
	out         chan any
	quotaSignal chan struct{}
	done        chan struct{}

	// Rate adaptation
	adaptMu        sync.Mutex
	lastAdaptation time.Time

	stopOnce sync.Once
}

var _ streams.Flow = (*AdaptiveThrottler)(nil)

// NewAdaptiveThrottler creates a new adaptive throttler
// If config is nil, default configuration will be used.
func NewAdaptiveThrottler(config *AdaptiveThrottlerConfig) (*AdaptiveThrottler, error) {
	if config == nil {
		defaultConfig := DefaultAdaptiveThrottlerConfig()
		config = &defaultConfig
	}

	// Validate configuration
	if config.MaxMemoryPercent <= 0 || config.MaxMemoryPercent > 100 {
		return nil, fmt.Errorf("invalid MaxMemoryPercent: %f", config.MaxMemoryPercent)
	}
	if config.MinThroughput < 1 || config.MaxThroughput < config.MinThroughput {
		return nil, fmt.Errorf("invalid throughput bounds: min=%d, max=%d", config.MinThroughput, config.MaxThroughput)
	}
	if config.AdaptationFactor <= 0 || config.AdaptationFactor >= 1 {
		return nil, fmt.Errorf("invalid AdaptationFactor: %f, must be in (0, 1)", config.AdaptationFactor)
	}
	if config.MaxCPUPercent <= 0 || config.MaxCPUPercent > 100 {
		return nil, fmt.Errorf("invalid MaxCPUPercent: %f", config.MaxCPUPercent)
	}
	if config.BufferSize < 1 {
		return nil, fmt.Errorf("invalid BufferSize: %d", config.BufferSize)
	}
	if config.SampleInterval < minSampleInterval {
		return nil, fmt.Errorf("invalid SampleInterval: %v; must be at least %v to prevent high CPU overhead", config.SampleInterval, minSampleInterval)
	}
	if config.HysteresisBuffer < 0 {
		return nil, fmt.Errorf("invalid HysteresisBuffer: %f", config.HysteresisBuffer)
	}
	if config.MaxRateChangeFactor <= 0 || config.MaxRateChangeFactor > 1 {
		return nil, fmt.Errorf("invalid MaxRateChangeFactor: %f, must be in (0, 1]", config.MaxRateChangeFactor)
	}

	// Initialize with max throughput
	initialRate := int64(config.MaxThroughput)

	at := &AdaptiveThrottler{
		config: *config,
		monitor: NewResourceMonitor(
			config.SampleInterval,
			config.MaxMemoryPercent,
			config.MaxCPUPercent,
			config.CPUUsageMode,
			config.MemoryReader,
		),
		period:      time.Second, // 1 second period
		in:          make(chan any),
		out:         make(chan any, config.BufferSize),
		quotaSignal: make(chan struct{}, 1),
		done:        make(chan struct{}),
	}

	at.currentRate.Store(initialRate)
	at.maxElements.Store(initialRate)
	at.lastAdaptation = time.Now()

	// Start rate adaptation goroutine
	go at.adaptRateLoop()

	// Start quota reset goroutine
	go at.resetQuotaCounterLoop()

	// Start buffering goroutine
	go at.buffer()

	return at, nil
}

// adaptRateLoop periodically adapts the throughput rate based on resource availability
func (at *AdaptiveThrottler) adaptRateLoop() {
	ticker := time.NewTicker(at.config.SampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			at.adaptRate()
		case <-at.done:
			return
		}
	}
}

// adaptRate adjusts the throughput rate based on current resource usage
func (at *AdaptiveThrottler) adaptRate() {
	at.adaptMu.Lock()
	defer at.adaptMu.Unlock()

	stats := at.monitor.GetStats()
	// constrained indicates if memory or CPU usage exceeds configured thresholds
	constrained := stats.MemoryUsedPercent > at.config.MaxMemoryPercent ||
		stats.CPUUsagePercent > at.config.MaxCPUPercent

	currentRate := float64(at.currentRate.Load())
	targetRate := currentRate

	if constrained {
		// Reduce rate when resources are constrained
		// Calculate how far over the limits we are (as a percentage)
		memoryOverage := math.Max(0, stats.MemoryUsedPercent-at.config.MaxMemoryPercent)
		cpuOverage := math.Max(0, stats.CPUUsagePercent-at.config.MaxCPUPercent)
		maxOverage := math.Max(memoryOverage, cpuOverage)

		// Scale reduction factor based on overage severity (0-100%)
		severityFactor := maxOverage / 50.0 // 50% overage = full severity
		severityFactor = math.Min(severityFactor, 1.0)

		// Calculate reduction: base factor + severity bonus
		reductionFactor := at.config.AdaptationFactor * (1.0 + severityFactor)
		maxReduction := currentRate * at.config.MaxRateChangeFactor
		reduction := math.Min(currentRate*reductionFactor, maxReduction)

		targetRate = currentRate - reduction

		// Avoid negative rates
		if targetRate < 0 {
			targetRate = 0
		}
	} else {
		// Increase rate when resources are available, with hysteresis
		memoryHeadroom := at.config.MaxMemoryPercent - stats.MemoryUsedPercent
		cpuHeadroom := at.config.MaxCPUPercent - stats.CPUUsagePercent
		minHeadroom := math.Min(memoryHeadroom, cpuHeadroom)

		// Apply hysteresis buffer - only increase if we have significant headroom
		effectiveHeadroom := minHeadroom - at.config.HysteresisBuffer
		if effectiveHeadroom > 0 {
			// Use square root scaling for stable, diminishing returns
			headroomRatio := math.Min(effectiveHeadroom/30.0, 1.0) // Cap at 30% headroom for scaling
			increaseFactor := at.config.AdaptationFactor * math.Sqrt(headroomRatio)
			maxIncrease := currentRate * at.config.MaxRateChangeFactor

			increase := math.Min(currentRate*increaseFactor, maxIncrease)
			targetRate = currentRate + increase
		}
	}

	if at.config.SmoothTransitions {
		// Smooth transitions: gradually approach target rate to avoid abrupt changes
		// Use a fixed smoothing factor of 0.3 (30% of remaining distance per cycle)
		const smoothingFactor = 0.3
		diff := targetRate - currentRate
		targetRate = currentRate + diff*smoothingFactor
	}

	// Enforce bounds
	targetRate = math.Max(float64(at.config.MinThroughput), targetRate)
	targetRate = math.Min(float64(at.config.MaxThroughput), targetRate)

	// Convert to integer and update atomically
	newRateInt := int64(math.Round(targetRate))

	// Only update if rate actually changed
	if newRateInt != at.currentRate.Load() {
		at.currentRate.Store(newRateInt)
		at.maxElements.Store(newRateInt)
		at.counter.Store(0) // Reset quota counter to apply new rate immediately
		// Wake any blocked emitters
		// so the new quota takes effect without waiting for the next period tick.
		at.notifyQuotaReset()
		at.lastAdaptation = time.Now()
	}
}

// resetQuotaCounterLoop resets the quota counter every period
func (at *AdaptiveThrottler) resetQuotaCounterLoop() {
	ticker := time.NewTicker(at.period)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			at.counter.Store(0)
			at.notifyQuotaReset()
		case <-at.done:
			return
		}
	}
}

// notifyQuotaReset notifies downstream processor of quota reset
func (at *AdaptiveThrottler) notifyQuotaReset() {
	select {
	case at.quotaSignal <- struct{}{}:
	default:
	}
}

// quotaExceeded checks if quota has been exceeded
func (at *AdaptiveThrottler) quotaExceeded() bool {
	return at.counter.Load() >= at.maxElements.Load()
}

// buffer buffers incoming elements and sends them to output channel
func (at *AdaptiveThrottler) buffer() {
	defer close(at.out)

	for {
		select {
		case element, ok := <-at.in:
			if !ok {
				return
			}
			at.emit(element)
		case <-at.done:
			return
		}
	}
}

func (at *AdaptiveThrottler) emit(element any) {
	for {
		if !at.quotaExceeded() {
			at.counter.Add(1)
			at.out <- element
			return
		}

		select {
		case <-at.quotaSignal:
		case <-at.done:
			// Shutting down: try to flush pending data, but drop if blocked to avoid deadlock
			select {
			case at.out <- element:
				// Successfully flushed
			default:
				// Channel is full or no readers - drop element to ensure clean shutdown
			}
			return
		}
	}
}

// Via asynchronously streams data to the given Flow and returns it
func (at *AdaptiveThrottler) Via(flow streams.Flow) streams.Flow {
	go at.streamPortioned(flow)
	return flow
}

// To streams data to the given Sink and blocks until completion
func (at *AdaptiveThrottler) To(sink streams.Sink) {
	at.streamPortioned(sink)
	sink.AwaitCompletion()
}

// Out returns the output channel
func (at *AdaptiveThrottler) Out() <-chan any {
	return at.out
}

// In returns the input channel
func (at *AdaptiveThrottler) In() chan<- any {
	return at.in
}

// streamPortioned streams elements enforcing the adaptive quota
func (at *AdaptiveThrottler) streamPortioned(inlet streams.Inlet) {
	defer close(inlet.In())

	for element := range at.out {
		inlet.In() <- element
	}
	at.stop()
}

// GetCurrentRate returns the current throughput rate (elements per second)
func (at *AdaptiveThrottler) GetCurrentRate() int64 {
	return at.currentRate.Load()
}

// GetResourceStats returns current resource statistics
func (at *AdaptiveThrottler) GetResourceStats() ResourceStats {
	return at.monitor.GetStats()
}

// Close stops the adaptive throttler and cleans up resources
func (at *AdaptiveThrottler) Close() {
	// Drain any pending quota signals to prevent goroutine leaks
	select {
	case <-at.quotaSignal:
	default:
	}

	at.stop()
}

func (at *AdaptiveThrottler) stop() {
	at.stopOnce.Do(func() {
		close(at.done)
		at.monitor.Close()
	})
}
