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
	// smoothingFactor is the factor by which the current rate is adjusted to approach the target rate.
	smoothingFactor = 0.3 // 30% of remaining distance per cycle
)

// AdaptiveThrottlerConfig configures the adaptive throttler behavior
type AdaptiveThrottlerConfig struct {
	// Resource monitoring configuration
	//
	// These settings control how the throttler monitors system resources
	// and determines when to throttle throughput.

	// MaxMemoryPercent is the maximum memory usage threshold (0-100 percentage).
	// When memory usage exceeds this threshold, throughput will be reduced.
	MaxMemoryPercent float64

	// MaxCPUPercent is the maximum CPU usage threshold (0-100 percentage).
	// When CPU usage exceeds this threshold, throughput will be reduced.
	MaxCPUPercent float64

	// SampleInterval is how often to sample system resources.
	// More frequent sampling provides faster response but increases CPU overhead.
	SampleInterval time.Duration

	// CPUUsageMode controls how CPU usage is measured.
	//
	// CPUUsageModeHeuristic: Estimates CPU usage using a simple heuristic (goroutine count),
	// suitable for platforms where accurate process CPU measurement is not supported.
	//
	// CPUUsageModeMeasured: Attempts to measure actual process CPU usage natively
	// (when supported), providing more accurate CPU usage readings.
	CPUUsageMode CPUUsageMode

	// MemoryReader is a user-provided custom function that returns memory usage percentage.
	// This can be particularly useful for containerized deployments or other environments
	// where standard system memory readings may not accurately reflect container-specific
	// usage.
	// If nil, system memory will be read via mem.VirtualMemory().
	// Must return memory used percentage (0-100).
	MemoryReader func() (float64, error)

	// Throughput bounds (in elements per second)
	//
	// These settings define the minimum and maximum throughput rates.

	// MinThroughput is the minimum throughput in elements per second.
	// The throttler will never reduce throughput below this value.
	MinThroughput int

	// MaxThroughput is the maximum throughput in elements per second.
	// The throttler will never increase throughput above this value.
	MaxThroughput int

	// Buffer configuration
	//
	// These settings control the internal buffering of elements.

	// BufferSize is the initial buffer size in number of elements.
	// This buffer holds incoming elements when throughput is throttled.
	BufferSize int

	// MaxBufferSize is the maximum buffer size in number of elements.
	// This prevents unbounded memory allocation during sustained throttling.
	MaxBufferSize int

	// Adaptation behavior
	//
	// These settings control how aggressively and smoothly the throttler
	// adapts to changing resource conditions.

	// AdaptationFactor controls how aggressively the throttler adapts (0.0-1.0).
	// Lower values (e.g., 0.1) result in slower, more conservative adaptation.
	// Higher values (e.g., 0.5) result in faster, more aggressive adaptation.
	AdaptationFactor float64

	// SmoothTransitions enables rate transition smoothing.
	// If true, the throughput rate will be smoothed over time to avoid abrupt changes.
	// This helps prevent oscillations and provides more stable behavior.
	SmoothTransitions bool

	// HysteresisBuffer prevents rapid state changes (in percentage points).
	// Requires this much additional headroom before increasing rate.
	// This prevents oscillations around resource thresholds.
	HysteresisBuffer float64

	// MaxRateChangeFactor limits the maximum rate change per adaptation cycle (0.0-1.0).
	// Limits how much the rate can change in a single step to prevent instability.
	MaxRateChangeFactor float64
}

// DefaultAdaptiveThrottlerConfig returns sensible defaults for most use cases.
//
// Default configuration parameters:
//
// Resource Monitoring:
//   - MaxMemoryPercent: 80.0% - Conservative memory threshold to prevent OOM
//   - MaxCPUPercent: 70.0% - Conservative CPU threshold to maintain responsiveness
//   - SampleInterval: 200ms - Balanced sampling frequency to minimize overhead
//   - CPUUsageMode: CPUUsageModeMeasured - Uses native process CPU measurement
//   - MemoryReader: nil - Uses system memory via mem.VirtualMemory()
//
// Throughput Bounds:
//   - MinThroughput: 10 elements/second - Ensures minimum processing rate
//   - MaxThroughput: 500 elements/second - Conservative maximum for stability
//
// Buffer Configuration:
//   - BufferSize: 500 elements - Matches max throughput for 1 second buffer at max rate
//   - MaxBufferSize: 10,000 elements - Prevents unbounded memory allocation
//
// Adaptation Behavior:
//   - AdaptationFactor: 0.15 - Conservative adaptation speed (15% adjustment per cycle)
//   - SmoothTransitions: true - Enables rate smoothing to avoid abrupt changes
//   - HysteresisBuffer: 5.0% - Prevents oscillations around resource thresholds
//   - MaxRateChangeFactor: 0.3 - Limits rate changes to 30% per cycle for stability
func DefaultAdaptiveThrottlerConfig() *AdaptiveThrottlerConfig {
	return &AdaptiveThrottlerConfig{
		MaxMemoryPercent:    80.0,
		MaxCPUPercent:       70.0,
		MinThroughput:       10,
		MaxThroughput:       500,
		SampleInterval:      200 * time.Millisecond,
		BufferSize:          500,
		MaxBufferSize:       10000,
		AdaptationFactor:    0.15,
		SmoothTransitions:   true,
		CPUUsageMode:        CPUUsageModeMeasured,
		HysteresisBuffer:    5.0,
		MaxRateChangeFactor: 0.3,
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

	// Current throughput rate in elements per second
	currentRate atomic.Int64

	// Rate control: period-based quota enforcement
	period      time.Duration // Time period for quota calculation (typically 1 second)
	maxElements atomic.Int64  // Maximum elements allowed per period
	counter     atomic.Int64  // Current element count in the period

	// Communication channels
	in          chan any      // Input channel for incoming elements
	out         chan any      // Output channel for throttled elements
	quotaSignal chan struct{} // Signal channel to notify when quota resets
	done        chan struct{} // Shutdown signal channel

	// Rate adaptation tracking
	lastAdaptation time.Time // Timestamp of last rate adaptation

	stopOnce sync.Once // Ensures cleanup happens only once
}

var _ streams.Flow = (*AdaptiveThrottler)(nil)

// validate validates the adaptive throttler configuration
func (config *AdaptiveThrottlerConfig) validate() error {
	if config.MaxMemoryPercent <= 0 || config.MaxMemoryPercent > 100 {
		return fmt.Errorf("invalid MaxMemoryPercent: %f", config.MaxMemoryPercent)
	}
	if config.MinThroughput < 1 || config.MaxThroughput < config.MinThroughput {
		return fmt.Errorf("invalid throughput bounds: min=%d, max=%d", config.MinThroughput, config.MaxThroughput)
	}
	if config.AdaptationFactor <= 0 || config.AdaptationFactor >= 1 {
		return fmt.Errorf("invalid AdaptationFactor: %f, must be in (0, 1)", config.AdaptationFactor)
	}
	if config.MaxCPUPercent <= 0 || config.MaxCPUPercent > 100 {
		return fmt.Errorf("invalid MaxCPUPercent: %f", config.MaxCPUPercent)
	}
	if config.BufferSize < 1 {
		return fmt.Errorf("invalid BufferSize: %d", config.BufferSize)
	}
	if config.MaxBufferSize < 1 {
		return fmt.Errorf("invalid MaxBufferSize: %d", config.MaxBufferSize)
	}
	if config.BufferSize > config.MaxBufferSize {
		return fmt.Errorf("BufferSize %d exceeds MaxBufferSize %d", config.BufferSize, config.MaxBufferSize)
	}
	if config.SampleInterval < minSampleInterval {
		return fmt.Errorf(
			"invalid SampleInterval: %v; must be at least %v to prevent high CPU overhead",
			config.SampleInterval, minSampleInterval)
	}
	if config.HysteresisBuffer < 0 {
		return fmt.Errorf("invalid HysteresisBuffer: %f", config.HysteresisBuffer)
	}
	if config.MaxRateChangeFactor <= 0 || config.MaxRateChangeFactor > 1 {
		return fmt.Errorf("invalid MaxRateChangeFactor: %f, must be in (0, 1]", config.MaxRateChangeFactor)
	}
	return nil
}

// NewAdaptiveThrottler creates a new adaptive throttler
// If config is nil, default configuration will be used.
func NewAdaptiveThrottler(config *AdaptiveThrottlerConfig) (*AdaptiveThrottler, error) {
	if config == nil {
		config = DefaultAdaptiveThrottlerConfig()
	}

	if err := config.validate(); err != nil {
		return nil, err
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

// In returns the input channel
func (at *AdaptiveThrottler) In() chan<- any {
	return at.in
}

// Out returns the output channel
func (at *AdaptiveThrottler) Out() <-chan any {
	return at.out
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
	stats := at.monitor.GetStats()
	currentRate := float64(at.currentRate.Load())

	// Calculate target rate based on resource constraints
	targetRate := at.calculateTargetRate(currentRate, stats)

	// Apply smoothing if enabled
	if at.config.SmoothTransitions {
		targetRate = at.applySmoothing(currentRate, targetRate)
	}

	// Enforce bounds
	targetRate = math.Max(float64(at.config.MinThroughput), targetRate)
	targetRate = math.Min(float64(at.config.MaxThroughput), targetRate)

	// Commit the new rate if it changed
	at.commitRateChange(targetRate)
}

// calculateTargetRate determines the target rate based on resource constraints
func (at *AdaptiveThrottler) calculateTargetRate(currentRate float64, stats ResourceStats) float64 {
	// Constrained is true if resources are above thresholds
	constrained := stats.MemoryUsedPercent > at.config.MaxMemoryPercent ||
		stats.CPUUsagePercent > at.config.MaxCPUPercent

	if constrained {
		return at.calculateReduction(currentRate, stats)
	}
	return at.calculateIncrease(currentRate, stats)
}

// calculateReduction computes the rate reduction when resources are constrained
func (at *AdaptiveThrottler) calculateReduction(currentRate float64, stats ResourceStats) float64 {
	// Calculate how far over the limits we are (as a percentage)
	memoryOverage := math.Max(0, stats.MemoryUsedPercent-at.config.MaxMemoryPercent)
	cpuOverage := math.Max(0, stats.CPUUsagePercent-at.config.MaxCPUPercent)
	maxOverage := math.Max(memoryOverage, cpuOverage)

	// Scale reduction factor based on overage severity (0-100%)
	severityFactor := math.Min(maxOverage/50.0, 1.0) // 50% overage = full severity

	// Calculate reduction: base factor + severity bonus
	reductionFactor := at.config.AdaptationFactor * (1.0 + severityFactor)
	maxReduction := currentRate * at.config.MaxRateChangeFactor
	reduction := math.Min(currentRate*reductionFactor, maxReduction)

	targetRate := currentRate - reduction

	// Avoid negative rates
	return math.Max(0, targetRate)
}

// calculateIncrease computes the rate increase when resources are available
func (at *AdaptiveThrottler) calculateIncrease(currentRate float64, stats ResourceStats) float64 {
	memoryHeadroom := at.config.MaxMemoryPercent - stats.MemoryUsedPercent
	cpuHeadroom := at.config.MaxCPUPercent - stats.CPUUsagePercent
	minHeadroom := math.Min(memoryHeadroom, cpuHeadroom)

	// Apply hysteresis buffer - only increase if we have significant headroom
	effectiveHeadroom := minHeadroom - at.config.HysteresisBuffer
	if effectiveHeadroom <= 0 {
		return currentRate // No increase if insufficient headroom
	}

	// Use square root scaling for stable, diminishing returns
	headroomRatio := math.Min(effectiveHeadroom/30.0, 1.0) // Cap at 30% headroom for scaling
	increaseFactor := at.config.AdaptationFactor * math.Sqrt(headroomRatio)
	maxIncrease := currentRate * at.config.MaxRateChangeFactor

	increase := math.Min(currentRate*increaseFactor, maxIncrease)
	return currentRate + increase
}

// applySmoothing gradually approaches the target rate to avoid abrupt changes
func (at *AdaptiveThrottler) applySmoothing(currentRate, targetRate float64) float64 {
	diff := targetRate - currentRate
	return currentRate + diff*smoothingFactor
}

// commitRateChange atomically updates the rate if it has changed
func (at *AdaptiveThrottler) commitRateChange(targetRate float64) {
	newRateInt := int64(math.Round(targetRate))
	currentRateInt := at.currentRate.Load()

	if newRateInt != currentRateInt {
		at.currentRate.Store(newRateInt)
		at.maxElements.Store(newRateInt)
		at.counter.Store(0) // Reset quota counter to apply new rate immediately
		// Wake any blocked emitters so the new quota takes effect
		// without waiting for the next period tick.
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

// emit emits an element to the output channel, blocking if quota is exceeded
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

// streamPortioned streams elements enforcing the adaptive quota
func (at *AdaptiveThrottler) streamPortioned(inlet streams.Inlet) {
	defer close(inlet.In())

	for element := range at.out {
		inlet.In() <- element
	}
	at.stop()
}

// stop stops the adaptive throttler and cleans up resources
func (at *AdaptiveThrottler) stop() {
	at.stopOnce.Do(func() {
		close(at.done)
		at.monitor.Close()
	})
}
