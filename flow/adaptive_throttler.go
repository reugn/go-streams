package flow

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/reugn/go-streams"
)

const (
	// minSampleInterval is the minimum allowed sampling interval
	minSampleInterval = 50 * time.Millisecond
	// smoothingFactor factor by which the current rate is adjusted when smoothing is enabled
	smoothingFactor = 0.3
)

// AdaptiveThrottlerConfig configures the adaptive throttler behavior
type AdaptiveThrottlerConfig struct {
	// MaxMemoryPercent is the maximum allowed memory usage percentage (0-100).
	// When memory usage exceeds this threshold, the throttler reduces processing rate.
	// Default config value: 85.0
	MaxMemoryPercent float64

	// MaxCPUPercent is the maximum allowed CPU usage percentage (0-100).
	// When CPU usage exceeds this threshold, the throttler reduces processing rate.
	// Default config value: 80.0
	MaxCPUPercent float64

	// Sampling configuration
	// SampleInterval is the frequency of resource monitoring and rate adjustments.
	// Minimum: 50ms. Default config value: 100ms
	SampleInterval time.Duration

	// CPUUsageMode specifies the CPU monitoring strategy.
	// Options: CPUUsageModeHeuristic (goroutine-based) or CPUUsageModeMeasured (system calls).
	// Default config value: CPUUsageModeMeasured
	CPUUsageMode CPUUsageMode

	// MemoryReader is an optional custom function to read memory usage percentage (0-100).
	// If nil, system memory usage is monitored automatically.
	// Function signature: func() (float64, error) - returns percentage and any error.
	MemoryReader func() (float64, error)

	// Rate Limits
	// InitialRate is the starting processing rate in items per second.
	// Must be between MinRate and MaxRate. Default config value: 1000
	InitialRate int

	// MinRate is the minimum allowed processing rate in items per second.
	// Must be > 0. Default config value: 10
	MinRate int

	// MaxRate is the maximum allowed processing rate in items per second.
	// Must be > MinRate. Default config value: 10000
	MaxRate int

	// Adjustment factors
	// BackoffFactor is the multiplier applied to reduce rate when resource constraints are exceeded.
	// Range: 0.0-1.0 (e.g., 0.7 means rate is reduced to 70% when constrained).
	// Default config value: 0.7
	BackoffFactor float64

	// RecoveryCPUThreshold is the CPU usage percentage below which rate recovery begins.
	// Must be < MaxCPUPercent. If zero, defaults to MaxCPUPercent-10 or 90% of MaxCPUPercent.
	// Range: 0.0-MaxCPUPercent
	RecoveryCPUThreshold float64

	// RecoveryMemoryThreshold is the memory usage percentage below which rate recovery begins.
	// Must be < MaxMemoryPercent. If zero, defaults to MaxMemoryPercent-10 or 90% of MaxMemoryPercent.
	// Range: 0.0-MaxMemoryPercent
	RecoveryMemoryThreshold float64

	// RecoveryFactor is the multiplier applied to increase rate during recovery periods.
	// Must be > 1.0 (e.g., 1.3 means rate increases by 30% during recovery).
	// Default config value: 1.3
	RecoveryFactor float64

	// EnableHysteresis prevents rapid rate oscillations by requiring both resource recovery
	// thresholds to be met before increasing rate. When false, rate increases as soon as
	// constraints are removed.
	// Default config value: true
	EnableHysteresis bool
}

// DefaultAdaptiveThrottlerConfig returns safe defaults
func DefaultAdaptiveThrottlerConfig() *AdaptiveThrottlerConfig {
	return &AdaptiveThrottlerConfig{
		MaxMemoryPercent: 85.0,
		MaxCPUPercent:    80.0,
		SampleInterval:   100 * time.Millisecond,
		CPUUsageMode:     CPUUsageModeMeasured,
		InitialRate:      1000,
		MinRate:          10,
		MaxRate:          10000,
		BackoffFactor:    0.7,
		RecoveryFactor:   1.3,
		EnableHysteresis: true,
	}
}

func (c *AdaptiveThrottlerConfig) validate() error {
	if c.SampleInterval < minSampleInterval {
		return fmt.Errorf("sample interval must be at least %v", minSampleInterval)
	}
	if c.MaxMemoryPercent <= 0 || c.MaxMemoryPercent > 100 {
		return fmt.Errorf("MaxMemoryPercent must be between 0 and 100")
	}
	if c.MaxCPUPercent < 0 || c.MaxCPUPercent > 100 {
		return fmt.Errorf("MaxCPUPercent must be between 0 and 100")
	}

	// Set default recovery thresholds if not specified
	if c.RecoveryMemoryThreshold == 0 {
		c.RecoveryMemoryThreshold = c.MaxMemoryPercent - 10
		if c.RecoveryMemoryThreshold < 0 {
			c.RecoveryMemoryThreshold = c.MaxMemoryPercent * 0.9 // 90% of max if max < 10
		}
	}
	if c.RecoveryCPUThreshold == 0 {
		c.RecoveryCPUThreshold = c.MaxCPUPercent - 10
		if c.RecoveryCPUThreshold < 0 {
			c.RecoveryCPUThreshold = c.MaxCPUPercent * 0.9 // 90% of max if max < 10
		}
	}

	// Validate recovery thresholds
	if c.RecoveryMemoryThreshold < 0 || c.RecoveryMemoryThreshold >= c.MaxMemoryPercent {
		return fmt.Errorf("RecoveryMemoryThreshold (%.1f) must be between 0 and MaxMemoryPercent (%.1f)",
			c.RecoveryMemoryThreshold, c.MaxMemoryPercent)
	}
	if c.MaxCPUPercent > 0 && (c.RecoveryCPUThreshold < 0 || c.RecoveryCPUThreshold >= c.MaxCPUPercent) {
		return fmt.Errorf("RecoveryCPUThreshold (%.1f) must be between 0 and MaxCPUPercent (%.1f)",
			c.RecoveryCPUThreshold, c.MaxCPUPercent)
	}

	if c.BackoffFactor >= 1.0 || c.BackoffFactor <= 0 {
		return fmt.Errorf("BackoffFactor must be between 0 and 1")
	}
	if c.MinRate <= 0 {
		return fmt.Errorf("MinRate must be greater than 0")
	}
	if c.MaxRate <= c.MinRate {
		return fmt.Errorf("MaxRate must be greater than MinRate")
	}
	if c.InitialRate < c.MinRate || c.InitialRate > c.MaxRate {
		return fmt.Errorf("InitialRate must be between MinRate (%d) and MaxRate (%d) inclusive", c.MinRate, c.MaxRate)
	}
	if c.RecoveryFactor <= 1.0 {
		return fmt.Errorf("RecoveryFactor must be greater than 1")
	}
	return nil
}

// AdaptiveThrottler implements a feedback control system that monitors resources
// and adjusts throughput dynamically using a token bucket with adjustable rate.
type AdaptiveThrottler struct {
	config          AdaptiveThrottlerConfig
	monitor         resourceMonitor
	currentRateBits uint64

	in     chan any
	out    chan any
	done   chan struct{}
	closed atomic.Bool
}

// NewAdaptiveThrottler creates a new instance
func NewAdaptiveThrottler(config *AdaptiveThrottlerConfig) (*AdaptiveThrottler, error) {
	if config == nil {
		config = DefaultAdaptiveThrottlerConfig()
	}
	if err := config.validate(); err != nil {
		return nil, err
	}

	monitor := globalMonitorRegistry.Acquire(
		config.SampleInterval,
		config.CPUUsageMode,
		config.MemoryReader,
	)

	at := &AdaptiveThrottler{
		config:  *config,
		monitor: monitor,
		in:      make(chan any),
		out:     make(chan any),
		done:    make(chan struct{}),
	}

	// Set initial rate atomically
	at.setRate(float64(config.InitialRate))

	// Start the monitor loop to adjust the rate based on the resource usage
	go at.monitorLoop()

	// Start the pipeline loop to emit items at the correct rate
	go at.pipelineLoop()

	return at, nil
}

// Via asynchronously streams data to the given Flow and returns it.
func (at *AdaptiveThrottler) Via(flow streams.Flow) streams.Flow {
	go at.streamPortioned(flow)
	return flow
}

// To streams data to the given Sink and blocks until the Sink has completed
// processing all data.
func (at *AdaptiveThrottler) To(sink streams.Sink) {
	at.streamPortioned(sink)
	sink.AwaitCompletion()
}

func (at *AdaptiveThrottler) Out() <-chan any {
	return at.out
}

func (at *AdaptiveThrottler) In() chan<- any {
	return at.in
}

// setRate updates the current rate limit atomically
func (at *AdaptiveThrottler) setRate(rate float64) {
	atomic.StoreUint64(&at.currentRateBits, math.Float64bits(rate))
}

// GetCurrentRate returns the current rate limit atomically
func (at *AdaptiveThrottler) GetCurrentRate() float64 {
	return math.Float64frombits(atomic.LoadUint64(&at.currentRateBits))
}

// GetResourceStats returns the latest resource statistics from the monitor
func (at *AdaptiveThrottler) GetResourceStats() ResourceStats {
	return at.monitor.GetStats()
}

// streamPortioned streams elements to the given Inlet from the throttler's output.
// Elements are sent to inlet.In() until at.out is closed.
func (at *AdaptiveThrottler) streamPortioned(inlet streams.Inlet) {
	defer close(inlet.In())
	for element := range at.Out() {
		select {
		case inlet.In() <- element:
		case <-at.done:
			// Throttler was closed, exit early
			return
		}
	}
}

func (at *AdaptiveThrottler) close() {
	if at.closed.CompareAndSwap(false, true) {
		close(at.done)
		at.monitor.Close()
	}
}

// monitorLoop handles periodic resource checks and rate adjustment.
func (at *AdaptiveThrottler) monitorLoop() {
	ticker := time.NewTicker(at.config.SampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-at.done:
			return
		case <-ticker.C:
			at.adjustRate()
		}
	}
}

// Pipeline Loop: Strict Pacer
// Ensures exactly 1/Rate seconds between emissions.
func (at *AdaptiveThrottler) pipelineLoop() {
	defer close(at.out)

	// The earliest allowed time for the next item to be emitted
	nextEmission := time.Now()

	for item := range at.in {
		// Check if the throttler is done
		select {
		case <-at.done:
			return
		default:
		}

		// Get the current interval
		rate := at.GetCurrentRate()
		if rate < 1.0 {
			rate = 1.0
		}

		// Calculate the interval between emissions for the current rate
		interval := time.Duration(float64(time.Second) / rate)

		now := time.Now()

		// If we are ahead of schedule, sleep until the next emission
		if now.Before(nextEmission) {
			sleepDuration := nextEmission.Sub(now)
			select {
			case <-time.After(sleepDuration):
				now = time.Now()
			case <-at.done:
				return
			}
		}

		nextEmission = now.Add(interval)

		// Emit
		select {
		case at.out <- item:
		case <-at.done:
			return
		}
	}
}

// adjustRate calculates the new rate based on stats and updates it atomically.
func (at *AdaptiveThrottler) adjustRate() {
	stats := at.monitor.GetStats()
	currentRate := at.GetCurrentRate()

	// Check if the resource usage is above the threshold
	isConstrained := stats.MemoryUsedPercent > at.config.MaxMemoryPercent ||
		stats.CPUUsagePercent > at.config.MaxCPUPercent

	// Check if the resource usage is below the recovery threshold
	isBelowRecovery := stats.MemoryUsedPercent < at.config.RecoveryMemoryThreshold &&
		stats.CPUUsagePercent < at.config.RecoveryCPUThreshold

	// Check if the resource usage is below the recovery threshold and hysteresis is disabled
	shouldIncrease := !isConstrained && (!at.config.EnableHysteresis || isBelowRecovery)

	targetRate := currentRate
	if isConstrained {
		// Reduce the rate by the backoff factor
		targetRate *= at.config.BackoffFactor
	} else if shouldIncrease {
		// Increase the rate by the recovery factor
		targetRate *= at.config.RecoveryFactor
		if targetRate > float64(at.config.MaxRate) {
			targetRate = float64(at.config.MaxRate)
		}
	}

	// Apply smoothing to the new rate
	newRate := currentRate + (targetRate-currentRate)*smoothingFactor

	// Enforce minimum rate
	if newRate < float64(at.config.MinRate) {
		newRate = float64(at.config.MinRate)
	}

	at.setRate(newRate)
}
