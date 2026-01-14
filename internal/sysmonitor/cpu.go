package sysmonitor

import (
	"time"
)

// ProcessCPUSampler provides a cross-platform interface for sampling
// CPU usage of the current process. Returns normalized CPU usage (0-100%)
// across all available CPU cores.
//
// Thread-safety: implementations keep internal cached state (e.g. last sample
// timestamp/CPU times) and are NOT safe for concurrent use. If you need to call
// Sample/Reset/IsInitialized from multiple goroutines, protect the sampler with
// an external mutex.
//
// Platform implementations:
//   - Linux: reads from /proc/[pid]/stat
//   - Darwin (macOS): uses syscall.Getrusage
//   - Windows: uses GetProcessTimes API
//   - Other platforms: returns error
type ProcessCPUSampler interface {
	// Sample returns normalized CPU usage percentage (0-100%) since last sample.
	// On first call, initializes state and returns 0.0. If elapsed time since
	// last sample is less than half of deltaTime, returns last known value.
	// Returns last known value on error.
	Sample(deltaTime time.Duration) float64

	// Reset clears sampler state for a new session. Next Sample call will
	// behave as the first sample.
	Reset()

	// IsInitialized returns true if at least one sample has been taken.
	IsInitialized() bool
}

// NewProcessSampler creates a CPU sampler for the current process.
// Automatically selects platform-specific implementation (Linux/Darwin/Windows).
// Returns error on unsupported platforms or if sampler creation fails.
func NewCPUSampler(fs FileSystem) (ProcessCPUSampler, error) {
	return newPlatformCPUSampler(fs)
}
