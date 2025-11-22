package sysmonitor

import (
	"time"
)

// ProcessCPUSampler samples CPU usage across different platforms
type ProcessCPUSampler interface {
	// Sample returns normalized CPU usage (0-100%) since last sample
	Sample(deltaTime time.Duration) float64

	// Reset clears sampler state for a new session
	Reset()

	// IsInitialized returns true if at least one sample has been taken
	IsInitialized() bool
}

// NewProcessSampler creates a CPU sampler for the current process
func NewProcessSampler() (ProcessCPUSampler, error) {
	return newProcessSampler()
}
