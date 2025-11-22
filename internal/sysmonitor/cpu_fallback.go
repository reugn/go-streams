//go:build !linux && !darwin && !windows

package sysmonitor

import (
	"errors"
	"time"
)

// ProcessSampler is a stub implementation for unsupported platforms
type ProcessSampler struct{}

// newProcessSampler returns an error on unsupported platforms
func newProcessSampler() (*ProcessSampler, error) {
	return nil, errors.New("CPU monitoring not supported on this platform")
}

// Sample always returns 0.0 on unsupported platforms
func (s *ProcessSampler) Sample(deltaTime time.Duration) float64 {
	return 0.0
}

// Reset does nothing on unsupported platforms
func (s *ProcessSampler) Reset() {}

// IsInitialized always returns false on unsupported platforms
func (s *ProcessSampler) IsInitialized() bool {
	return false
}
