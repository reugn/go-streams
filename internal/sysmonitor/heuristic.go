package sysmonitor

import (
	"math"
	"runtime"
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

// GoroutineHeuristicSampler uses goroutine count as a CPU usage proxy
type GoroutineHeuristicSampler struct{}

// NewGoroutineHeuristicSampler creates a new heuristic CPU sampler
func NewGoroutineHeuristicSampler() ProcessCPUSampler {
	return &GoroutineHeuristicSampler{}
}

// Verify implementation of ProcessCPUSampler interface
var _ ProcessCPUSampler = &GoroutineHeuristicSampler{}

// Sample returns the CPU usage percentage over the given time delta
func (s *GoroutineHeuristicSampler) Sample(_ time.Duration) float64 {
	// Uses logarithmic scaling for more realistic CPU estimation
	// Base level: 1-10 goroutines = baseline CPU usage (10-20%)
	// Logarithmic growth to avoid overestimation at high goroutine counts
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

// Reset prepares the sampler for a new sampling session
func (s *GoroutineHeuristicSampler) Reset() {
	// No state to reset for heuristic sampler
}

// IsInitialized returns true if the sampler has been initialized with at least one sample
func (s *GoroutineHeuristicSampler) IsInitialized() bool {
	return true
}
