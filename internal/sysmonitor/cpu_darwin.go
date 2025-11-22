//go:build darwin

package sysmonitor

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"syscall"
	"time"
)

// cpuTimesStat holds CPU time in seconds
type cpuTimesStat struct {
	User   float64
	System float64
}

// ProcessSampler samples CPU usage for the current process
type ProcessSampler struct {
	pid         int
	lastUTime   float64
	lastSTime   float64
	lastSample  time.Time
	lastPercent float64
}

// newProcessSampler creates a new CPU sampler for the current process
func newProcessSampler() (*ProcessSampler, error) {
	pid := os.Getpid()
	if pid < 0 || pid > math.MaxInt32 {
		return nil, fmt.Errorf("invalid PID: %d", pid)
	}

	return &ProcessSampler{
		pid: pid,
	}, nil
}

// Sample returns the CPU usage percentage since the last sample
func (s *ProcessSampler) Sample(deltaTime time.Duration) float64 {
	utime, stime, err := s.readProcessTimesDarwin()
	if err != nil {
		return s.lastPercent
	}

	now := time.Now()
	if s.lastSample.IsZero() {
		s.lastUTime = utime
		s.lastSTime = stime
		s.lastSample = now
		s.lastPercent = 0.0
		return 0.0
	}

	elapsed := now.Sub(s.lastSample)
	if elapsed < deltaTime/2 {
		return s.lastPercent
	}

	prevTotal := s.lastUTime + s.lastSTime
	currTotal := utime + stime
	cpuTimeDelta := currTotal - prevTotal
	wallTimeSeconds := elapsed.Seconds()

	if wallTimeSeconds <= 0 {
		return s.lastPercent
	}

	// Normalized to 0-100% (divides by numCPU for system-wide metric)
	numcpu := runtime.NumCPU()
	if numcpu <= 0 {
		numcpu = 1 // Safety check
	}
	percent := (cpuTimeDelta / wallTimeSeconds) * 100.0 / float64(numcpu)

	if percent < 0.0 {
		percent = 0.0
	} else if percent > 100.0 {
		percent = 100.0
	}

	s.lastUTime = utime
	s.lastSTime = stime
	s.lastSample = now
	s.lastPercent = percent

	return percent
}

// Reset clears sampler state for a new session
func (s *ProcessSampler) Reset() {
	s.lastUTime = 0
	s.lastSTime = 0
	s.lastSample = time.Time{}
	s.lastPercent = 0.0
}

// IsInitialized returns true if at least one sample has been taken
func (s *ProcessSampler) IsInitialized() bool {
	return !s.lastSample.IsZero()
}

// readProcessTimesDarwin reads CPU times via syscall.Getrusage (returns seconds)
func (s *ProcessSampler) readProcessTimesDarwin() (utime, stime float64, err error) {
	var rusage syscall.Rusage
	err = syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)
	if err != nil {
		return 0, 0, err
	}

	utime = float64(rusage.Utime.Sec) + float64(rusage.Utime.Usec)/1e6
	stime = float64(rusage.Stime.Sec) + float64(rusage.Stime.Usec)/1e6
	return utime, stime, nil
}