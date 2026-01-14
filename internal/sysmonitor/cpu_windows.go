//go:build windows

package sysmonitor

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"syscall"
	"time"
)

// windowsProcessSampler samples CPU usage for the current process on Windows
type windowsProcessSampler struct {
	pid         int
	lastUTime   float64
	lastSTime   float64
	lastSample  time.Time
	lastPercent float64
}

// newPlatformCPUSampler matches the factory signature required by cpu.go.
func newPlatformCPUSampler(_ FileSystem) (ProcessCPUSampler, error) {
	pid := os.Getpid()
	if pid < 0 || pid > math.MaxInt32 {
		return nil, fmt.Errorf("invalid PID: %d", pid)
	}

	return &windowsProcessSampler{
		pid: pid,
	}, nil
}

// Sample returns the CPU usage percentage since the last sample
func (s *windowsProcessSampler) Sample(deltaTime time.Duration) float64 {
	utime, stime, err := s.getCurrentCPUTimes()
	if err != nil {
		// If we have a previous valid sample, return it; otherwise return 0
		if s.lastSample.IsZero() {
			return 0.0
		}
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

	// GetProcessTimes returns sum of all threads across all cores
	cpuTimeDelta := (utime + stime) - (s.lastUTime + s.lastSTime)
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

	// Handle negative deltas (can happen due to clock adjustments or process restarts)
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
func (s *windowsProcessSampler) Reset() {
	s.lastUTime = 0.0
	s.lastSTime = 0.0
	s.lastSample = time.Time{}
	s.lastPercent = 0.0
}

// IsInitialized returns true if at least one sample has been taken
func (s *windowsProcessSampler) IsInitialized() bool {
	return !s.lastSample.IsZero()
}

// getProcessCPUTimes retrieves CPU times via GetProcessTimes (returns FILETIME)
func getProcessCPUTimes(pid int) (syscall.Filetime, syscall.Filetime, error) {
	var c, e, k, u syscall.Filetime

	currentPid := os.Getpid()
	var h syscall.Handle
	if pid == currentPid {
		var err error
		h, err = syscall.GetCurrentProcess()
		if err != nil {
			return k, u, fmt.Errorf("failed to get current process handle: %w", err)
		}
	} else {
		// Try PROCESS_QUERY_LIMITED_INFORMATION first (works on more Windows versions)
		// Fall back to PROCESS_QUERY_INFORMATION if that fails
		var err error
		// Define constant here since we can't depend on x/sys/windows
		const PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
		h, err = syscall.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
		if err != nil {
			// Fallback to PROCESS_QUERY_INFORMATION
			h, err = syscall.OpenProcess(syscall.PROCESS_QUERY_INFORMATION, false, uint32(pid))
			if err != nil {
				return k, u, fmt.Errorf("failed to open process %d: %w", pid, err)
			}
		}
		defer syscall.CloseHandle(h)
	}

	err := syscall.GetProcessTimes(h, &c, &e, &k, &u)
	if err != nil {
		return k, u, fmt.Errorf("failed to get process times for PID %d: %w", pid, err)
	}

	return k, u, nil
}

// convertFiletimeToSeconds converts FILETIME (100ns intervals) to seconds
func convertFiletimeToSeconds(ft syscall.Filetime) float64 {
	// Join high/low into single 64-bit integer
	ticks := int64(ft.HighDateTime)<<32 | int64(ft.LowDateTime)
	// 1 tick = 100ns = 0.0000001 seconds
	return float64(ticks) * 1e-7
}

// getCurrentCPUTimes reads CPU times for the process (returns seconds)
func (s *windowsProcessSampler) getCurrentCPUTimes() (utime, stime float64, err error) {
	k, u, err := getProcessCPUTimes(s.pid)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get CPU times for process %d: %w", s.pid, err)
	}

	utime = convertFiletimeToSeconds(u)
	stime = convertFiletimeToSeconds(k)
	return utime, stime, nil
}
