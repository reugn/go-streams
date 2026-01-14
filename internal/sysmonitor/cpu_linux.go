//go:build linux

package sysmonitor

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// linuxProcessSampler samples CPU usage for the current process on Linux
type linuxProcessSampler struct {
	fs          FileSystem
	pid         int
	lastUTime   float64
	lastSTime   float64
	lastSample  time.Time
	lastPercent float64
	clockTicks  int64
}

// newPlatformCPUSampler is the factory entry point used by cpu.go
func newPlatformCPUSampler(fs FileSystem) (ProcessCPUSampler, error) {
	pid := os.Getpid()
	if pid < 0 || pid > math.MaxInt32 {
		return nil, fmt.Errorf("invalid PID: %d", pid)
	}

	// We attempt to get clock ticks using the provided FS.
	// If it fails, we fallback to 100.
	ticks, err := getClockTicks(fs)
	if err != nil {
		ticks = 100
	}

	return &linuxProcessSampler{
		fs:         fs,
		pid:        pid,
		clockTicks: ticks,
	}, nil
}

// Sample returns the CPU usage percentage since the last sample
func (s *linuxProcessSampler) Sample(deltaTime time.Duration) float64 {
	now := time.Now()
	if s.lastSample.IsZero() {
		// First sample - try to initialize
		utime, stime, err := s.readProcessTimes()
		if err != nil {
			// If we can't read process times, still mark as initialized to avoid repeated attempts
			s.lastSample = now
			s.lastPercent = 0.0
			return 0.0
		}
		s.lastUTime = float64(utime)
		s.lastSTime = float64(stime)
		s.lastSample = now
		s.lastPercent = 0.0
		return 0.0
	}

	utime, stime, err := s.readProcessTimes()
	if err != nil {
		return s.lastPercent // Return last known value on error
	}

	elapsed := now.Sub(s.lastSample)
	// If called too frequently, return cached value to avoid jitter
	if elapsed < deltaTime/2 {
		return s.lastPercent
	}

	// Convert ticks to seconds and calculate CPU usage
	prevTotalTime := s.lastUTime + s.lastSTime
	currTotalTime := float64(utime) + float64(stime)
	cpuTimeDelta := currTotalTime - prevTotalTime
	cpuTimeSeconds := cpuTimeDelta / float64(s.clockTicks)
	wallTimeSeconds := elapsed.Seconds()

	// Normalized to 0-100% (divides by numCPU for system-wide metric)
	numcpu := runtime.NumCPU()
	if numcpu <= 0 {
		numcpu = 1 // Safety check
	}

	if wallTimeSeconds <= 0 {
		return s.lastPercent
	}

	percent := (cpuTimeSeconds / wallTimeSeconds) * 100.0 / float64(numcpu)

	if percent > 100.0 {
		percent = 100.0
	} else if percent < 0.0 {
		percent = 0.0
	}
	s.lastUTime = float64(utime)
	s.lastSTime = float64(stime)
	s.lastSample = now
	s.lastPercent = percent

	return percent
}

// Reset clears sampler state for a new session
func (s *linuxProcessSampler) Reset() {
	s.lastUTime = 0.0
	s.lastSTime = 0.0
	s.lastSample = time.Time{}
	s.lastPercent = 0.0
}

// IsInitialized returns true if at least one sample has been taken
func (s *linuxProcessSampler) IsInitialized() bool {
	return !s.lastSample.IsZero()
}

// readProcessTimes reads CPU times from /proc/<pid>/stat (returns ticks)
func (s *linuxProcessSampler) readProcessTimes() (utime, stime int64, err error) {
	path := fmt.Sprintf("/proc/%d/stat", s.pid)

	content, err := s.fs.ReadFile(path)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	fields := strings.Fields(string(content))
	if len(fields) < 17 {
		return 0, 0, fmt.Errorf(
			"invalid stat file format for /proc/%d/stat: expected at least 17 fields, got %d",
			s.pid, len(fields))
	}

	// utime=field[13], stime=field[14]
	utime, err = strconv.ParseInt(fields[13], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse utime from field[13]: %w", err)
	}

	stime, err = strconv.ParseInt(fields[14], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse stime from field[14]: %w", err)
	}

	return utime, stime, nil
}

// getClockTicks reads clock ticks per second from /proc/self/auxv (AT_CLKTCK=17)
func getClockTicks(fs FileSystem) (int64, error) {
	data, err := fs.ReadFile("/proc/self/auxv")
	if err != nil {
		return 100, nil // fallback
	}

	// Try 64-bit format first (16 bytes per entry)
	if len(data)%16 == 0 {
		buf := bytes.NewReader(data)
		var id, val uint64

		for {
			if err := binary.Read(buf, binary.LittleEndian, &id); err != nil {
				break
			}
			if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
				break
			}

			if id == 17 && val > 0 && val <= 10000 { // AT_CLKTCK
				return int64(val), nil
			}
		}
	}

	// Try 32-bit format (8 bytes per entry)
	if len(data)%8 == 0 {
		return parseAuxv32(data)
	}

	return 100, nil // fallback
}

// parseAuxv32 parses 32-bit auxv format
func parseAuxv32(data []byte) (int64, error) {
	buf := bytes.NewReader(data)
	var id, val uint32

	for {
		if err := binary.Read(buf, binary.LittleEndian, &id); err != nil {
			break
		}
		if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
			break
		}

		if id == 17 && val > 0 && val <= 10000 { // AT_CLKTCK
			return int64(val), nil
		}
	}

	return 100, nil
}
