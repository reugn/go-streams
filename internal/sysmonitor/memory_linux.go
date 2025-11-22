//go:build linux

package sysmonitor

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

var (
	memoryReader     MemoryReader
	fileSystem       FileSystem
	memoryReaderMu   sync.RWMutex
	memoryReaderOnce sync.Once
)

// GetSystemMemory returns the current system memory statistics.
// This function auto-detects the environment (cgroup v2, v1, or host)
// and returns appropriate memory information.
func GetSystemMemory() (SystemMemory, error) {
	memoryReaderOnce.Do(func() {
		memoryReaderMu.Lock()
		if memoryReader == nil {
			memoryReader = readSystemMemoryAuto
			fileSystem = OSFileSystem{}
		}
		memoryReaderMu.Unlock()
	})

	memoryReaderMu.RLock()
	reader := memoryReader
	memoryReaderMu.RUnlock()

	return reader()
}

// SetMemoryReader replaces the current memory reader (for testing)
func SetMemoryReader(reader MemoryReader) func() {
	memoryReaderMu.Lock()
	prev := memoryReader
	memoryReader = reader
	memoryReaderMu.Unlock()
	return func() {
		memoryReaderMu.Lock()
		memoryReader = prev
		memoryReaderMu.Unlock()
	}
}

type cgroupMemoryConfig struct {
	usagePath      string
	limitPath      string
	statPath       string
	statKey        string
	version        string
	checkUnlimited bool
}

var (
	cgroupV2Config = cgroupMemoryConfig{
		usagePath:      "/sys/fs/cgroup/memory.current",
		limitPath:      "/sys/fs/cgroup/memory.max",
		statPath:       "/sys/fs/cgroup/memory.stat",
		statKey:        "inactive_file",
		version:        "v2",
		checkUnlimited: false,
	}
	cgroupV1Config = cgroupMemoryConfig{
		usagePath:      "/sys/fs/cgroup/memory/memory.usage_in_bytes",
		limitPath:      "/sys/fs/cgroup/memory/memory.limit_in_bytes",
		statPath:       "/sys/fs/cgroup/memory/memory.stat",
		statKey:        "total_inactive_file",
		version:        "v1",
		checkUnlimited: true,
	}
)

// readSystemMemoryAuto detects the environment once and "upgrades" the reader
func readSystemMemoryAuto() (SystemMemory, error) {
	if m, err := readCgroupMemoryWithFS(fileSystem, cgroupV2Config); err == nil {
		memoryReaderMu.Lock()
		memoryReader = makeCgroupV2Reader(fileSystem)
		memoryReaderMu.Unlock()
		return m, nil
	}

	if m, err := readCgroupMemoryWithFS(fileSystem, cgroupV1Config); err == nil {
		memoryReaderMu.Lock()
		memoryReader = makeCgroupV1Reader(fileSystem)
		memoryReaderMu.Unlock()
		return m, nil
	}

	// Fallback to Host (Bare metal / VM / Unlimited Container)
	m, err := readSystemMemory()
	if err != nil {
		return SystemMemory{}, fmt.Errorf("failed to read system memory from all sources: %w", err)
	}
	memoryReaderMu.Lock()
	memoryReader = readSystemMemory
	memoryReaderMu.Unlock()
	return m, nil
}

// makeCgroupV2Reader creates a MemoryReader function for cgroup v2
func makeCgroupV2Reader(fs FileSystem) MemoryReader {
	return func() (SystemMemory, error) {
		return readCgroupMemoryWithFS(fs, cgroupV2Config)
	}
}

// makeCgroupV1Reader creates a MemoryReader function for cgroup v1
func makeCgroupV1Reader(fs FileSystem) MemoryReader {
	return func() (SystemMemory, error) {
		return readCgroupMemoryWithFS(fs, cgroupV1Config)
	}
}

func readCgroupMemoryWithFS(fs FileSystem, config cgroupMemoryConfig) (SystemMemory, error) {
	usage, err := readCgroupValueWithFS(fs, config.usagePath, false)
	if err != nil {
		return SystemMemory{}, fmt.Errorf("failed to read cgroup %s memory usage: %w", config.version, err)
	}

	limit, err := readCgroupValueWithFS(fs, config.limitPath, config.checkUnlimited)
	if err != nil {
		return SystemMemory{}, fmt.Errorf("failed to read cgroup %s memory limit: %w", config.version, err)
	}

	// Parse memory.stat to find reclaimable memory
	inactiveFile, err := readCgroupStatWithFS(fs, config.statPath, config.statKey)
	if err != nil {
		inactiveFile = 0 // Default to 0 if unavailable
	}

	// Available = (Limit - Usage) + Reclaimable
	// Note: If Limit - Usage is near zero, the kernel would reclaim inactive_file
	// Handle case where usage exceeds limit
	var available uint64
	if usage > limit {
		available = inactiveFile // Only reclaimable memory is available
	} else {
		available = (limit - usage) + inactiveFile
	}

	if available > limit {
		available = limit
	}

	return SystemMemory{
		Total:     limit,
		Available: available,
	}, nil
}

func readCgroupValueWithFS(fs FileSystem, path string, checkUnlimited bool) (uint64, error) {
	data, err := fs.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("failed to read file %s: %w", path, err)
	}
	str := strings.TrimSpace(string(data))
	if str == "max" {
		return 0, fmt.Errorf("unlimited memory limit")
	}
	val, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse value %q from %s: %w", str, path, err)
	}
	// Check for "unlimited" (random huge number in V1)
	if checkUnlimited && val > (1<<60) {
		return 0, fmt.Errorf("unlimited memory limit")
	}
	return val, nil
}

func readCgroupStatWithFS(fs FileSystem, path string, key string) (uint64, error) {
	f, err := fs.Open(path)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if bytes.HasPrefix(line, []byte(key)) {
			fields := bytes.Fields(line)
			if len(fields) >= 2 {
				val, err := strconv.ParseUint(string(fields[1]), 10, 64)
				if err != nil {
					return 0, fmt.Errorf("failed to parse value for key %q in %s: %w", key, path, err)
				}
				return val, nil
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error reading %s: %w", path, err)
	}
	return 0, fmt.Errorf("key %q not found in %s", key, path)
}

// readSystemMemory reads memory info from /proc/meminfo
func readSystemMemory() (SystemMemory, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return SystemMemory{}, fmt.Errorf("failed to open /proc/meminfo: %w", err)
	}
	defer file.Close()

	return parseMemInfo(file)
}

// parseMemInfo parses the /proc/meminfo file format
func parseMemInfo(r io.Reader) (SystemMemory, error) {
	scanner := bufio.NewScanner(r)

	var total, available, free, cached uint64
	memAvailableFound := false

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)

		if len(fields) < 2 {
			continue
		}

		key := strings.TrimSuffix(fields[0], ":")
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}

		// Convert from kB to bytes
		// Check for overflow
		const maxValueBeforeOverflow = (1<<64 - 1) / 1024
		if value > maxValueBeforeOverflow {
			return SystemMemory{}, fmt.Errorf("memory value too large: %d kB would overflow when converting to bytes", value)
		}
		value *= 1024

		switch key {
		case "MemTotal":
			total = value
		case "MemAvailable":
			available = value
			memAvailableFound = true
		case "MemFree":
			free = value
		case "Cached":
			cached = value
		}

		// Early exit if we have MemTotal and MemAvailable (kernel 3.14+)
		if total > 0 && memAvailableFound {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return SystemMemory{}, fmt.Errorf("error reading meminfo: %w", err)
	}

	if total == 0 {
		return SystemMemory{}, fmt.Errorf("could not find MemTotal in /proc/meminfo")
	}

	// Fallback calculation for MemAvailable
	if !memAvailableFound {
		available = free + cached
		if available == 0 {
			return SystemMemory{}, fmt.Errorf(
				"could not find MemAvailable in /proc/meminfo and fallback calculation failed (MemFree and Cached not found or both zero)")
		}
	}

	return SystemMemory{
		Total:     total,
		Available: available,
	}, nil
}
