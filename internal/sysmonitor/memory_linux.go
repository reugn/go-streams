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
	memoryReader MemoryReader
	fileSystem   FileSystem
	// memoryReaderMu protects concurrent access to memoryReader
	memoryReaderMu sync.RWMutex
)

// GetSystemMemory returns the current system memory statistics.
// This function auto-detects the environment (cgroup v2, v1, or host)
// and returns appropriate memory information.
func GetSystemMemory() (SystemMemory, error) {
	memoryReaderMu.RLock()
	reader := memoryReader
	memoryReaderMu.RUnlock()

	if reader == nil {
		memoryReaderMu.Lock()
		if memoryReader == nil {
			memoryReader = readSystemMemoryAuto
			fileSystem = OSFileSystem{}
		}
		reader = memoryReader
		memoryReaderMu.Unlock()
	}

	return reader()
}

// readSystemMemoryAuto detects the environment once and "upgrades" the reader
func readSystemMemoryAuto() (SystemMemory, error) {
	if m, err := readCgroupV2Memory(); err == nil {
		memoryReaderMu.Lock()
		memoryReader = readCgroupV2Memory
		memoryReaderMu.Unlock()
		return m, nil
	}

	if m, err := readCgroupV1Memory(); err == nil {
		memoryReaderMu.Lock()
		memoryReader = readCgroupV1Memory
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

func readCgroupV2Memory() (SystemMemory, error) {
	return readCgroupV2MemoryWithFS(fileSystem)
}

func readCgroupV2MemoryWithFS(fs FileSystem) (SystemMemory, error) {
	usage, err := readCgroupValueWithFS(fs, "/sys/fs/cgroup/memory.current")
	if err != nil {
		return SystemMemory{}, fmt.Errorf("failed to read cgroup v2 memory usage: %w", err)
	}

	limit, err := readCgroupValueWithFS(fs, "/sys/fs/cgroup/memory.max")
	if err != nil {
		return SystemMemory{}, fmt.Errorf("failed to read cgroup v2 memory limit: %w", err)
	}

	// Parse memory.stat to find reclaimable memory (inactive_file)
	inactiveFile, err := readCgroupStatWithFS(fs, "/sys/fs/cgroup/memory.stat", "inactive_file")
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

func readCgroupV1Memory() (SystemMemory, error) {
	return readCgroupV1MemoryWithFS(fileSystem)
}

func readCgroupV1MemoryWithFS(fs FileSystem) (SystemMemory, error) {
	usage, err := readCgroupValueWithFS(fs, "/sys/fs/cgroup/memory/memory.usage_in_bytes")
	if err != nil {
		return SystemMemory{}, fmt.Errorf("failed to read cgroup v1 memory usage: %w", err)
	}

	limit, err := readCgroupValueWithFS(fs, "/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return SystemMemory{}, fmt.Errorf("failed to read cgroup v1 memory limit: %w", err)
	}

	// Check for "unlimited" (random huge number in V1)
	if limit > (1 << 60) {
		return SystemMemory{}, os.ErrNotExist
	}

	// Parse memory.stat for V1
	// Note: total_inactive_file is optional - if unavailable, we continue with 0 (graceful degradation)
	inactiveFile, _ := readCgroupStatWithFS(fs, "/sys/fs/cgroup/memory/memory.stat", "total_inactive_file")

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

func readCgroupValueWithFS(fs FileSystem, path string) (uint64, error) {
	data, err := fs.ReadFile(path)
	if err != nil {
		return 0, err
	}
	str := strings.TrimSpace(string(data))
	if str == "max" {
		return 0, os.ErrNotExist
	}
	val, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse value %q from %s: %w", str, path, err)
	}
	return val, nil
}

func readCgroupStatWithFS(fs FileSystem, path string, key string) (uint64, error) {
	f, err := fs.Open(path)
	if err != nil {
		return 0, err
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

	var total, available uint64
	found := 0

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
		value *= 1024

		switch key {
		case "MemTotal":
			total = value
			found++
		case "MemAvailable":
			available = value
			found++
		}

		if found == 2 {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return SystemMemory{}, fmt.Errorf("error reading meminfo: %w", err)
	}

	if found != 2 {
		return SystemMemory{}, fmt.Errorf(
			"could not find MemTotal and MemAvailable in /proc/meminfo (found %d of 2 required fields)",
			found)
	}

	return SystemMemory{
		Total:     total,
		Available: available,
	}, nil
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
