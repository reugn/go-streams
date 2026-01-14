//go:build linux

package sysmonitor

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
)

// linuxMemoryReader implements ProcessMemoryReader for Linux.
// It encapsulates the logic for auto-detecting Cgroup/Host memory stats.
type linuxMemoryReader struct {
	// delegate is the active strategy (CgroupV2, CgroupV1, or Host).
	delegate func() (SystemMemory, error)
	fs       FileSystem
	mu       sync.RWMutex
	once     sync.Once
}

// newPlatformMemoryReader is the factory entry point used by memory.go.
func newPlatformMemoryReader(fs FileSystem) ProcessMemoryReader {
	return &linuxMemoryReader{
		fs: fs,
	}
}

// Sample returns the current system memory statistics.
// It lazily initializes the correct strategy (Cgroup vs Host) on the first call.
func (m *linuxMemoryReader) Sample() (SystemMemory, error) {
	m.once.Do(func() {
		m.detectEnvironment()
	})

	m.mu.RLock()
	strategy := m.delegate
	m.mu.RUnlock()

	if strategy == nil {
		return SystemMemory{}, fmt.Errorf("failed to initialize memory reader strategy")
	}

	return strategy()
}

// detectEnvironment checks for Cgroups and sets the appropriate delegate strategy.
func (m *linuxMemoryReader) detectEnvironment() {
	// 1. Try Cgroup V2
	if m.cgroupFilesExist(cgroupV2Config) {
		m.mu.Lock()
		m.delegate = m.makeCgroupV2Strategy()
		m.mu.Unlock()
		return
	}

	// 2. Try Cgroup V1
	if m.cgroupFilesExist(cgroupV1Config) {
		m.mu.Lock()
		m.delegate = m.makeCgroupV1Strategy()
		m.mu.Unlock()
		return
	}

	// 3. Fallback to Host /proc/meminfo
	m.mu.Lock()
	m.delegate = m.readHostMemory
	m.mu.Unlock()
}

// readHostMemory reads memory info from /proc/meminfo (via FS abstraction)
func (m *linuxMemoryReader) readHostMemory() (SystemMemory, error) {
	file, err := m.fs.Open("/proc/meminfo")
	if err != nil {
		return SystemMemory{}, fmt.Errorf("failed to open /proc/meminfo: %w", err)
	}
	defer file.Close()

	return parseMemInfo(file)
}

// makeCgroupV2Strategy returns a function bound to the current FS instance
func (m *linuxMemoryReader) makeCgroupV2Strategy() func() (SystemMemory, error) {
	return func() (SystemMemory, error) {
		return readCgroupMemoryWithFS(m.fs, cgroupV2Config)
	}
}

// makeCgroupV1Strategy returns a function bound to the current FS instance
func (m *linuxMemoryReader) makeCgroupV1Strategy() func() (SystemMemory, error) {
	return func() (SystemMemory, error) {
		return readCgroupMemoryWithFS(m.fs, cgroupV1Config)
	}
}

// cgroupFilesExist checks if the required cgroup files exist and are readable
func (m *linuxMemoryReader) cgroupFilesExist(config cgroupMemoryConfig) bool {
	// Check if all required files can be read
	paths := []string{config.usagePath, config.limitPath, config.statPath}
	for _, path := range paths {
		if _, err := m.fs.ReadFile(path); err != nil {
			return false
		}
	}
	return true
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

func parseMemInfo(r io.Reader) (SystemMemory, error) {
	memFields, err := parseMemInfoFields(r)
	if err != nil {
		return SystemMemory{}, err
	}

	if memFields.total == 0 {
		return SystemMemory{}, fmt.Errorf("could not find MemTotal in /proc/meminfo")
	}

	available, err := calculateAvailableMemory(memFields)
	if err != nil {
		return SystemMemory{}, err
	}

	if available > memFields.total {
		available = memFields.total
	}

	return SystemMemory{
		Total:     memFields.total,
		Available: available,
	}, nil
}

type memInfoFields struct {
	total             uint64
	available         uint64
	free              uint64
	cached            uint64
	memAvailableFound bool
}

func parseMemInfoFields(r io.Reader) (memInfoFields, error) {
	scanner := bufio.NewScanner(r)
	var fields memInfoFields

	for scanner.Scan() {
		line := scanner.Text()
		lineFields := strings.Fields(line)

		if len(lineFields) < 2 {
			continue
		}

		key := strings.TrimSuffix(lineFields[0], ":")
		value, err := strconv.ParseUint(lineFields[1], 10, 64)
		if err != nil {
			continue
		}

		const maxValueBeforeOverflow = (1<<64 - 1) / 1024
		if value > maxValueBeforeOverflow {
			return memInfoFields{}, fmt.Errorf(
				"memory value too large: %d kB would overflow when converting to bytes", value)
		}
		value *= 1024

		switch key {
		case "MemTotal":
			fields.total = value
		case "MemAvailable":
			fields.available = value
			fields.memAvailableFound = true
		case "MemFree":
			fields.free = value
		case "Cached":
			fields.cached = value
		}

		if fields.total > 0 && fields.memAvailableFound {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return memInfoFields{}, fmt.Errorf("error reading meminfo: %w", err)
	}

	return fields, nil
}

func calculateAvailableMemory(fields memInfoFields) (uint64, error) {
	if fields.memAvailableFound {
		return fields.available, nil
	}
	available := fields.free + fields.cached
	if available == 0 {
		return 0, fmt.Errorf(
			"could not find MemAvailable in /proc/meminfo and fallback calculation failed " +
				"(MemFree and Cached not found or both zero)")
	}
	return available, nil
}
