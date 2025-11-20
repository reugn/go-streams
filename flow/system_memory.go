package flow

import (
	"bufio"
	"bytes"
	"errors"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/shirou/gopsutil/v4/mem"
)

// FileSystem abstracts file system operations for testing
type FileSystem interface {
	ReadFile(name string) ([]byte, error)
	Open(name string) (fs.File, error)
}

// osFileSystem implements FileSystem using the os package
type osFileSystem struct{}

func (osFileSystem) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

func (osFileSystem) Open(name string) (fs.File, error) {
	return os.Open(name)
}

type systemMemoryReader func() (systemMemory, error)

var systemMemoryProvider atomic.Value
var fileSystemProvider atomic.Value

func init() {
	systemMemoryProvider.Store(systemMemoryReader(readSystemMemoryAuto))
	fileSystemProvider.Store(FileSystem(osFileSystem{}))
}

// readSystemMemoryAuto detects the environment once and "upgrades" the reader
func readSystemMemoryAuto() (systemMemory, error) {
	if m, err := readCgroupV2Memory(); err == nil {
		systemMemoryProvider.Store(systemMemoryReader(readCgroupV2Memory))
		return m, nil
	}

	if m, err := readCgroupV1Memory(); err == nil {
		systemMemoryProvider.Store(systemMemoryReader(readCgroupV1Memory))
		return m, nil
	}

	// Fallback to Host (Bare metal / VM / Unlimited Container)
	systemMemoryProvider.Store(systemMemoryReader(readSystemMemory))
	return readSystemMemory()
}

func readCgroupV2Memory() (systemMemory, error) {
	return readCgroupV2MemoryWithFS(loadFileSystem())
}

func readCgroupV2MemoryWithFS(fs FileSystem) (systemMemory, error) {
	usage, err := readCgroupValueWithFS(fs, "/sys/fs/cgroup/memory.current")
	if err != nil {
		return systemMemory{}, err
	}

	limit, err := readCgroupValueWithFS(fs, "/sys/fs/cgroup/memory.max")
	if err != nil {
		return systemMemory{}, err
	}

	// Parse memory.stat to find reclaimable memory (inactive_file)
	inactiveFile, _ := readCgroupStatWithFS(fs, "/sys/fs/cgroup/memory.stat", "inactive_file")

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

	return systemMemory{
		Total:     limit,
		Available: available,
	}, nil
}

func readCgroupV1Memory() (systemMemory, error) {
	return readCgroupV1MemoryWithFS(loadFileSystem())
}

func readCgroupV1MemoryWithFS(fs FileSystem) (systemMemory, error) {
	usage, err := readCgroupValueWithFS(fs, "/sys/fs/cgroup/memory/memory.usage_in_bytes")
	if err != nil {
		return systemMemory{}, err
	}

	limit, err := readCgroupValueWithFS(fs, "/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return systemMemory{}, err
	}

	// Check for "unlimited" (random huge number in V1)
	if limit > (1 << 60) {
		return systemMemory{}, os.ErrNotExist
	}

	// Parse memory.stat for V1
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

	return systemMemory{
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
	return strconv.ParseUint(str, 10, 64)
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
				return strconv.ParseUint(string(fields[1]), 10, 64)
			}
		}
	}
	return 0, errors.New("key not found")
}

func readSystemMemory() (systemMemory, error) {
	v, err := mem.VirtualMemory()
	if err != nil {
		return systemMemory{}, err
	}
	return systemMemory{
		Total:     v.Total,
		Available: v.Available,
	}, nil
}

func getSystemMemory() (systemMemory, error) {
	return loadSystemMemoryReader()()
}

// GetSystemMemory returns the current system memory statistics.
// This function auto-detects the environment (cgroup v2, v1, or host)
// and returns appropriate memory information.
func GetSystemMemory() (SystemMemory, error) {
	mem, err := getSystemMemory()
	if err != nil {
		return SystemMemory{}, err
	}
	return SystemMemory(mem), nil
}

// SystemMemory represents system memory information
type SystemMemory struct {
	Total     uint64
	Available uint64
}

func loadSystemMemoryReader() systemMemoryReader {
	return systemMemoryProvider.Load().(systemMemoryReader)
}

// setSystemMemoryReader replaces the current system memory reader and returns a restore function.
// Created for testing/mocking purposes.
func setSystemMemoryReader(reader systemMemoryReader) func() {
	prev := loadSystemMemoryReader()
	systemMemoryProvider.Store(reader)
	return func() {
		systemMemoryProvider.Store(prev)
	}
}

func loadFileSystem() FileSystem {
	return fileSystemProvider.Load().(FileSystem)
}
