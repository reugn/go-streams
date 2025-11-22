//go:build windows

package sysmonitor

import (
	"fmt"
	"sync"
	"syscall"
	"unsafe"
)

var (
	kernel32                 = syscall.NewLazyDLL("kernel32.dll")
	procGlobalMemoryStatusEx = kernel32.NewProc("GlobalMemoryStatusEx")

	memoryReader = getSystemMemoryWindows
	memoryReaderMu sync.RWMutex
)

type memoryStatusEx struct {
	dwLength                uint32
	dwMemoryLoad            uint32
	ullTotalPhys            uint64
	ullAvailPhys            uint64
	ullTotalPageFile        uint64
	ullAvailPageFile        uint64
	ullTotalVirtual         uint64
	ullAvailVirtual         uint64
	ullAvailExtendedVirtual uint64
}

func GetSystemMemory() (SystemMemory, error) {
	memoryReaderMu.RLock()
	reader := memoryReader
	memoryReaderMu.RUnlock()
	return reader()
}

func getSystemMemoryWindows() (SystemMemory, error) {
	var memStatus memoryStatusEx

	memStatus.dwLength = uint32(unsafe.Sizeof(memStatus))

	ret, _, err := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&memStatus)))

	// If the function fails, the return value is zero.
	if ret == 0 || err != nil {
		return SystemMemory{}, fmt.Errorf("failed to get system memory status via GlobalMemoryStatusEx: %w", err)
	}

	return SystemMemory{
		Total:     memStatus.ullTotalPhys,
		Available: memStatus.ullAvailPhys,
	}, nil
}

// SetMemoryReader replaces the current memory reader (for testing).
// It returns a cleanup function to restore the previous reader.
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
