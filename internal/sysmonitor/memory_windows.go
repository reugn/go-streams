//go:build windows

package sysmonitor

import (
	"fmt"
	"syscall"
	"unsafe"
)

var (
	kernel32                 = syscall.NewLazyDLL("kernel32.dll")
	procGlobalMemoryStatusEx = kernel32.NewProc("GlobalMemoryStatusEx")
)

// memoryStatusEx matches the MEMORYSTATUSEX structure in Windows API
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

// windowsMemoryReader implements ProcessMemoryReader for Windows using Win32 API.
type windowsMemoryReader struct{}

// newPlatformMemoryReader is the factory entry point.
func newPlatformMemoryReader(_ FileSystem) ProcessMemoryReader {
	return &windowsMemoryReader{}
}

// Sample returns the current system memory statistics.
func (w *windowsMemoryReader) Sample() (SystemMemory, error) {
	var memStatus memoryStatusEx
	memStatus.dwLength = uint32(unsafe.Sizeof(memStatus))

	// Call GlobalMemoryStatusEx
	ret, _, err := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&memStatus)))

	// If the function fails, the return value is zero.
	if ret == 0 {
		return SystemMemory{}, fmt.Errorf("failed to get system memory status via GlobalMemoryStatusEx: %w", err)
	}

	return SystemMemory{
		Total:     memStatus.ullTotalPhys,
		Available: memStatus.ullAvailPhys,
	}, nil
}
