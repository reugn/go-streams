//go:build !linux && !windows && (!darwin || !cgo)

package sysmonitor

import (
	"errors"
	"runtime"
)

// fallbackMemoryReader is a placeholder for unsupported platforms.
type fallbackMemoryReader struct{}

// newPlatformMemoryReader is the factory entry point.
func newPlatformMemoryReader(_ FileSystem) ProcessMemoryReader {
	return &fallbackMemoryReader{}
}

// Sample returns an error indicating that memory monitoring is not supported.
func (f *fallbackMemoryReader) Sample() (SystemMemory, error) {
	if runtime.GOOS == "darwin" {
		// Darwin memory monitoring is not supported on platforms that don't have CGO enabled.
		return SystemMemory{}, errors.New("memory monitoring on Darwin requires CGO_ENABLED=1")
	}
	return SystemMemory{}, errors.New("memory monitoring not supported on this platform")
}
