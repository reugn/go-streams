//go:build !linux && !windows && (!darwin || (darwin && !cgo))

package sysmonitor

import (
	"errors"
	"runtime"
	"sync"
)

var (
	// memoryReader is nil on unsupported platforms
	memoryReader MemoryReader
	// memoryReaderMu protects concurrent access to memoryReader
	memoryReaderMu sync.RWMutex
)

// GetSystemMemory returns an error on unsupported platforms.
func GetSystemMemory() (SystemMemory, error) {
	memoryReaderMu.RLock()
	reader := memoryReader
	memoryReaderMu.RUnlock()

	if reader != nil {
		return reader()
	}

	if runtime.GOOS == "darwin" {
		return SystemMemory{}, errors.New("memory monitoring not supported on this platform without cgo")
	}
	return SystemMemory{}, errors.New("memory monitoring not supported on this platform")
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
