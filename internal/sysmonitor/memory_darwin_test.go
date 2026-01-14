//go:build darwin

package sysmonitor

import (
	"testing"
)

func TestDarwinMemoryReaderIntegration(t *testing.T) {
	reader := newPlatformMemoryReader(nil).(*darwinMemoryReader)

	mem, err := reader.Sample()
	if err != nil {
		t.Fatalf("Failed to sample Darwin memory: %v", err)
	}

	if mem.Total == 0 {
		t.Error("Expected non-zero total memory")
	}

	if mem.Available == 0 {
		t.Error("Expected non-zero available memory")
	}

	if mem.Available > mem.Total {
		t.Errorf("Available memory (%d) should not exceed total memory (%d)", mem.Available, mem.Total)
	}

	t.Logf("Darwin Memory - Total: %d bytes, Available: %d bytes", mem.Total, mem.Available)
}

func TestNewProcessMemoryReader(t *testing.T) {
	// Test the public API function
	reader := NewProcessMemoryReader(nil)

	// Should return a non-nil ProcessMemoryReader
	if reader == nil {
		t.Fatal("NewProcessMemoryReader returned nil")
	}
}

func TestDarwinMemoryReader_ReasonableValues(t *testing.T) {
	reader := newPlatformMemoryReader(nil).(*darwinMemoryReader)

	mem, err := reader.Sample()
	if err != nil {
		t.Fatalf("Failed to sample Darwin memory: %v", err)
	}

	// Darwin systems typically have at least 1GB RAM
	const minExpectedMemory = 1024 * 1024 * 1024 // 1GB in bytes
	if mem.Total < minExpectedMemory {
		t.Errorf("Total memory (%d bytes) seems unreasonably low, expected at least %d bytes", mem.Total, minExpectedMemory)
	}

	// Available memory should be less than total
	if mem.Available >= mem.Total {
		t.Errorf("Available memory (%d) should be less than total memory (%d)", mem.Available, mem.Total)
	}
}
