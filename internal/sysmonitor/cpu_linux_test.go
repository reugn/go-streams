//go:build linux

package sysmonitor

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/reugn/go-streams/internal/testutil"
)

// generateAuxv creates a binary payload mimicking /proc/self/auxv
// ID 17 = AT_CLKTCK
func generateAuxv(ticks uint64) []byte {
	buf := new(bytes.Buffer)
	// Write AT_CLKTCK entry
	binary.Write(buf, binary.LittleEndian, uint64(17)) // ID
	binary.Write(buf, binary.LittleEndian, ticks)      // Value
	// Write null entry to end list
	binary.Write(buf, binary.LittleEndian, uint64(0))
	binary.Write(buf, binary.LittleEndian, uint64(0))
	return buf.Bytes()
}

func TestLinuxProcessSampler(t *testing.T) {
	// Setup Mock FS
	mockFS := &testutil.MockFileSystem{
		Files: map[string][]byte{
			"/proc/self/auxv": generateAuxv(100), // 100 ticks per second
		},
	}

	// Test Factory
	samplerInterface, err := newPlatformCPUSampler(mockFS)
	if err != nil {
		t.Fatalf("Failed to create sampler: %v", err)
	}
	sampler := samplerInterface.(*linuxProcessSampler)

	// Verify Ticks parsing
	if sampler.clockTicks != 100 {
		t.Errorf("Expected 100 clock ticks, got %d", sampler.clockTicks)
	}

	// Setup Test Data
	// format: pid (comm) state ppid pgrp session tty_nr tpgid flags minflt cminflt majflt cmajflt utime stime ...
	// Fields 13 (utime) and 14 (stime) are 0-indexed in fields array (so indices 13 and 14)
	// Actually strings.Fields 1-based index in documentation usually maps to:
	// 13: utime, 14: stime.
	// Let's construct a valid line. "100 (go) R 1 1 1 0 -1 4194304 100 0 0 0 10 20 0 0 ..."
	// ticks = 10 + 20 = 30 total ticks
	statContent1 := []byte("100 (test) R 1 1 1 0 -1 0 0 0 0 0 10 20 0 0 20 0 1 0 0 0 0 0 0 0 0 0 0 0 0")
	mockFS.Files[fmt.Sprintf("/proc/%d/stat", sampler.pid)] = statContent1

	// 1. First Sample (Initialization)
	val := sampler.Sample(time.Second)
	if val != 0.0 {
		t.Errorf("First sample should be 0.0, got %f", val)
	}
	if !sampler.IsInitialized() {
		t.Error("Sampler should be initialized after first call")
	}

	// 2. Second Sample (Activity)
	// Increase ticks by 50 (User) + 50 (System) = 100 ticks delta
	// 100 ticks / 100 ticks/sec = 1 second CPU time
	statContent2 := []byte("100 (test) R 1 1 1 0 -1 0 0 0 0 0 60 70 0 0 20 0 1 0 0 0 0 0 0 0 0 0 0 0 0")
	mockFS.Files[fmt.Sprintf("/proc/%d/stat", sampler.pid)] = statContent2

	// Sleep briefly to allow Sample's timing logic to progress.
	// This simulates passage of real time for the test.
	time.Sleep(50 * time.Millisecond)
	val = sampler.Sample(0) // 0 delta to force calculation

	if val <= 0 {
		t.Errorf("Expected CPU usage > 0, got %f", val)
	}

	// 3. Test Reset
	sampler.Reset()
	if sampler.IsInitialized() {
		t.Error("Sampler should not be initialized after Reset")
	}

	// 4. Test Bad File
	mockFS.Files[fmt.Sprintf("/proc/%d/stat", sampler.pid)] = []byte("garbage")
	val = sampler.Sample(time.Second)
	// Should return last known good value (or 0 if reset)
	if val != 0.0 {
		t.Errorf("Expected 0.0 on error after reset, got %f", val)
	}
}

func TestGetClockTicksFallback(t *testing.T) {
	mockFS := &testutil.MockFileSystem{Files: map[string][]byte{}} // Empty FS

	ticks, err := getClockTicks(mockFS)
	if err != nil {
		t.Errorf("Expected no error on fallback, got %v", err)
	}
	if ticks != 100 {
		t.Errorf("Expected fallback ticks 100, got %d", ticks)
	}
}

// generateAuxv32 creates a binary payload mimicking 32-bit /proc/self/auxv
func generateAuxv32(ticks uint32) []byte {
	buf := new(bytes.Buffer)
	// Write AT_CLKTCK entry (32-bit format)
	binary.Write(buf, binary.LittleEndian, uint32(17)) // ID
	binary.Write(buf, binary.LittleEndian, ticks)      // Value
	// Write null entry to end list
	binary.Write(buf, binary.LittleEndian, uint32(0))
	binary.Write(buf, binary.LittleEndian, uint32(0))
	return buf.Bytes()
}

func TestParseAuxv32(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected int64
	}{
		{
			name:     "Valid 32-bit auxv with ticks",
			data:     generateAuxv32(250),
			expected: 250,
		},
		{
			name:     "Valid 32-bit auxv with different ticks",
			data:     generateAuxv32(500),
			expected: 500,
		},
		{
			name:     "32-bit auxv with invalid ticks (too high)",
			data:     generateAuxv32(20000), // > 10000, should be ignored
			expected: 100,                   // fallback
		},
		{
			name:     "32-bit auxv with zero ticks",
			data:     generateAuxv32(0), // should be ignored
			expected: 100,               // fallback
		},
		{
			name:     "Empty auxv data",
			data:     []byte{},
			expected: 100, // fallback
		},
		{
			name:     "Malformed auxv data (odd length)",
			data:     []byte{1, 2, 3}, // not multiple of 8
			expected: 100,             // fallback
		},
		{
			name: "32-bit auxv with wrong ID",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.LittleEndian, uint32(99)) // Wrong ID
				binary.Write(buf, binary.LittleEndian, uint32(250))
				binary.Write(buf, binary.LittleEndian, uint32(0))
				binary.Write(buf, binary.LittleEndian, uint32(0))
				return buf.Bytes()
			}(),
			expected: 100, // fallback
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseAuxv32(tt.data)
			if err != nil {
				t.Errorf("parseAuxv32() error = %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("parseAuxv32() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestGetClockTicks32Bit(t *testing.T) {
	mockFS := &testutil.MockFileSystem{
		Files: map[string][]byte{
			"/proc/self/auxv": generateAuxv32(250), // 32-bit format (8 bytes per entry)
		},
	}

	ticks, err := getClockTicks(mockFS)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if ticks != 250 {
		t.Errorf("Expected 250 ticks, got %d", ticks)
	}
}

func TestNewPlatformCPUSamplerErrors(t *testing.T) {
	// Test clock ticks read failure (should still succeed with fallback)
	mockFS := &testutil.MockFileSystem{
		OpenErrs: map[string]error{
			"/proc/self/auxv": fmt.Errorf("permission denied"),
		},
	}

	sampler, err := newPlatformCPUSampler(mockFS)
	if err != nil {
		t.Errorf("Expected no error with fallback, got %v", err)
	}
	if sampler == nil {
		t.Error("Expected sampler to be created")
	}

	// Verify fallback ticks were used
	linuxSampler := sampler.(*linuxProcessSampler)
	if linuxSampler.clockTicks != 100 {
		t.Errorf("Expected fallback ticks 100, got %d", linuxSampler.clockTicks)
	}
}
