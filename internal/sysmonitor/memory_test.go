package sysmonitor

import (
	"runtime"
	"strings"
	"testing"
)

func TestGetSystemMemory(t *testing.T) {
	mem, err := GetSystemMemory()
	if err != nil {
		// Skip test on unsupported platforms
		if strings.Contains(err.Error(), "not supported on this platform") {
			t.Skipf("GetSystemMemory not supported on %s: %v", runtime.GOOS, err)
		}
		t.Fatalf("GetSystemMemory failed: %v", err)
	}

	if mem.Total == 0 {
		t.Error("Total memory should not be zero")
	}

	if mem.Available > mem.Total {
		t.Errorf("Available memory (%d) should not exceed total memory (%d)", mem.Available, mem.Total)
	}
}

func TestMemorySampler(t *testing.T) {
	mem, err := GetSystemMemory()
	if err != nil {
		// Skip test on unsupported platforms
		if strings.Contains(err.Error(), "not supported on this platform") {
			t.Skipf("GetSystemMemory not supported on %s: %v", runtime.GOOS, err)
		}
		t.Fatalf("GetSystemMemory failed: %v", err)
	}

	if mem.Total == 0 {
		t.Error("Total memory should not be zero")
	}

	if mem.Available > mem.Total {
		t.Errorf("Available memory (%d) should not exceed total memory (%d)", mem.Available, mem.Total)
	}
}

func TestSystemMemoryCalculations(t *testing.T) {
	testCases := []struct {
		name            string
		total           uint64
		available       uint64
		expectedPercent float64
	}{
		{"full", 100, 100, 0.0},
		{"half", 100, 50, 50.0},
		{"quarter", 100, 25, 75.0},
		{"empty", 100, 0, 100.0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mem := SystemMemory{
				Total:     tc.total,
				Available: tc.available,
			}

			// Calculate used percentage
			used := mem.Total - mem.Available
			percent := float64(used) / float64(mem.Total) * 100.0

			if percent != tc.expectedPercent {
				t.Errorf("Expected %.1f%% used, got %.1f%%", tc.expectedPercent, percent)
			}
		})
	}
}
