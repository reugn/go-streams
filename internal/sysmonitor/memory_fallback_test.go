//go:build !linux && !windows && (!darwin || !cgo)

package sysmonitor

import (
	"errors"
	"runtime"
	"strings"
	"testing"
)

func TestNewProcessMemoryReader(t *testing.T) {
	// Test the public API function
	reader := NewProcessMemoryReader(nil)

	// Should return a fallbackMemoryReader
	if reader == nil {
		t.Fatal("NewProcessMemoryReader returned nil")
	}
}

func TestFallbackMemoryReader(t *testing.T) {
	reader := newPlatformMemoryReader(nil).(*fallbackMemoryReader)

	_, err := reader.Sample()
	if err == nil {
		t.Fatal("Expected error from fallback memory reader")
	}

	expectedMsg := "memory monitoring not supported on this platform"
	if runtime.GOOS == "darwin" {
		expectedMsg = "memory monitoring on Darwin requires CGO_ENABLED=1"
	}

	if !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("Expected error message to contain %q, got %q", expectedMsg, err.Error())
	}
}

func TestFallbackMemoryReader_Darwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("This test is only relevant on Darwin")
	}

	reader := newPlatformMemoryReader(nil).(*fallbackMemoryReader)

	_, err := reader.Sample()
	if err == nil {
		t.Fatal("Expected error from fallback memory reader on Darwin")
	}

	expectedMsg := "memory monitoring on Darwin requires CGO_ENABLED=1"
	if !errors.Is(err, err) || !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("Expected error message to contain %q, got %q", expectedMsg, err.Error())
	}
}

func TestFallbackMemoryReader_OtherPlatforms(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("This test is only relevant on non-Darwin platforms")
	}

	reader := newPlatformMemoryReader(nil).(*fallbackMemoryReader)

	_, err := reader.Sample()
	if err == nil {
		t.Fatal("Expected error from fallback memory reader")
	}

	expectedMsg := "memory monitoring not supported on this platform"
	if !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("Expected error message to contain %q, got %q", expectedMsg, err.Error())
	}
}
