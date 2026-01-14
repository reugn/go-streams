package sysmonitor

import (
	"io/fs"
	"testing"
	"time"

	"github.com/reugn/go-streams/internal/testutil"
)

// MockFS for the factory test
type factoryMockFS struct{}

func (f *factoryMockFS) ReadFile(_ string) ([]byte, error) { return nil, nil }
func (f *factoryMockFS) Open(_ string) (fs.File, error)    { return nil, nil }

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

// Verify that testutil.MockFileSystem implements FileSystem
var _ FileSystem = (*testutil.MockFileSystem)(nil)

func TestNewCPUSampler(t *testing.T) {
	fs := &factoryMockFS{}

	sampler, err := NewCPUSampler(fs)
	if err != nil {
		t.Fatalf("NewCPUSampler returned error: %v", err)
	}

	if sampler == nil {
		t.Fatal("NewCPUSampler returned nil")
	}

	// Basic interface check
	sampler.Reset()
	_ = sampler.IsInitialized() // Ensure method exists and is callable
}

// TestProcessCPUSamplerInterface tests the ProcessCPUSampler interface
// using any available platform implementation
func TestProcessCPUSamplerInterface(t *testing.T) {
	// Create a mock filesystem for testing
	fs := &factoryMockFS{}

	sampler, err := NewCPUSampler(fs)
	if err != nil {
		t.Skipf("CPU sampler not available on this platform: %v", err)
	}

	// Test 1: Interface compliance - verify all methods exist and are callable
	if sampler == nil {
		t.Fatal("NewCPUSampler returned nil sampler")
	}

	// Test 2: Initial state - should not be initialized
	if sampler.IsInitialized() {
		t.Error("New sampler should not be initialized")
	}

	// Test 3: First sample - should initialize and return 0.0
	firstSample := sampler.Sample(time.Second)
	if !sampler.IsInitialized() {
		t.Error("Sampler should be initialized after first sample")
	}
	if firstSample != 0.0 {
		t.Errorf("First sample should return 0.0, got %f", firstSample)
	}

	// Test 4: Subsequent samples - should return valid CPU values
	secondSample := sampler.Sample(time.Second)
	if secondSample < 0.0 || secondSample > 100.0 {
		t.Errorf("CPU sample should be between 0-100, got %f", secondSample)
	}

	// Test 5: Reset functionality
	sampler.Reset()
	if sampler.IsInitialized() {
		t.Error("Sampler should not be initialized after reset")
	}

	// Test 6: Sample after reset - should behave like first sample
	resetSample := sampler.Sample(time.Second)
	if !sampler.IsInitialized() {
		t.Error("Sampler should be initialized after sample following reset")
	}
	if resetSample != 0.0 {
		t.Errorf("Sample after reset should return 0.0, got %f", resetSample)
	}

	// Test 7: Rapid sampling - should return last known value if delta too small
	rapidSample := sampler.Sample(time.Millisecond)
	// This should return the last known value (resetSample) since delta is too small
	if rapidSample != resetSample {
		t.Logf("Rapid sample returned %f, expected last value %f", rapidSample, resetSample)
		// This is informational - behavior may vary by implementation
	}
}

// TestProcessMemoryReaderInterface tests the ProcessMemoryReader interface
// using any available platform implementation
func TestProcessMemoryReaderInterface(t *testing.T) {
	// Create a mock filesystem for testing
	fs := &factoryMockFS{}

	reader := NewProcessMemoryReader(fs)
	if reader == nil {
		t.Fatal("NewProcessMemoryReader returned nil reader")
	}

	// Test 1: Basic sampling functionality
	mem, err := reader.Sample()
	if err != nil {
		t.Skipf("Memory reader not functional on this platform: %v", err)
	}

	// Test 2: Memory values should be reasonable
	if mem.Total == 0 {
		t.Error("Total memory should not be zero")
	}
	if mem.Available > mem.Total {
		t.Errorf("Available memory (%d) should not exceed total memory (%d)", mem.Available, mem.Total)
	}

	// Test 3: Multiple samples should be consistent
	mem2, err := reader.Sample()
	if err != nil {
		t.Errorf("Second sample failed: %v", err)
	}

	// Total memory should be consistent
	if mem2.Total != mem.Total {
		t.Errorf("Total memory changed between samples: %d -> %d", mem.Total, mem2.Total)
	}

	// Available memory should be reasonable (within 10% of previous value)
	upperBound := uint64(float64(mem.Available) * 1.1)
	lowerBound := uint64(float64(mem.Available) * 0.9)
	if mem2.Available > upperBound || mem2.Available < lowerBound {
		t.Logf("Available memory changed significantly: %d -> %d", mem.Available, mem2.Available)
		// This is informational as memory usage can fluctuate
	}
}

// TestSamplerErrorHandling tests error conditions that work across platforms
func TestSamplerErrorHandling(t *testing.T) {
	// Test with mock filesystem that can simulate errors
	mockFS := &testutil.MockFileSystem{
		OpenErrs: map[string]error{
			"/proc/self/stat": &testError{msg: "mock stat error"},
			"/proc/meminfo":   &testError{msg: "mock meminfo error"},
			"/proc/self/auxv": &testError{msg: "mock auxv error"},
		},
	}

	// Test CPU sampler with error conditions
	cpuSampler, err := NewCPUSampler(mockFS)
	if err == nil {
		t.Log("CPU sampler creation succeeded despite mock errors - this may be expected on some platforms")
		// If it succeeds, test that it still functions
		if cpuSampler != nil {
			sample := cpuSampler.Sample(time.Second)
			if sample < 0.0 {
				t.Errorf("CPU sample should not be negative: %f", sample)
			}
		}
	} else {
		t.Logf("CPU sampler creation failed as expected: %v", err)
	}

	// Test memory reader with error conditions
	memReader := NewProcessMemoryReader(mockFS)
	if memReader != nil {
		_, err := memReader.Sample()
		// Error is expected but not guaranteed on all platforms
		if err != nil {
			t.Logf("Memory reader returned expected error: %v", err)
		} else {
			t.Log("Memory reader succeeded despite mock errors - this may be expected on some platforms")
		}
	}
}
