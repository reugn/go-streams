package sysmonitor

import (
	"reflect"
	"testing"
	"time"
	"unsafe"
)

// setUnexportedField sets an unexported field using unsafe reflection.
func setUnexportedField(t *testing.T, field reflect.Value, value interface{}) {
	t.Helper()
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).
		Elem().
		Set(reflect.ValueOf(value))
}

// setTestState sets internal sampler state for testing.
func setTestState(t *testing.T, sampler ProcessCPUSampler, lastUTime, lastSTime float64, lastSample time.Time) {
	t.Helper()
	val := reflect.ValueOf(sampler).Elem()

	// Set lastUTime
	if field := val.FieldByName("lastUTime"); field.IsValid() {
		setUnexportedField(t, field, lastUTime)
	}

	// Set lastSTime
	if field := val.FieldByName("lastSTime"); field.IsValid() {
		setUnexportedField(t, field, lastSTime)
	}

	// Set lastSample
	if field := val.FieldByName("lastSample"); field.IsValid() {
		setUnexportedField(t, field, lastSample)
	}
}

func TestNewProcessSampler(t *testing.T) {
	sampler, err := NewProcessSampler()
	if err != nil {
		t.Fatalf("NewProcessSampler failed: %v", err)
	}
	if sampler == nil {
		t.Fatal("NewProcessSampler should not be nil")
	}

	// Test basic interface compliance
	_ = sampler.Sample(100 * time.Millisecond)
	sampler.Reset()
	_ = sampler.IsInitialized()
}

func TestProcessSampler_Initialization(t *testing.T) {
	sampler, err := NewProcessSampler()
	if err != nil {
		t.Fatalf("NewProcessSampler failed: %v", err)
	}
	if sampler == nil {
		t.Fatal("NewProcessSampler should not be nil")
	}

	// Initially not initialized
	if sampler.IsInitialized() {
		t.Error("ProcessSampler should not be initialized initially")
	}

	// After first sample, should be initialized
	sampler.Sample(100 * time.Millisecond)
	if !sampler.IsInitialized() {
		t.Error("ProcessSampler should be initialized after first sample")
	}

	// After reset, should not be initialized
	sampler.Reset()
	if sampler.IsInitialized() {
		t.Error("ProcessSampler should not be initialized after reset")
	}
}

func TestProcessSampler_Sample(t *testing.T) {
	sampler, err := NewProcessSampler()
	if err != nil {
		t.Fatalf("NewProcessSampler failed: %v", err)
	}

	percent := sampler.Sample(100 * time.Millisecond)
	if percent != 0.0 {
		t.Errorf("first sample should return 0.0, got %v", percent)
	}

	time.Sleep(10 * time.Millisecond)
	percent = sampler.Sample(10 * time.Millisecond)
	if percent < 0.0 || percent > 100.0 {
		t.Errorf("CPU percent should be between 0 and 100, got %v", percent)
	}

	sampler.Reset()
	if sampler.IsInitialized() {
		t.Error("sampler should not be initialized after reset")
	}
}

func TestProcessSampler_SampleEdgeCases(t *testing.T) {
	sampler, err := NewProcessSampler()
	if err != nil {
		t.Fatalf("NewProcessSampler failed: %v", err)
	}

	sampler.Sample(100 * time.Millisecond)
	_ = sampler.Sample(100 * time.Millisecond)
	time.Sleep(1 * time.Millisecond)
	percent2 := sampler.Sample(100 * time.Millisecond)
	if percent2 < 0.0 || percent2 > 100.0 {
		t.Errorf("CPU percent should be between 0 and 100, got %v", percent2)
	}

	time.Sleep(50 * time.Millisecond)
	percent3 := sampler.Sample(50 * time.Millisecond)
	if percent3 < 0.0 || percent3 > 100.0 {
		t.Errorf("CPU percent should be clamped to 0-100, got %v", percent3)
	}
}

// TestProcessSampler_SampleErrorHandling tests error handling.
func TestProcessSampler_SampleErrorHandling(t *testing.T) {
	sampler, err := NewProcessSampler()
	if err != nil {
		t.Fatalf("NewProcessSampler failed: %v", err)
	}

	initialPercent := sampler.Sample(100 * time.Millisecond)
	if initialPercent != 0.0 {
		t.Errorf("first sample should return 0.0, got %v", initialPercent)
	}

	time.Sleep(10 * time.Millisecond)
	validPercent := sampler.Sample(10 * time.Millisecond)
	if validPercent < 0.0 || validPercent > 100.0 {
		t.Errorf("CPU percent should be between 0 and 100, got %v", validPercent)
	}

	sampler.Reset()
}

// TestProcessSampler_SampleNumCPUEdgeCase tests numcpu <= 0 safety check.
func TestProcessSampler_SampleNumCPUEdgeCase(t *testing.T) {
	sampler, err := NewProcessSampler()
	if err != nil {
		t.Fatalf("NewProcessSampler failed: %v", err)
	}

	sampler.Sample(100 * time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	percent := sampler.Sample(10 * time.Millisecond)
	if percent < 0.0 || percent > 100.0 {
		t.Errorf("CPU percent should be between 0 and 100, got %v", percent)
	}
}

// TestProcessSampler_SampleNegativePercent tests clamping of negative CPU percent.
func TestProcessSampler_SampleNegativePercent(t *testing.T) {
	sampler, err := NewProcessSampler()
	if err != nil {
		t.Fatalf("NewProcessSampler failed: %v", err)
	}

	sampler.Sample(100 * time.Millisecond)
	setTestState(t, sampler, 100.0, 50.0, time.Now().Add(-1*time.Second))

	time.Sleep(10 * time.Millisecond)
	percent := sampler.Sample(100 * time.Millisecond)

	if percent < 0.0 {
		t.Errorf("CPU percent should be clamped to 0.0 when negative, got %v", percent)
	}
	if percent > 100.0 {
		t.Errorf("CPU percent should not exceed 100.0, got %v", percent)
	}
}

// TestProcessSampler_SampleHighPercent tests clamping of CPU percent > 100%.
func TestProcessSampler_SampleHighPercent(t *testing.T) {
	sampler, err := NewProcessSampler()
	if err != nil {
		t.Fatalf("NewProcessSampler failed: %v", err)
	}

	sampler.Sample(100 * time.Millisecond)
	setTestState(t, sampler, 0.0, 0.0, time.Now().Add(-1*time.Millisecond))
	time.Sleep(10 * time.Millisecond)
	setTestState(t, sampler, 0.0, 0.0, time.Now().Add(-1*time.Millisecond))
	percent := sampler.Sample(100 * time.Millisecond)

	if percent > 100.0 {
		t.Errorf("CPU percent should be clamped to 100.0 when > 100, got %v", percent)
	}
	if percent < 0.0 {
		t.Errorf("CPU percent should not be negative, got %v", percent)
	}
}

// TestProcessSampler_SampleZeroWallTime tests handling of zero or negative wall time.
func TestProcessSampler_SampleZeroWallTime(t *testing.T) {
	sampler, err := NewProcessSampler()
	if err != nil {
		t.Fatalf("NewProcessSampler failed: %v", err)
	}

	sampler.Sample(100 * time.Millisecond)
	setTestState(t, sampler, 0.0, 0.0, time.Now())
	percent := sampler.Sample(100 * time.Millisecond)

	if percent < 0.0 || percent > 100.0 {
		t.Errorf("CPU percent should be valid (0-100) or lastPercent, got %v", percent)
	}
}
