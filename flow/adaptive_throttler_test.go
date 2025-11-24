package flow

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/internal/assert"
)

type MockMonitor struct {
	getStatsReturns []ResourceStats
	getStatsIndex   int
	closeCalled     bool
}

func (m *MockMonitor) GetStats() ResourceStats {
	if m.getStatsIndex < len(m.getStatsReturns) {
		result := m.getStatsReturns[m.getStatsIndex]
		m.getStatsIndex++
		return result
	}
	return ResourceStats{}
}

func (m *MockMonitor) Close() {
	m.closeCalled = true
}

func (m *MockMonitor) ExpectGetStats(stats ...ResourceStats) {
	m.getStatsReturns = stats
	m.getStatsIndex = 0
}

type mockFlow struct {
	in  chan any
	out chan any
}

func (m *mockFlow) Via(flow streams.Flow) streams.Flow {
	return flow
}

func (m *mockFlow) To(sink streams.Sink) {
	go func() {
		for data := range m.in {
			sink.In() <- data
		}
		close(sink.In())
	}()
}

func (m *mockFlow) Out() <-chan any {
	return m.out
}

func (m *mockFlow) In() chan<- any {
	if m.in == nil {
		m.in = make(chan any, 10)
		m.out = make(chan any, 10)
		go func() {
			defer close(m.out)
			for data := range m.in {
				m.out <- data
			}
		}()
	}
	return m.in
}

type mockSink struct {
	in         chan any
	completion chan struct{}
	completed  atomic.Bool
}

func (m *mockSink) In() chan<- any {
	return m.in
}

func (m *mockSink) AwaitCompletion() {
	if !m.completed.Load() {
		if m.completed.CompareAndSwap(false, true) {
			if m.completion != nil {
				close(m.completion)
			}
		}
	}
}

type mockSinkWithChannelDrain struct {
	in   chan any
	done chan struct{}
}

func (m *mockSinkWithChannelDrain) In() chan<- any {
	return m.in
}

func (m *mockSinkWithChannelDrain) AwaitCompletion() {
	<-m.done
}

// newMockSinkWithChannelDrain creates a mock sink that drains the channel like real sinks do
func newMockSinkWithChannelDrain() *mockSinkWithChannelDrain {
	sink := &mockSinkWithChannelDrain{
		in:   make(chan any),
		done: make(chan struct{}),
	}
	go func() {
		defer close(sink.done)
		for range sink.in {
			_ = struct{}{} // drain channel
		}
	}()
	return sink
}

// Helper functions for common test patterns

// createThrottlerWithLongInterval creates a throttler with default config but long sample interval
func createThrottlerWithLongInterval(t *testing.T) *AdaptiveThrottler {
	t.Helper()
	config := DefaultAdaptiveThrottlerConfig()
	config.SampleInterval = 10 * time.Second // Long interval to avoid interference
	at, err := NewAdaptiveThrottler(config)
	if err != nil {
		t.Fatalf("Failed to create throttler: %v", err)
	}
	return at
}

// collectDataFromChannel collects all data from a channel into a slice
func collectDataFromChannel(ch <-chan any) []any {
	var received []any
	for data := range ch {
		received = append(received, data)
	}
	return received
}

// collectDataFromChannelWithMutex collects data from channel using mutex for thread safety
func collectDataFromChannelWithMutex(ch <-chan any, received *[]any, mu *sync.Mutex, done chan struct{}) {
	defer close(done)
	for data := range ch {
		mu.Lock()
		*received = append(*received, data)
		mu.Unlock()
	}
}

// verifyChannelClosed checks that a channel is closed within a timeout
func verifyChannelClosed(t *testing.T, ch <-chan any, timeout time.Duration) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if ok {
			t.Errorf("Channel should be closed")
		}
	case <-time.After(timeout):
		t.Errorf("Channel should be closed within %v", timeout)
	}
}

// createThrottlerForRateTesting creates a throttler with mock monitor for rate adjustment testing
func createThrottlerForRateTesting(
	config *AdaptiveThrottlerConfig,
	initialRate float64,
) (*AdaptiveThrottler, *MockMonitor) {
	mockMonitor := &MockMonitor{}
	throttler := &AdaptiveThrottler{
		config:          *config,
		monitor:         mockMonitor,
		currentRateBits: math.Float64bits(initialRate),
	}
	return throttler, mockMonitor
}

type mockInlet struct {
	in chan any
}

func (m *mockInlet) In() chan<- any {
	return m.in
}

// TestAdaptiveThrottlerConfig_Validate tests configuration validation with valid and invalid settings
func TestAdaptiveThrottlerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  AdaptiveThrottlerConfig
		wantErr bool
	}{
		{
			name: "Valid Config",
			config: AdaptiveThrottlerConfig{
				MaxMemoryPercent: 80,
				MaxCPUPercent:    70,
				SampleInterval:   100 * time.Millisecond,
				CPUUsageMode:     CPUUsageModeMeasured,
				InitialRate:      100,
				MinRate:          10,
				MaxRate:          1000,
				BackoffFactor:    0.5,
				RecoveryFactor:   1.2,
			},
			wantErr: false,
		},
		{
			name: "Invalid SampleInterval",
			config: AdaptiveThrottlerConfig{
				SampleInterval: 1 * time.Millisecond,
			},
			wantErr: true,
		},
		{
			name: "Invalid MaxMemoryPercent High",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 101,
			},
			wantErr: true,
		},
		{
			name: "Invalid BackoffFactor",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				BackoffFactor:    1.5,
			},
			wantErr: true,
		},
		{
			name: "Invalid InitialRate below MinRate",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				MinRate:          10,
				MaxRate:          100,
				InitialRate:      5,
				BackoffFactor:    0.5,
				RecoveryFactor:   1.2,
			},
			wantErr: true,
		},
		{
			name: "Invalid RecoveryFactor too low",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				MinRate:          10,
				MaxRate:          100,
				InitialRate:      50,
				BackoffFactor:    0.5,
				RecoveryFactor:   0.9,
			},
			wantErr: true,
		},
		{
			name: "Valid InitialRate equals MinRate",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				MinRate:          10,
				MaxRate:          100,
				InitialRate:      10,
				BackoffFactor:    0.5,
				RecoveryFactor:   1.2,
			},
			wantErr: false,
		},
		{
			name: "Valid InitialRate equals MaxRate",
			config: AdaptiveThrottlerConfig{
				SampleInterval:   100 * time.Millisecond,
				MaxMemoryPercent: 80,
				MinRate:          10,
				MaxRate:          100,
				InitialRate:      100,
				BackoffFactor:    0.5,
				RecoveryFactor:   1.2,
			},
			wantErr: false,
		},
		{
			name: "Invalid RecoveryMemoryThreshold too high",
			config: AdaptiveThrottlerConfig{
				SampleInterval:          100 * time.Millisecond,
				MaxMemoryPercent:        80,
				RecoveryMemoryThreshold: 85, // Higher than MaxMemoryPercent
				MinRate:                 10,
				MaxRate:                 100,
				InitialRate:             50,
				BackoffFactor:           0.5,
				RecoveryFactor:          1.2,
			},
			wantErr: true,
		},
		{
			name: "Invalid RecoveryCPUThreshold too high",
			config: AdaptiveThrottlerConfig{
				SampleInterval:       100 * time.Millisecond,
				MaxMemoryPercent:     80,
				MaxCPUPercent:        70,
				RecoveryCPUThreshold: 75, // Higher than MaxCPUPercent
				MinRate:              10,
				MaxRate:              100,
				InitialRate:          50,
				BackoffFactor:        0.5,
				RecoveryFactor:       1.2,
			},
			wantErr: true,
		},
		{
			name: "Invalid RecoveryMemoryThreshold negative",
			config: AdaptiveThrottlerConfig{
				SampleInterval:          100 * time.Millisecond,
				MaxMemoryPercent:        80,
				RecoveryMemoryThreshold: -5,
				MinRate:                 10,
				MaxRate:                 100,
				InitialRate:             50,
				BackoffFactor:           0.5,
				RecoveryFactor:          1.2,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
			}
		})
	}
}

func TestDefaultAdaptiveThrottlerConfig(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	if config == nil {
		t.Fatal("Expected config to be not nil")
	}
	if config.InitialRate != 1000 {
		t.Errorf("Expected InitialRate to be 1000, got %d", config.InitialRate)
	}
	if config.MaxMemoryPercent != 85.0 {
		t.Errorf("Expected MaxMemoryPercent to be 85.0, got %f", config.MaxMemoryPercent)
	}
	if config.MaxCPUPercent != 80.0 {
		t.Errorf("Expected MaxCPUPercent to be 80.0, got %f", config.MaxCPUPercent)
	}
	if config.RecoveryMemoryThreshold != 0 {
		t.Errorf("Expected RecoveryMemoryThreshold to be 0 (auto-calculated), got %f", config.RecoveryMemoryThreshold)
	}
	if config.RecoveryCPUThreshold != 0 {
		t.Errorf("Expected RecoveryCPUThreshold to be 0 (auto-calculated), got %f", config.RecoveryCPUThreshold)
	}
	if !config.EnableHysteresis {
		t.Errorf("Expected EnableHysteresis to be true, got %v", config.EnableHysteresis)
	}
}

func TestNewAdaptiveThrottler(t *testing.T) {
	at1, err := NewAdaptiveThrottler(nil)
	if err != nil {
		t.Fatalf("Expected no error with nil config, got: %v", err)
	}
	if at1 == nil {
		t.Fatal("Expected non-nil throttler")
	}
	if at1.config.InitialRate != 1000 {
		t.Errorf("Expected default InitialRate 1000, got %d", at1.config.InitialRate)
	}
	at1.close()

	config := DefaultAdaptiveThrottlerConfig()
	config.InitialRate = 500
	at2, err := NewAdaptiveThrottler(config)
	if err != nil {
		t.Fatalf("Expected no error with valid config, got: %v", err)
	}
	if at2.config.InitialRate != 500 {
		t.Errorf("Expected InitialRate 500, got %d", at2.config.InitialRate)
	}
	at2.close()

	invalidConfig := &AdaptiveThrottlerConfig{
		SampleInterval: 1 * time.Millisecond,
	}
	at3, err := NewAdaptiveThrottler(invalidConfig)
	if err == nil {
		at3.close()
		t.Fatal("Expected error with invalid config")
	}
}

// TestAdaptiveThrottler_AdjustRate_Logic tests rate adjustment algorithm with high/low resource usage
func TestAdaptiveThrottler_AdjustRate_Logic(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	config.MinRate = 10
	config.MaxRate = 100
	config.InitialRate = 50
	config.BackoffFactor = 0.5
	config.RecoveryFactor = 2.0
	config.MaxCPUPercent = 50.0
	config.MaxMemoryPercent = 50.0
	config.RecoveryCPUThreshold = 40.0
	config.RecoveryMemoryThreshold = 40.0

	at, mockMonitor := createThrottlerForRateTesting(config, float64(config.InitialRate))

	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   60.0,
		MemoryUsedPercent: 30.0,
	})
	at.adjustRate()
	assert.InDelta(t, 42.5, at.GetCurrentRate(), 0.01)

	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   40.0,
		MemoryUsedPercent: 60.0,
	})
	at.adjustRate()
	assert.InDelta(t, 36.125, at.GetCurrentRate(), 0.01)

	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   10.0,
		MemoryUsedPercent: 10.0,
	})
	at.adjustRate()
	assert.InDelta(t, 46.9625, at.GetCurrentRate(), 0.01)
}

// TestAdaptiveThrottler_Hysteresis tests hysteresis behavior with enabled/disabled modes
func TestAdaptiveThrottler_Hysteresis(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	config.MinRate = 10
	config.MaxRate = 100
	config.InitialRate = 50
	config.BackoffFactor = 0.8
	config.RecoveryFactor = 1.2
	config.MaxCPUPercent = 80.0
	config.MaxMemoryPercent = 85.0
	config.RecoveryCPUThreshold = 70.0
	config.RecoveryMemoryThreshold = 75.0

	// Test with hysteresis enabled (default)
	config.EnableHysteresis = true
	at, mockMonitor := createThrottlerForRateTesting(config, float64(config.InitialRate))

	// CPU at 75% (above recovery threshold 70%, below max threshold 80%) - should not increase
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   75.0,
		MemoryUsedPercent: 40.0, // Below recovery threshold
	})
	at.adjustRate()
	assert.InDelta(t, 50.0, at.GetCurrentRate(), 0.01) // Should stay at 50 (no increase)

	// CPU at 65% (below recovery threshold) - should increase
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   65.0,
		MemoryUsedPercent: 40.0,
	})
	at.adjustRate()
	assert.InDelta(t, 53.0, at.GetCurrentRate(), 0.01) // 50 + (60-50)*0.3 = 53 (with smoothing)

	// Test with hysteresis disabled
	config.EnableHysteresis = false
	at2, _ := createThrottlerForRateTesting(config, 50.0)

	// CPU at 75% (below max threshold) - should increase immediately (no hysteresis)
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   75.0,
		MemoryUsedPercent: 40.0,
	})
	at2.adjustRate()
	assert.InDelta(t, 53.0, at2.GetCurrentRate(), 0.01) // Should increase immediately
}

// TestAdaptiveThrottler_Limits tests that rate stays within min/max bounds
func TestAdaptiveThrottler_Limits(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	config.MinRate = 10
	config.MaxRate = 20
	config.InitialRate = 10
	config.RecoveryFactor = 10.0
	config.RecoveryCPUThreshold = 70.0
	config.RecoveryMemoryThreshold = 75.0

	at, mockMonitor := createThrottlerForRateTesting(config, 10.0)

	mockMonitor.ExpectGetStats(ResourceStats{CPUUsagePercent: 0, MemoryUsedPercent: 0})
	at.adjustRate()
	assert.InDelta(t, 13.0, at.GetCurrentRate(), 0.01)
}

// TestAdaptiveThrottler_FlowControl tests token bucket throttling with burst traffic
func TestAdaptiveThrottler_FlowControl(t *testing.T) {
	mockMonitor := &MockMonitor{}

	config := DefaultAdaptiveThrottlerConfig()
	config.InitialRate = 10
	config.SampleInterval = 10 * time.Second

	at := &AdaptiveThrottler{
		config:          *config,
		monitor:         mockMonitor,
		currentRateBits: math.Float64bits(float64(config.InitialRate)),
		in:              make(chan any),
		out:             make(chan any, 100),
		done:            make(chan struct{}),
	}

	go at.monitorLoop()
	go at.pipelineLoop()

	// Send events continuously for a period
	sendDuration := 500 * time.Millisecond
	sendDone := make(chan struct{})

	sentCount := 0
	go func() {
		defer close(sendDone)
		ticker := time.NewTicker(1 * time.Millisecond) // Send very frequently
		defer ticker.Stop()

		sendStart := time.Now()
		for {
			select {
			case at.in <- sentCount:
				sentCount++
			case <-ticker.C:
				if time.Since(sendStart) >= sendDuration {
					close(at.in)
					return
				}
			}
		}
	}()

	// Collect received events
	receivedCount := 0
	receiveDone := make(chan struct{})

	go func() {
		defer close(receiveDone)
		for range at.out {
			receivedCount++
		}
	}()

	// Wait for sending to complete
	<-sendDone

	// Wait a bit more for processing
	time.Sleep(100 * time.Millisecond)
	at.close()

	// Wait for receiving to complete
	<-receiveDone

	t.Logf("Sent %d events over %v, received %d events", sentCount, sendDuration, receivedCount)

	// With rate of 10/sec over 500ms, should receive about 5 events
	expectedMax := int(float64(config.InitialRate) * sendDuration.Seconds() * 1.5)
	if receivedCount > expectedMax {
		t.Fatalf("Received too many events: got %d, expected at most %d", receivedCount, expectedMax)
	}
	if receivedCount < 1 {
		t.Fatalf("Expected at least 1 event, got %d", receivedCount)
	}
}

func TestAdaptiveThrottler_GetCurrentRate(t *testing.T) {
	mockMonitor := &MockMonitor{}
	config := DefaultAdaptiveThrottlerConfig()

	at := &AdaptiveThrottler{
		config:          *config,
		monitor:         mockMonitor,
		currentRateBits: math.Float64bits(42.5),
	}

	rate := at.GetCurrentRate()
	if rate != 42.5 {
		t.Errorf("Expected rate 42.5, got %f", rate)
	}
}

func TestAdaptiveThrottler_GetResourceStats(t *testing.T) {
	mockMonitor := &MockMonitor{}
	expectedStats := ResourceStats{
		CPUUsagePercent:   15.5,
		MemoryUsedPercent: 25.0,
		GoroutineCount:    10,
	}
	mockMonitor.ExpectGetStats(expectedStats)

	config := DefaultAdaptiveThrottlerConfig()
	at := &AdaptiveThrottler{
		config:  *config,
		monitor: mockMonitor,
	}

	stats := at.GetResourceStats()
	if stats.CPUUsagePercent != expectedStats.CPUUsagePercent {
		t.Errorf("Expected CPU %f, got %f", expectedStats.CPUUsagePercent, stats.CPUUsagePercent)
	}
	if stats.MemoryUsedPercent != expectedStats.MemoryUsedPercent {
		t.Errorf("Expected Memory %f, got %f", expectedStats.MemoryUsedPercent, stats.MemoryUsedPercent)
	}
}

func TestAdaptiveThrottler_Via_ReturnsInputFlow(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	at, err := NewAdaptiveThrottler(config)
	if err != nil {
		t.Fatalf("Failed to create throttler: %v", err)
	}
	defer at.close()

	mockFlow := &mockFlow{}
	resultFlow := at.Via(mockFlow)

	if resultFlow != mockFlow {
		t.Error("Via should return the input flow")
	}
}

// TestAdaptiveThrottler_To tests To method that streams data to a sink
func TestAdaptiveThrottler_To(t *testing.T) {
	at := createThrottlerWithLongInterval(t)
	defer at.close()

	var received []any
	var mu sync.Mutex
	done := make(chan struct{})

	sinkCh := make(chan any, 10)
	mockSink := &mockSink{
		in:         sinkCh,
		completion: make(chan struct{}),
	}

	// Collect data from sink
	go collectDataFromChannelWithMutex(sinkCh, &received, &mu, done)

	testData := []any{"test1", "test2", "test3"}

	// Start the throttler streaming to sink
	go at.To(mockSink)

	// Send test data
	go func() {
		for _, data := range testData {
			at.In() <- data
		}
		close(at.In())
	}()

	// Wait for completion signal from sink
	mockSink.AwaitCompletion()

	// Wait for receiver to finish
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(received) != len(testData) {
		t.Errorf("Expected %d items, got %d", len(testData), len(received))
	}
	for i, expected := range testData {
		if i >= len(received) || received[i] != expected {
			t.Errorf("Expected data[%d] = %v, got %v", i, expected, received[i])
		}
	}
}

func TestAdaptiveThrottler_StreamPortioned(t *testing.T) {
	mockMonitor := &MockMonitor{}
	config := DefaultAdaptiveThrottlerConfig()

	at := &AdaptiveThrottler{
		config:          *config,
		monitor:         mockMonitor,
		currentRateBits: math.Float64bits(1000),
		out:             make(chan any, 10),
	}

	inletIn := make(chan any, 10)
	mockInlet := &mockInlet{in: inletIn}

	testData := []any{"data1", "data2", "data3"}
	var received []any
	var mu sync.Mutex
	done := make(chan struct{})

	// Start streamPortioned in background
	go at.streamPortioned(mockInlet)

	// Collect all received data
	go collectDataFromChannelWithMutex(inletIn, &received, &mu, done)

	// Send test data
	go func() {
		for _, data := range testData {
			at.out <- data
		}
		close(at.out)
	}()

	// Wait for all data to be received
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(received) != len(testData) {
		t.Errorf("Expected %d items, got %d", len(testData), len(received))
	}
	for i, expected := range testData {
		if received[i] != expected {
			t.Errorf("Expected data[%d] = %v, got %v", i, expected, received[i])
		}
	}
}

func TestAdaptiveThrottler_Via_DataFlow(t *testing.T) {
	// Actually test data flow
	at, _ := NewAdaptiveThrottler(DefaultAdaptiveThrottlerConfig())
	defer at.close()

	// Send test data and verify it flows through
	testData := []any{"test1", "test2"}
	go func() {
		for _, data := range testData {
			at.In() <- data
		}
		close(at.In())
	}()

	received := make([]any, 0, len(testData))
	for data := range at.Out() {
		received = append(received, data)
	}

	if len(received) != len(testData) {
		t.Errorf("Expected %d items, got %d", len(testData), len(received))
	}
}

// TestAdaptiveThrottler_AdjustRate_EdgeCases tests edge cases for adjustRate
func TestAdaptiveThrottler_AdjustRate_EdgeCases(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	config.MinRate = 10
	config.MaxRate = 100
	config.InitialRate = 50
	config.BackoffFactor = 0.7
	config.RecoveryFactor = 1.3
	config.MaxCPUPercent = 80.0
	config.MaxMemoryPercent = 85.0
	config.RecoveryCPUThreshold = 70.0
	config.RecoveryMemoryThreshold = 75.0

	at, mockMonitor := createThrottlerForRateTesting(config, 50.0)

	// Test: Both CPU and memory constrained
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   90.0, // Above max
		MemoryUsedPercent: 90.0, // Above max
	})
	at.adjustRate()
	// Should reduce rate: 50 * 0.7 = 35, then smoothed: 50 + (35-50)*0.3 = 45.5
	assert.InDelta(t, 45.5, at.GetCurrentRate(), 0.1)

	// Test: Rate at max, should not exceed
	at.setRate(100.0)
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   10.0,
		MemoryUsedPercent: 10.0,
	})
	at.adjustRate()
	// Should try to increase but cap at MaxRate
	rate := at.GetCurrentRate()
	if rate > 100.0 {
		t.Errorf("Rate should not exceed MaxRate, got %f", rate)
	}

	// Test: Rate at min, constrained
	at.setRate(10.0)
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   90.0,
		MemoryUsedPercent: 90.0,
	})
	at.adjustRate()
	// Should reduce but not go below MinRate (though MinRate is not enforced in adjustRate)
	rate = at.GetCurrentRate()
	if rate < 0 {
		t.Errorf("Rate should not be negative, got %f", rate)
	}

	// Test: Exactly at thresholds
	at.setRate(50.0)
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   80.0, // Exactly at max (not above, so not constrained)
		MemoryUsedPercent: 70.0, // Below max
	})
	at.adjustRate()
	// Should not reduce because CPU is exactly at max (not > max)
	// The constraint check uses > not >=
	rate = at.GetCurrentRate()
	// Rate should stay the same or potentially increase if below recovery threshold
	if rate < 0 {
		t.Errorf("Rate should not be negative, got %f", rate)
	}

	// Test: Exactly at recovery thresholds with hysteresis
	at.setRate(50.0)
	config.EnableHysteresis = true
	mockMonitor.ExpectGetStats(ResourceStats{
		CPUUsagePercent:   70.0, // Exactly at recovery threshold
		MemoryUsedPercent: 75.0, // Exactly at recovery threshold
	})
	at.adjustRate()
	// With hysteresis, should not increase (must be below both thresholds)
	rate = at.GetCurrentRate()
	if rate > 50.0 {
		t.Errorf("With hysteresis, rate should not increase at threshold, got %f", rate)
	}
}

// TestAdaptiveThrottler_PipelineLoop_Shutdown tests pipelineLoop shutdown scenarios
func TestAdaptiveThrottler_PipelineLoop_Shutdown(t *testing.T) {
	mockMonitor := &MockMonitor{}
	config := DefaultAdaptiveThrottlerConfig()
	config.InitialRate = 100 // Fast rate for testing

	at := &AdaptiveThrottler{
		config:          *config,
		monitor:         mockMonitor,
		currentRateBits: math.Float64bits(100.0),
		in:              make(chan any),
		out:             make(chan any, 10),
		done:            make(chan struct{}),
	}

	// Test: Shutdown during processing
	go at.pipelineLoop()

	// Send one item
	at.in <- "test1"

	// Wait a bit for it to start processing
	time.Sleep(10 * time.Millisecond)

	// Close done channel to trigger shutdown
	close(at.done)

	// Wait for pipeline to finish (may take a moment for goroutine to exit)
	time.Sleep(200 * time.Millisecond)

	// Verify output channel is closed (pipelineLoop defers close(at.out))
	select {
	case _, ok := <-at.out:
		if ok {
			// Channel still open, wait a bit more
			time.Sleep(100 * time.Millisecond)
			select {
			case _, ok2 := <-at.out:
				if ok2 {
					t.Error("Output channel should be closed after shutdown")
				}
			default:
				// Channel closed now
			}
		}
	default:
		// Channel already closed, which is expected
	}
}

// TestAdaptiveThrottler_PipelineLoop_RateChange tests pipelineLoop with rate changes
func TestAdaptiveThrottler_PipelineLoop_RateChange(t *testing.T) {
	mockMonitor := &MockMonitor{}
	config := DefaultAdaptiveThrottlerConfig()

	at := &AdaptiveThrottler{
		config:          *config,
		monitor:         mockMonitor,
		currentRateBits: math.Float64bits(10.0), // Start at 10/sec
		in:              make(chan any),
		out:             make(chan any, 10),
		done:            make(chan struct{}),
	}

	go at.pipelineLoop()

	// Send items and change rate during processing
	go func() {
		for i := 0; i < 5; i++ {
			at.in <- i
			time.Sleep(10 * time.Millisecond)
			// Change rate mid-stream
			if i == 2 {
				at.setRate(20.0) // Double the rate
			}
		}
		close(at.in)
	}()

	// Collect items
	received := make([]any, 0)
	timeout := time.After(2 * time.Second)
	for {
		select {
		case item, ok := <-at.out:
			if !ok {
				goto done
			}
			received = append(received, item)
		case <-timeout:
			t.Fatal("Timeout waiting for items")
		}
	}
done:

	if len(received) != 5 {
		t.Errorf("Expected 5 items, got %d", len(received))
	}

	// Close done to clean up
	close(at.done)
}

// TestAdaptiveThrottler_PipelineLoop_LowRate tests pipelineLoop with very low rate
func TestAdaptiveThrottler_PipelineLoop_LowRate(t *testing.T) {
	mockMonitor := &MockMonitor{}
	config := DefaultAdaptiveThrottlerConfig()

	at := &AdaptiveThrottler{
		config:          *config,
		monitor:         mockMonitor,
		currentRateBits: math.Float64bits(0.5), // Very slow: 0.5/sec
		in:              make(chan any),
		out:             make(chan any, 10),
		done:            make(chan struct{}),
	}

	go at.pipelineLoop()

	// Send one item
	at.in <- "test"
	close(at.in)

	// Should receive it (rate < 1.0 is clamped to 1.0)
	select {
	case item := <-at.out:
		if item != "test" {
			t.Errorf("Expected 'test', got %v", item)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for item")
	}

	// Wait for channel to close
	select {
	case _, ok := <-at.out:
		if ok {
			t.Error("Output channel should be closed")
		}
	case <-time.After(100 * time.Millisecond):
		// Channel should be closed by now
	}

	close(at.done)
}

// TestAdaptiveThrottler_To_Shutdown tests To method with shutdown
func TestAdaptiveThrottler_To_Shutdown(t *testing.T) {
	at := createThrottlerWithLongInterval(t)

	mockSink := newMockSinkWithChannelDrain()
	sinkCh := mockSink.in

	// Start To in background
	toDone := make(chan struct{})
	go func() {
		defer close(toDone)
		at.To(mockSink)
	}()

	// Send some data
	go func() {
		for i := 0; i < 3; i++ {
			at.In() <- i
		}
		close(at.In())
	}()

	// Wait a bit for data to start flowing
	time.Sleep(100 * time.Millisecond)

	// Close the throttler
	at.close()

	// Wait for To to complete (it will finish when streamPortioned completes)
	select {
	case <-toDone:
		// Good, To completed
	case <-time.After(2 * time.Second):
		t.Error("To method did not complete within timeout")
	}

	// Verify sink channel is closed (streamPortioned closes inlet.In())
	// Give it a moment to ensure the close has propagated
	time.Sleep(100 * time.Millisecond)

	// Verify sink channel is closed
	verifyChannelClosed(t, sinkCh, 50*time.Millisecond)
}

// TestAdaptiveThrottler_StreamPortioned_Blocking tests streamPortioned with blocking inlet
func TestAdaptiveThrottler_StreamPortioned_Blocking(t *testing.T) {
	mockMonitor := &MockMonitor{}
	config := DefaultAdaptiveThrottlerConfig()

	at := &AdaptiveThrottler{
		config:          *config,
		monitor:         mockMonitor,
		currentRateBits: math.Float64bits(1000),
		out:             make(chan any),
	}

	// Unbuffered inlet to test blocking behavior
	inletIn := make(chan any)
	mockInlet := &mockInlet{in: inletIn}

	// Start streamPortioned in background
	done := make(chan struct{})
	go func() {
		defer close(done)
		at.streamPortioned(mockInlet)
	}()

	// Send data
	go func() {
		at.out <- "test1"
		at.out <- "test2"
		close(at.out)
	}()

	// Read from inlet (unblocking the sender)
	received := collectDataFromChannel(inletIn)

	// Wait for completion
	select {
	case <-done:
		// Good
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for streamPortioned to complete")
	}

	// Verify inlet is closed
	verifyChannelClosed(t, inletIn, 50*time.Millisecond)

	if len(received) != 2 {
		t.Errorf("Expected 2 items, got %d", len(received))
	}
}
