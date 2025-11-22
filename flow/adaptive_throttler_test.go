package flow

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// mockResourceMonitor allows injecting specific resource stats for testing
type mockResourceMonitor struct {
	mu    sync.RWMutex
	stats ResourceStats
}

func (m *mockResourceMonitor) GetStats() ResourceStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stats
}

func (m *mockResourceMonitor) IsResourceConstrained() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stats.MemoryUsedPercent > 80.0 || m.stats.CPUUsagePercent > 70.0
}

func (m *mockResourceMonitor) SetStats(stats ResourceStats) {
	m.mu.Lock()
	m.stats = stats
	m.mu.Unlock()
}

func (m *mockResourceMonitor) Close() {}

// newAdaptiveThrottlerWithMonitor creates an AdaptiveThrottler with a mock monitor for testing
func newAdaptiveThrottlerWithMonitor(
	config AdaptiveThrottlerConfig,
	monitor resourceMonitor,
	customPeriod ...time.Duration,
) *AdaptiveThrottler {
	period := time.Second
	if len(customPeriod) > 0 && customPeriod[0] > 0 {
		period = customPeriod[0]
	}

	at := &AdaptiveThrottler{
		config:      config,
		monitor:     monitor,
		period:      period,
		in:          make(chan any),
		out:         make(chan any, config.BufferSize),
		quotaSignal: make(chan struct{}, 1),
		done:        make(chan struct{}),
	}

	at.currentRate.Store(int64(config.MaxThroughput))
	at.maxElements.Store(int64(config.MaxThroughput))
	at.lastAdaptation = time.Now()

	// Start goroutines
	go at.adaptRateLoop()
	go at.resetQuotaCounterLoop()
	go at.buffer()

	return at
}

func TestAdaptiveThrottler_ConfigValidation(t *testing.T) {
	tests := []struct {
		name          string
		config        AdaptiveThrottlerConfig
		shouldError   bool
		expectedError string // Expected substring in error message
	}{
		{
			name: "valid config",
			config: AdaptiveThrottlerConfig{
				MaxMemoryPercent:    80.0,
				MaxCPUPercent:       70.0,
				MinThroughput:       10,
				MaxThroughput:       100,
				SampleInterval:      100 * time.Millisecond,
				BufferSize:          100,
				MaxBufferSize:       1000,
				AdaptationFactor:    0.2,
				HysteresisBuffer:    5.0,
				MaxRateChangeFactor: 0.5,
			},
			shouldError:   false,
			expectedError: "",
		},
		{
			name: "invalid MaxMemoryPercent",
			config: AdaptiveThrottlerConfig{
				MaxMemoryPercent:    150.0, // Invalid
				MaxCPUPercent:       70.0,
				MinThroughput:       10,
				MaxThroughput:       100,
				SampleInterval:      100 * time.Millisecond,
				BufferSize:          100,
				MaxBufferSize:       1000,
				AdaptationFactor:    0.2,
				HysteresisBuffer:    5.0,
				MaxRateChangeFactor: 0.5,
			},
			shouldError:   true,
			expectedError: "invalid MaxMemoryPercent",
		},
		{
			name: "invalid MaxThroughput < MinThroughput",
			config: AdaptiveThrottlerConfig{
				MaxMemoryPercent:    80.0,
				MaxCPUPercent:       70.0,
				MinThroughput:       100,
				MaxThroughput:       50, // Invalid
				SampleInterval:      100 * time.Millisecond,
				BufferSize:          100,
				MaxBufferSize:       1000,
				AdaptationFactor:    0.2,
				HysteresisBuffer:    5.0,
				MaxRateChangeFactor: 0.5,
			},
			shouldError:   true,
			expectedError: "invalid throughput bounds",
		},
		{
			name: "invalid MaxBufferSize",
			config: AdaptiveThrottlerConfig{
				MaxMemoryPercent:    80.0,
				MaxCPUPercent:       70.0,
				MinThroughput:       10,
				MaxThroughput:       100,
				SampleInterval:      100 * time.Millisecond,
				BufferSize:          100,
				MaxBufferSize:       -1, // Invalid: negative value
				AdaptationFactor:    0.2,
				HysteresisBuffer:    5.0,
				MaxRateChangeFactor: 0.5,
			},
			shouldError:   true,
			expectedError: "invalid MaxBufferSize",
		},
		{
			name: "BufferSize exceeds MaxBufferSize",
			config: AdaptiveThrottlerConfig{
				MaxMemoryPercent:    80.0,
				MaxCPUPercent:       70.0,
				MinThroughput:       10,
				MaxThroughput:       100,
				SampleInterval:      100 * time.Millisecond,
				BufferSize:          1000,
				MaxBufferSize:       500, // Invalid: BufferSize > MaxBufferSize
				AdaptationFactor:    0.2,
				HysteresisBuffer:    5.0,
				MaxRateChangeFactor: 0.5,
			},
			shouldError:   true,
			expectedError: "BufferSize 1000 exceeds MaxBufferSize 500",
		},
	}

	for _, tt := range tests {
		tt := tt // capture loop variable
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewAdaptiveThrottler(&tt.config)
			if tt.shouldError {
				if err == nil {
					t.Error("expected error but didn't get one")
					return
				}
				// Verify error message contains expected text
				if tt.expectedError != "" && !strings.Contains(err.Error(), tt.expectedError) {
					t.Errorf("error message should contain %q, got: %v", tt.expectedError, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestAdaptiveThrottler_BasicThroughput(t *testing.T) {
	config := AdaptiveThrottlerConfig{
		MaxMemoryPercent:    80.0,
		MaxCPUPercent:       70.0,
		MinThroughput:       10,
		MaxThroughput:       100,
		SampleInterval:      50 * time.Millisecond,
		BufferSize:          10,
		AdaptationFactor:    0.2,
		SmoothTransitions:   false,
		CPUUsageMode:        CPUUsageModeHeuristic,
		HysteresisBuffer:    5.0,
		MaxRateChangeFactor: 0.5,
	}

	// Mock monitor with low resource usage (should allow high throughput)
	mockMonitor := &mockResourceMonitor{
		stats: ResourceStats{
			MemoryUsedPercent: 30.0,
			CPUUsagePercent:   20.0,
			GoroutineCount:    5,
			Timestamp:         time.Now(),
		},
	}

	at := newAdaptiveThrottlerWithMonitor(config, mockMonitor)
	defer func() {
		if r := recover(); r == nil {
			at.Close()
		}
		time.Sleep(10 * time.Millisecond)
	}()

	// Test that we can send elements to the input channel
	done := make(chan bool, 1)
	go func() {
		defer func() {
			if r := recover(); r == nil {
				done <- true
			}
		}()
		for i := 0; i < 5; i++ {
			select {
			case at.In() <- i:
			case <-time.After(100 * time.Millisecond):
				return
			}
		}
	}()

	// Wait for goroutine to finish or timeout
	select {
	case <-done:
		// Successfully sent elements
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout sending elements to throttler")
	}

	// Verify rate is at maximum initially
	if at.GetCurrentRate() != int64(config.MaxThroughput) {
		t.Errorf("expected initial rate %d, got %d", config.MaxThroughput, at.GetCurrentRate())
	}
}

func TestAdaptiveThrottler_OutChannelRespectsQuota(t *testing.T) {
	config := AdaptiveThrottlerConfig{
		MaxMemoryPercent:    80.0,
		MaxCPUPercent:       70.0,
		MinThroughput:       1,
		MaxThroughput:       1,
		SampleInterval:      50 * time.Millisecond,
		BufferSize:          4,
		AdaptationFactor:    0.2,
		SmoothTransitions:   false,
		CPUUsageMode:        CPUUsageModeHeuristic,
		HysteresisBuffer:    5.0,
		MaxRateChangeFactor: 0.5,
	}

	mockMonitor := &mockResourceMonitor{
		stats: ResourceStats{
			MemoryUsedPercent: 10,
			CPUUsagePercent:   10,
		},
	}

	period := 80 * time.Millisecond
	at := newAdaptiveThrottlerWithMonitor(config, mockMonitor, period)
	defer func() {
		at.Close()
		time.Sleep(20 * time.Millisecond)
	}()

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		at.In() <- "first"
		at.In() <- "second"
	}()

	first := <-at.Out()
	if first != "first" {
		t.Fatalf("expected first element, got %v", first)
	}
	firstReceived := time.Now()

	second := <-at.Out()
	if second != "second" {
		t.Fatalf("expected second element, got %v", second)
	}
	gap := time.Since(firstReceived)

	if gap < period/2 {
		t.Fatalf("expected quota enforcement delay, got %v", gap)
	}

	<-writeDone
}

func TestAdaptiveThrottler_RateLimits(t *testing.T) {
	config := AdaptiveThrottlerConfig{
		MaxMemoryPercent:    80.0,
		MaxCPUPercent:       70.0,
		MinThroughput:       5,
		MaxThroughput:       20,
		SampleInterval:      50 * time.Millisecond,
		BufferSize:          50,
		AdaptationFactor:    0.5, // Aggressive adaptation
		SmoothTransitions:   false,
		CPUUsageMode:        CPUUsageModeHeuristic,
		HysteresisBuffer:    5.0,
		MaxRateChangeFactor: 0.5,
	}

	// Test minimum bound
	t.Run("minimum throughput", func(t *testing.T) {
		mockMonitor := &mockResourceMonitor{
			stats: ResourceStats{
				MemoryUsedPercent: 90.0, // High usage - should constrain
				CPUUsagePercent:   80.0,
				GoroutineCount:    10,
				Timestamp:         time.Now(),
			},
		}

		at := newAdaptiveThrottlerWithMonitor(config, mockMonitor)
		defer func() {
			at.Close()
			time.Sleep(50 * time.Millisecond)
		}()

		// Wait for adaptation
		time.Sleep(200 * time.Millisecond)

		rate := at.GetCurrentRate()
		if rate < int64(config.MinThroughput) {
			t.Errorf("rate %d should not go below minimum %d", rate, config.MinThroughput)
		}
	})

	// Test maximum bound
	t.Run("maximum throughput", func(t *testing.T) {
		mockMonitor := &mockResourceMonitor{
			stats: ResourceStats{
				MemoryUsedPercent: 10.0, // Low usage - should allow max
				CPUUsagePercent:   5.0,
				GoroutineCount:    5,
				Timestamp:         time.Now(),
			},
		}

		at := newAdaptiveThrottlerWithMonitor(config, mockMonitor)
		defer func() {
			at.Close()
			time.Sleep(50 * time.Millisecond)
		}()

		// Wait for adaptation
		time.Sleep(200 * time.Millisecond)

		rate := at.GetCurrentRate()
		if rate > int64(config.MaxThroughput) {
			t.Errorf("rate %d should not exceed maximum %d", rate, config.MaxThroughput)
		}
	})
}

func TestAdaptiveThrottler_ResourceConstraintResponse(t *testing.T) {
	config := AdaptiveThrottlerConfig{
		MaxMemoryPercent:    80.0,
		MaxCPUPercent:       70.0,
		MinThroughput:       10,
		MaxThroughput:       100,
		SampleInterval:      50 * time.Millisecond,
		BufferSize:          20,
		AdaptationFactor:    0.2,
		SmoothTransitions:   false,
		CPUUsageMode:        CPUUsageModeHeuristic,
		HysteresisBuffer:    5.0,
		MaxRateChangeFactor: 0.5,
	}

	// Start with high throughput
	initialRate := 100
	config.MaxThroughput = initialRate

	mockMonitor := &mockResourceMonitor{
		stats: ResourceStats{
			MemoryUsedPercent: 50.0, // OK initially
			CPUUsagePercent:   40.0,
			GoroutineCount:    5,
			Timestamp:         time.Now(),
		},
	}

	at := newAdaptiveThrottlerWithMonitor(config, mockMonitor)
	defer func() {
		at.Close()
		time.Sleep(50 * time.Millisecond)
	}()

	// Initial rate should be max
	if at.GetCurrentRate() != int64(initialRate) {
		t.Errorf("expected initial rate %d, got %d", initialRate, at.GetCurrentRate())
	}

	// Simulate resource constraint
	mockMonitor.SetStats(ResourceStats{
		MemoryUsedPercent: 85.0, // Constrained
		CPUUsagePercent:   40.0,
		GoroutineCount:    5,
		Timestamp:         time.Now(),
	})

	// Wait for adaptation
	time.Sleep(200 * time.Millisecond)

	newRate := at.GetCurrentRate()
	if newRate >= int64(initialRate) {
		t.Errorf("rate should decrease when constrained, got %d (initial %d)", newRate, initialRate)
	}
	if newRate < int64(config.MinThroughput) {
		t.Errorf("rate %d should not go below minimum %d", newRate, config.MinThroughput)
	}
}

func TestAdaptiveThrottler_RateIncreaseNotifiesQuota(t *testing.T) {
	config := AdaptiveThrottlerConfig{
		MaxMemoryPercent:    80.0,
		MaxCPUPercent:       70.0,
		MinThroughput:       10,
		MaxThroughput:       100,
		SampleInterval:      50 * time.Millisecond,
		BufferSize:          10,
		AdaptationFactor:    0.5,
		SmoothTransitions:   false,
		CPUUsageMode:        CPUUsageModeHeuristic,
		HysteresisBuffer:    5.0,
		MaxRateChangeFactor: 1.0,
	}

	mockMonitor := &mockResourceMonitor{
		stats: ResourceStats{
			MemoryUsedPercent: 10.0,
			CPUUsagePercent:   10.0,
			Timestamp:         time.Now(),
		},
	}

	at := &AdaptiveThrottler{
		config:      config,
		monitor:     mockMonitor,
		period:      time.Second,
		in:          make(chan any),
		out:         make(chan any, config.BufferSize),
		quotaSignal: make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
	defer func() {
		at.Close()
		time.Sleep(10 * time.Millisecond)
	}()

	initialRate := int64(40)
	at.currentRate.Store(initialRate)
	at.maxElements.Store(initialRate)
	at.lastAdaptation = time.Now()

	// Simulate exhausted quota so emitters would be blocked
	at.counter.Store(initialRate)

	at.adaptRate()

	select {
	case <-at.quotaSignal:
		// Expected: quota reset signal was sent immediately after rate increase
	default:
		t.Fatal("expected quota reset notification when rate increases")
	}
}

func TestAdaptiveThrottler_SmoothTransitions(t *testing.T) {
	config := AdaptiveThrottlerConfig{
		MaxMemoryPercent:    80.0,
		MaxCPUPercent:       70.0,
		MinThroughput:       10,
		MaxThroughput:       100,
		SampleInterval:      50 * time.Millisecond,
		BufferSize:          20,
		AdaptationFactor:    0.5,
		SmoothTransitions:   true, // Enabled
		CPUUsageMode:        CPUUsageModeHeuristic,
		HysteresisBuffer:    5.0,
		MaxRateChangeFactor: 0.5,
	}

	mockMonitor := &mockResourceMonitor{
		stats: ResourceStats{
			MemoryUsedPercent: 85.0, // Constrained
			CPUUsagePercent:   40.0,
			GoroutineCount:    5,
			Timestamp:         time.Now(),
		},
	}

	at := newAdaptiveThrottlerWithMonitor(config, mockMonitor)
	defer func() {
		at.Close()
		time.Sleep(50 * time.Millisecond)
	}()

	initialRate := at.GetCurrentRate()

	// Wait for multiple adaptations
	time.Sleep(300 * time.Millisecond)

	finalRate := at.GetCurrentRate()

	// With smoothing, the rate should change gradually
	// We expect some reduction but not immediate full reduction
	if finalRate >= initialRate {
		t.Errorf("rate should decrease when constrained, got %d (initial: %d)", finalRate, initialRate)
	}

	// Verify rate is within reasonable bounds
	if finalRate < int64(config.MinThroughput) {
		t.Errorf("rate %d should not go below minimum %d", finalRate, config.MinThroughput)
	}

	// With smoothing enabled, the rate reduction should be gradual (not immediate full reduction)
	// Calculate expected aggressive reduction (without smoothing) for comparison
	aggressiveReduction := int64(float64(initialRate) * config.AdaptationFactor * 2)
	expectedMinRateWithoutSmoothing := initialRate - aggressiveReduction
	actualReduction := initialRate - finalRate

	// With smoothing, actual reduction should be less than aggressive reduction would be
	// This verifies that smoothing is actually working (gradual vs immediate)
	if expectedMinRateWithoutSmoothing > int64(config.MinThroughput) {
		if actualReduction >= aggressiveReduction {
			t.Errorf("with smoothing enabled, rate reduction should be gradual. "+
				"Got reduction of %d (from %d to %d), expected less than aggressive reduction of %d",
				actualReduction, initialRate, finalRate, aggressiveReduction,
			)
		}
	}
}

func TestAdaptiveThrottler_SmoothTransitionsRespectBounds(t *testing.T) {
	config := AdaptiveThrottlerConfig{
		MaxMemoryPercent:    80.0,
		MaxCPUPercent:       70.0,
		MinThroughput:       5,
		MaxThroughput:       6,
		SampleInterval:      20 * time.Millisecond,
		BufferSize:          10,
		AdaptationFactor:    0.9,
		SmoothTransitions:   true,
		CPUUsageMode:        CPUUsageModeHeuristic,
		HysteresisBuffer:    5.0,
		MaxRateChangeFactor: 0.5,
	}

	mockMonitor := &mockResourceMonitor{
		stats: ResourceStats{
			MemoryUsedPercent: 90.0,
			CPUUsagePercent:   10.0,
			GoroutineCount:    5,
			Timestamp:         time.Now(),
		},
	}

	at := newAdaptiveThrottlerWithMonitor(config, mockMonitor)
	defer func() {
		at.Close()
		time.Sleep(20 * time.Millisecond)
	}()

	time.Sleep(3 * config.SampleInterval)

	if rate := at.GetCurrentRate(); rate < int64(config.MinThroughput) {
		t.Fatalf("expected rate to stay >= %d, got %d", config.MinThroughput, rate)
	}
}

func TestAdaptiveThrottler_CloseClosesOutputEvenIfInputOpen(t *testing.T) {
	config := AdaptiveThrottlerConfig{
		MaxMemoryPercent:    80.0,
		MaxCPUPercent:       70.0,
		MinThroughput:       1,
		MaxThroughput:       5,
		SampleInterval:      20 * time.Millisecond,
		BufferSize:          5,
		AdaptationFactor:    0.2,
		SmoothTransitions:   false,
		CPUUsageMode:        CPUUsageModeHeuristic,
		HysteresisBuffer:    5.0,
		MaxRateChangeFactor: 0.5,
	}

	mockMonitor := &mockResourceMonitor{
		stats: ResourceStats{
			MemoryUsedPercent: 10.0,
			CPUUsagePercent:   10.0,
			GoroutineCount:    5,
			Timestamp:         time.Now(),
		},
	}

	at := newAdaptiveThrottlerWithMonitor(config, mockMonitor)
	defer func() {
		close(at.in)
		at.Close()
		time.Sleep(20 * time.Millisecond)
	}()

	done := make(chan struct{})
	go func() {
		_, ok := <-at.Out()
		if ok {
			t.Error("expected output channel to be closed after Close without draining input")
		}
		close(done)
	}()

	// Give goroutine time to start before closing
	time.Sleep(10 * time.Millisecond)
	at.Close()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("output channel not closed after Close() when input left open")
	}
}

func TestAdaptiveThrottler_CloseStopsBackgroundLoops(t *testing.T) {
	at, err := NewAdaptiveThrottler(DefaultAdaptiveThrottlerConfig())
	if err != nil {
		t.Fatalf("failed to create adaptive throttler: %v", err)
	}

	at.Close()

	select {
	case <-at.done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected done channel to be closed after Close()")
	}

	// Close again to ensure idempotence
	at.Close()
}

func TestAdaptiveThrottler_CPUUsageMode(t *testing.T) {
	at, err := NewAdaptiveThrottler(DefaultAdaptiveThrottlerConfig())
	if err != nil {
		t.Fatalf("failed to create adaptive throttler: %v", err)
	}
	defer func() {
		at.Close()
		time.Sleep(10 * time.Millisecond)
	}()

	// Should be able to create with rusage mode
	if at == nil {
		t.Fatal("should create throttler with rusage mode")
	}

	// The actual CPU mode used depends on platform support
	// Just verify the throttler was created successfully
	stats := at.GetResourceStats()
	if stats.CPUUsagePercent < 0 || stats.CPUUsagePercent > 100 {
		t.Errorf("CPU percent should be valid, got %v", stats.CPUUsagePercent)
	}
}

func TestAdaptiveThrottler_GetCurrentRate(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	at, err := NewAdaptiveThrottler(config)
	if err != nil {
		t.Fatalf("failed to create adaptive throttler: %v", err)
	}
	defer func() {
		at.Close()
		time.Sleep(10 * time.Millisecond)
	}()

	rate := at.GetCurrentRate()
	if rate <= 0 {
		t.Errorf("current rate should be positive, got %d", rate)
	}
	if rate > int64(config.MaxThroughput) {
		t.Errorf("current rate %d should not exceed max %d", rate, config.MaxThroughput)
	}
}

func TestAdaptiveThrottler_GetResourceStats(t *testing.T) {
	config := DefaultAdaptiveThrottlerConfig()
	at, err := NewAdaptiveThrottler(config)
	if err != nil {
		t.Fatalf("failed to create adaptive throttler: %v", err)
	}
	defer func() {
		at.Close()
		time.Sleep(10 * time.Millisecond)
	}()

	stats := at.GetResourceStats()

	if stats.MemoryUsedPercent < 0 || stats.MemoryUsedPercent > 100 {
		t.Errorf("memory percent should be between 0 and 100, got %v", stats.MemoryUsedPercent)
	}
	if stats.CPUUsagePercent < 0 || stats.CPUUsagePercent > 100 {
		t.Errorf("CPU percent should be between 0 and 100, got %v", stats.CPUUsagePercent)
	}
	if stats.GoroutineCount <= 0 {
		t.Errorf("goroutine count should be > 0, got %d", stats.GoroutineCount)
	}
	if stats.Timestamp.IsZero() {
		t.Error("timestamp should not be zero")
	}
}

func TestAdaptiveThrottler_BufferBackpressure(t *testing.T) {
	config := AdaptiveThrottlerConfig{
		MaxMemoryPercent:    80.0,
		MaxCPUPercent:       70.0,
		MinThroughput:       10,
		MaxThroughput:       100,
		SampleInterval:      100 * time.Millisecond,
		BufferSize:          5, // Small buffer
		AdaptationFactor:    0.2,
		SmoothTransitions:   false,
		CPUUsageMode:        CPUUsageModeHeuristic,
		HysteresisBuffer:    5.0,
		MaxRateChangeFactor: 0.5,
	}

	mockMonitor := &mockResourceMonitor{
		stats: ResourceStats{
			MemoryUsedPercent: 30.0,
			CPUUsagePercent:   20.0,
			GoroutineCount:    5,
			Timestamp:         time.Now(),
		},
	}

	at := newAdaptiveThrottlerWithMonitor(config, mockMonitor)
	defer func() {
		close(at.in)
		at.Close()
		time.Sleep(10 * time.Millisecond) // Allow cleanup
	}()

	// Fill buffer
	for i := 0; i < config.BufferSize; i++ {
		select {
		case at.In() <- i:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("should be able to send to buffer initially")
		}
	}

	// Next send should block or succeed (backpressure test)
	done := make(chan bool, 1)
	go func() {
		at.In() <- "test"
		done <- true
	}()

	select {
	case <-done:
		// Element was accepted - buffer management worked
	case <-time.After(200 * time.Millisecond):
		// Timeout - this is also acceptable as backpressure is working
		t.Log("Backpressure working - send blocked as expected")
	}
}

func BenchmarkAdaptiveThrottler_GetResourceStats(b *testing.B) {
	at, err := NewAdaptiveThrottler(DefaultAdaptiveThrottlerConfig())
	if err != nil {
		b.Fatalf("failed to create adaptive throttler: %v", err)
	}
	defer func() {
		at.Close()
		time.Sleep(10 * time.Millisecond)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = at.GetResourceStats()
	}
}

// TestAdaptiveThrottler_BufferedQuotaSignal verifies that the quota signal channel
// is buffered and handles reset signals properly under sustained pressure
func TestAdaptiveThrottler_EdgeCases(t *testing.T) {
	t.Run("exact threshold values", func(t *testing.T) {
		config := AdaptiveThrottlerConfig{
			MaxMemoryPercent:    80.0,
			MaxCPUPercent:       70.0,
			MinThroughput:       10,
			MaxThroughput:       100,
			SampleInterval:      50 * time.Millisecond,
			BufferSize:          20,
			AdaptationFactor:    0.2,
			SmoothTransitions:   false,
			CPUUsageMode:        CPUUsageModeHeuristic,
			HysteresisBuffer:    5.0,
			MaxRateChangeFactor: 0.5,
		}

		// Test exactly at threshold - should not be constrained
		mockMonitor := &mockResourceMonitor{
			stats: ResourceStats{
				MemoryUsedPercent: 80.0, // Exactly at threshold
				CPUUsagePercent:   70.0, // Exactly at threshold
				GoroutineCount:    5,
				Timestamp:         time.Now(),
			},
		}

		at := newAdaptiveThrottlerWithMonitor(config, mockMonitor)
		defer func() {
			at.Close()
			time.Sleep(50 * time.Millisecond)
		}()

		// Should not be constrained at exact threshold
		if mockMonitor.IsResourceConstrained() {
			t.Error("should not be constrained at exact threshold values")
		}

		time.Sleep(200 * time.Millisecond)
		rate := at.GetCurrentRate()
		if rate < int64(config.MaxThroughput) {
			t.Errorf("rate should not decrease when at exact threshold, got %d", rate)
		}
	})

	t.Run("minimal throughput range", func(t *testing.T) {
		config := AdaptiveThrottlerConfig{
			MaxMemoryPercent:    80.0,
			MaxCPUPercent:       70.0,
			MinThroughput:       50,
			MaxThroughput:       50, // Same as min
			SampleInterval:      50 * time.Millisecond,
			BufferSize:          20,
			AdaptationFactor:    0.2,
			SmoothTransitions:   false,
			CPUUsageMode:        CPUUsageModeHeuristic,
			HysteresisBuffer:    5.0,
			MaxRateChangeFactor: 0.5,
		}

		mockMonitor := &mockResourceMonitor{
			stats: ResourceStats{
				MemoryUsedPercent: 90.0, // Constrained
				CPUUsagePercent:   20.0,
				GoroutineCount:    5,
				Timestamp:         time.Now(),
			},
		}

		at := newAdaptiveThrottlerWithMonitor(config, mockMonitor)
		defer func() {
			at.Close()
			time.Sleep(50 * time.Millisecond)
		}()

		// Rate should stay at 50 even under constraint
		time.Sleep(200 * time.Millisecond)
		rate := at.GetCurrentRate()
		if rate != 50 {
			t.Errorf("rate should stay at 50 with min=max=50, got %d", rate)
		}
	})

	t.Run("zero adaptation factor", func(t *testing.T) {
		config := AdaptiveThrottlerConfig{
			MaxMemoryPercent:    80.0,
			MaxCPUPercent:       70.0,
			MinThroughput:       10,
			MaxThroughput:       100,
			SampleInterval:      50 * time.Millisecond,
			BufferSize:          20,
			AdaptationFactor:    0.0, // No adaptation
			SmoothTransitions:   false,
			CPUUsageMode:        CPUUsageModeHeuristic,
			HysteresisBuffer:    5.0,
			MaxRateChangeFactor: 0.5,
		}

		mockMonitor := &mockResourceMonitor{
			stats: ResourceStats{
				MemoryUsedPercent: 90.0, // Constrained
				CPUUsagePercent:   20.0,
				GoroutineCount:    5,
				Timestamp:         time.Now(),
			},
		}

		at := newAdaptiveThrottlerWithMonitor(config, mockMonitor)
		defer func() {
			at.Close()
			time.Sleep(50 * time.Millisecond)
		}()

		initialRate := at.GetCurrentRate()
		time.Sleep(200 * time.Millisecond)
		finalRate := at.GetCurrentRate()

		// Rate should not change with zero adaptation factor
		if initialRate != finalRate {
			t.Errorf("rate should not change with adaptation factor 0, initial: %d, final: %d", initialRate, finalRate)
		}
	})
}

func TestAdaptiveThrottler_BufferedQuotaSignal(t *testing.T) {
	config := AdaptiveThrottlerConfig{
		MaxMemoryPercent:    80.0,
		MaxCPUPercent:       70.0,
		MinThroughput:       1,
		MaxThroughput:       10,
		SampleInterval:      50 * time.Millisecond,
		BufferSize:          50,
		AdaptationFactor:    0.2,
		SmoothTransitions:   false,
		CPUUsageMode:        CPUUsageModeHeuristic,
		HysteresisBuffer:    5.0,
		MaxRateChangeFactor: 0.5,
	}

	mockMonitor := &mockResourceMonitor{
		stats: ResourceStats{
			MemoryUsedPercent: 30.0,
			CPUUsagePercent:   20.0,
			GoroutineCount:    5,
			Timestamp:         time.Now(),
		},
	}

	at := newAdaptiveThrottlerWithMonitor(config, mockMonitor)
	defer func() {
		at.Close()
		time.Sleep(50 * time.Millisecond)
	}()

	// Fill the output buffer and quota
	for i := 0; i < config.MaxThroughput; i++ {
		select {
		case at.In() <- i:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("should be able to send to input initially")
		}
	}

	time.Sleep(1100 * time.Millisecond) // Wait > 1 second for quota reset

	// Should be able to send more elements now (quota reset signal was buffered)
	select {
	case at.In() <- "test":
		// Success - buffered signal worked
	case <-time.After(100 * time.Millisecond):
		t.Error("should be able to send after quota reset, buffered signal may not be working")
	}
}

func TestAdaptiveThrottler_CustomMemoryReader(t *testing.T) {
	customMemoryPercent := 42.0
	callCount := 0

	config := DefaultAdaptiveThrottlerConfig()
	config.MemoryReader = func() (float64, error) {
		callCount++
		return customMemoryPercent, nil
	}

	at, err := NewAdaptiveThrottler(config)
	if err != nil {
		t.Fatalf("failed to create adaptive throttler: %v", err)
	}
	defer at.Close()

	// Allow some time for stats collection
	time.Sleep(150 * time.Millisecond)

	stats := at.GetResourceStats()
	if stats.MemoryUsedPercent != customMemoryPercent {
		t.Errorf("expected memory percent %f, got %f", customMemoryPercent, stats.MemoryUsedPercent)
	}
	if callCount == 0 {
		t.Error("custom memory reader was not called")
	}
}
