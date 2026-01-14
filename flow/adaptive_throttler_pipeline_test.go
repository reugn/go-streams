package flow

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/reugn/go-streams/internal/testutil"
)

// MockMonitor is a mock implementation of resourceMonitor for testing.
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

// createThrottlerWithLongInterval creates a throttler with default config but long sample interval.
func createThrottlerWithLongInterval(t *testing.T) *AdaptiveThrottler {
	t.Helper()
	config := DefaultAdaptiveThrottlerConfig()
	config.SampleInterval = 10 * time.Second
	at, err := NewAdaptiveThrottler(config)
	if err != nil {
		t.Fatalf("Failed to create throttler: %v", err)
	}
	return at
}

// TestAdaptiveThrottler_FlowControl tests token bucket throttling with burst traffic.
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

// TestAdaptiveThrottler_To tests To method that streams data to a sink.
func TestAdaptiveThrottler_To(t *testing.T) {
	at := createThrottlerWithLongInterval(t)
	defer at.close()

	var received []any
	var mu sync.Mutex
	done := make(chan struct{})

	sinkCh := make(chan any, 10)
	mockSink := testutil.NewMockSink(sinkCh, make(chan struct{}))

	// Collect data from sink
	go testutil.CollectDataFromChannelWithMutex(sinkCh, &received, &mu, done)

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
	mockInlet := testutil.NewMockInlet(inletIn)

	testData := []any{"data1", "data2", "data3"}
	var received []any
	var mu sync.Mutex
	done := make(chan struct{})

	// Start streamPortioned in background
	go at.streamPortioned(mockInlet)

	// Collect all received data
	go testutil.CollectDataFromChannelWithMutex(inletIn, &received, &mu, done)

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
	config := DefaultAdaptiveThrottlerConfig()
	config.SampleInterval = 10 * time.Second
	config.InitialRate = 100

	at, err := NewAdaptiveThrottler(config)
	if err != nil {
		t.Fatalf("failed to create throttler: %v", err)
	}
	defer at.close()

	downstream := NewPassThrough()

	resultFlow := at.Via(downstream)
	if resultFlow != downstream {
		t.Fatalf("Via should return the downstream flow instance")
	}

	testData := []any{"test1", "test2", "test3"}
	go func() {
		for _, data := range testData {
			at.In() <- data
		}
		close(at.In())
	}()

	received := testutil.CollectDataFromChannel(resultFlow.Out())

	if len(received) != len(testData) {
		t.Fatalf("Expected %d items, got %d", len(testData), len(received))
	}
	for i, expected := range testData {
		if received[i] != expected {
			t.Errorf("Expected data[%d]=%v, got %v", i, expected, received[i])
		}
	}
}

func TestAdaptiveThrottler_GetResourceStats(t *testing.T) {
	mockMonitor := &MockMonitor{}
	expected := ResourceStats{
		MemoryUsedPercent: 55.5,
		CPUUsagePercent:   42.3,
	}
	mockMonitor.ExpectGetStats(expected)

	at := &AdaptiveThrottler{monitor: mockMonitor}

	stats := at.GetResourceStats()

	if stats != expected {
		t.Fatalf("expected stats %+v, got %+v", expected, stats)
	}
}

// TestAdaptiveThrottler_PipelineLoop_Shutdown tests pipelineLoop shutdown scenarios.
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

	go at.pipelineLoop()

	at.in <- "test1"
	time.Sleep(10 * time.Millisecond)

	close(at.done)
	time.Sleep(200 * time.Millisecond)

	select {
	case _, ok := <-at.out:
		if ok {
			time.Sleep(100 * time.Millisecond)
			select {
			case _, ok2 := <-at.out:
				if ok2 {
					t.Error("Output channel should be closed after shutdown")
				}
			default:
			}
		}
	default:
	}
}

// TestAdaptiveThrottler_PipelineLoop_RateChange tests pipelineLoop with rate changes.
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

// TestAdaptiveThrottler_PipelineLoop_LowRate tests pipelineLoop with very low rate.
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

// TestAdaptiveThrottler_To_Shutdown tests To method with shutdown.
func TestAdaptiveThrottler_To_Shutdown(t *testing.T) {
	at := createThrottlerWithLongInterval(t)

	mockSink := testutil.NewMockSinkWithChannelDrain()
	sinkCh := mockSink.GetChannel()

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

	at.close()

	// Wait for To to complete (it will finish when streamPortioned completes)
	select {
	case <-toDone:
	case <-time.After(2 * time.Second):
		t.Error("To method did not complete within timeout")
	}

	time.Sleep(100 * time.Millisecond)

	testutil.VerifyChannelClosed(t, sinkCh, 50*time.Millisecond)
}

// TestAdaptiveThrottler_StreamPortioned_Blocking tests streamPortioned with blocking inlet.
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
	mockInlet := testutil.NewMockInlet(inletIn)

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
	received := testutil.CollectDataFromChannel(inletIn)

	// Wait for completion
	select {
	case <-done:
		// Good
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for streamPortioned to complete")
	}

	// Verify inlet is closed
	testutil.VerifyChannelClosed(t, inletIn, 50*time.Millisecond)

	if len(received) != 2 {
		t.Errorf("Expected 2 items, got %d", len(received))
	}
}
