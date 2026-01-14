package testutil

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// MockSink is a mock implementation of a sink for testing.
type MockSink struct {
	in         chan any
	completion chan struct{}
	completed  atomic.Bool
}

// NewMockSink creates a new MockSink with the given channel.
func NewMockSink(in chan any, completion chan struct{}) *MockSink {
	return &MockSink{
		in:         in,
		completion: completion,
	}
}

func (m *MockSink) In() chan<- any {
	return m.in
}

func (m *MockSink) AwaitCompletion() {
	if !m.completed.Load() {
		if m.completed.CompareAndSwap(false, true) {
			if m.completion != nil {
				close(m.completion)
			}
		}
	}
}

// MockSinkWithChannelDrain is a mock sink that automatically drains its input channel.
type MockSinkWithChannelDrain struct {
	in   chan any
	done chan struct{}
}

func (m *MockSinkWithChannelDrain) In() chan<- any {
	return m.in
}

func (m *MockSinkWithChannelDrain) AwaitCompletion() {
	<-m.done
}

// GetChannel returns the receive-only channel for reading data.
func (m *MockSinkWithChannelDrain) GetChannel() <-chan any {
	return m.in
}

// NewMockSinkWithChannelDrain creates a mock sink that drains the channel like real sinks do.
func NewMockSinkWithChannelDrain() *MockSinkWithChannelDrain {
	sink := &MockSinkWithChannelDrain{
		in:   make(chan any),
		done: make(chan struct{}),
	}
	go func() {
		defer close(sink.done)
		for range sink.in {
			_ = struct{}{}
		}
	}()
	return sink
}

// CollectDataFromChannel collects all data from a channel into a slice.
// If the channel is buffered, we reserve that capacity up front.
func CollectDataFromChannel(ch <-chan any) []any {
	received := make([]any, 0, len(ch))
	for data := range ch {
		received = append(received, data)
	}
	return received
}

// CollectDataFromChannelWithMutex collects data from channel using mutex for thread safety.
func CollectDataFromChannelWithMutex(ch <-chan any, received *[]any, mu *sync.Mutex, done chan struct{}) {
	defer close(done)
	for data := range ch {
		mu.Lock()
		*received = append(*received, data)
		mu.Unlock()
	}
}

// VerifyChannelClosed checks that a channel is closed within a timeout.
func VerifyChannelClosed(t *testing.T, ch <-chan any, timeout time.Duration) {
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

// MockInlet is a mock implementation of an inlet for testing.
type MockInlet struct {
	in chan any
}

// NewMockInlet creates a new MockInlet with the given channel.
func NewMockInlet(in chan any) *MockInlet {
	return &MockInlet{in: in}
}

func (m *MockInlet) In() chan<- any {
	return m.in
}
