package flow

import (
	"container/heap"
	"errors"
	"sync"
	"time"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/util"
)

// SlidingWindow assigns elements to windows of fixed length configured by the window
// size parameter.
// An additional window slide parameter controls how frequently a sliding window is started.
// Hence, sliding windows can be overlapping if the slide is smaller than the window size.
// In this case elements are assigned to multiple windows.
type SlidingWindow[T any] struct {
	sync.Mutex
	windowSize         time.Duration
	slidingInterval    time.Duration
	queue              *PriorityQueue
	in                 chan interface{}
	out                chan interface{}
	done               chan struct{}
	timestampExtractor func(T) int64
}

// Verify SlidingWindow satisfies the Flow interface.
var _ streams.Flow = (*SlidingWindow[any])(nil)

// NewSlidingWindow returns a new processing time based SlidingWindow.
// Processing time refers to the system time of the machine that is executing the
// respective operation.
//
// windowSize is the Duration of generated windows.
// slidingInterval is the sliding interval of generated windows.
func NewSlidingWindow(
	windowSize time.Duration,
	slidingInterval time.Duration) (*SlidingWindow[any], error) {
	return NewSlidingWindowWithTSExtractor[any](windowSize, slidingInterval, nil)
}

// NewSlidingWindowWithTSExtractor returns a new event time based SlidingWindow.
// Event time is the time that each individual event occurred on its producing device.
// Gives correct results on out-of-order events, late events, or on replays of data.
//
// windowSize is the Duration of generated windows.
// slidingInterval is the sliding interval of generated windows.
// timestampExtractor is the record timestamp (in nanoseconds) extractor.
func NewSlidingWindowWithTSExtractor[T any](
	windowSize time.Duration,
	slidingInterval time.Duration,
	timestampExtractor func(T) int64) (*SlidingWindow[T], error) {

	if windowSize < slidingInterval {
		return nil, errors.New("slidingInterval is larger than windowSize")
	}

	slidingWindow := &SlidingWindow[T]{
		windowSize:         windowSize,
		slidingInterval:    slidingInterval,
		queue:              &PriorityQueue{},
		in:                 make(chan interface{}),
		out:                make(chan interface{}),
		done:               make(chan struct{}),
		timestampExtractor: timestampExtractor,
	}
	go slidingWindow.receive()

	return slidingWindow, nil
}

// Via streams data through the given flow
func (sw *SlidingWindow[T]) Via(flow streams.Flow) streams.Flow {
	go sw.emit()
	go sw.transmit(flow)
	return flow
}

// To streams data to the given sink
func (sw *SlidingWindow[T]) To(sink streams.Sink) {
	go sw.emit()
	sw.transmit(sink)
}

// Out returns an output channel for sending data
func (sw *SlidingWindow[T]) Out() <-chan interface{} {
	return sw.out
}

// In returns an input channel for receiving data
func (sw *SlidingWindow[T]) In() chan<- interface{} {
	return sw.in
}

// transmit submits closed windows to the next Inlet.
func (sw *SlidingWindow[T]) transmit(inlet streams.Inlet) {
	for window := range sw.out {
		inlet.In() <- window
	}
	close(inlet.In())
}

// timestamp extracts the timestamp from an element if the timestampExtractor is set.
// It returns system clock time otherwise.
func (sw *SlidingWindow[T]) timestamp(element T) int64 {
	if sw.timestampExtractor == nil {
		return util.NowNano()
	}
	return sw.timestampExtractor(element)
}

// receive buffers the incoming elements by pushing them into a priority queue,
// ordered by their creation time.
func (sw *SlidingWindow[T]) receive() {
	for element := range sw.in {
		item := &Item{element, sw.timestamp(element.(T)), 0}
		sw.Lock()
		heap.Push(sw.queue, item)
		sw.Unlock()
	}
	close(sw.done)
}

// emit captures and emits a new window every sw.slidingInterval.
func (sw *SlidingWindow[T]) emit() {
	// wait for the sliding window to start
	time.Sleep(sw.windowSize - sw.slidingInterval)

	ticker := time.NewTicker(sw.slidingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sw.dispatchWindow()

		case <-sw.done:
			sw.dispatchWindow()
			close(sw.out)
			return
		}
	}
}

// dispatchWindow creates a new window and slides the elements queue.
// It sends the slice of elements to the output channel if the window is not empty.
func (sw *SlidingWindow[T]) dispatchWindow() {
	sw.Lock()
	// build a window of elements
	var windowBottomIndex int
	now := util.NowNano()
	windowUpperIndex := sw.queue.Len()
	slideUpperIndex := windowUpperIndex
	slideUpperTime := now - sw.windowSize.Nanoseconds() + sw.slidingInterval.Nanoseconds()
	windowBottomTime := now - sw.windowSize.Nanoseconds()
	for i, item := range *sw.queue {
		if item.epoch < windowBottomTime {
			windowBottomIndex = i
		}
		if item.epoch > slideUpperTime {
			slideUpperIndex = i
			break
		}
	}
	windowElements := extractWindowElements(sw.queue.Slice(windowBottomIndex, windowUpperIndex))
	if windowUpperIndex > 0 { // the queue is not empty
		// slice the queue using the lower and upper bounds
		sliced := sw.queue.Slice(slideUpperIndex, windowUpperIndex)
		// reset the queue
		sw.queue = &sliced
		heap.Init(sw.queue)
	}
	sw.Unlock()

	// send elements if the window is not empty
	if len(windowElements) > 0 {
		sw.out <- windowElements
	}
}

// extractWindowElements generates a window of elements from a given slice of queue items.
func extractWindowElements(items []*Item) []interface{} {
	elements := make([]interface{}, len(items))
	for i, item := range items {
		elements[i] = item.Msg
	}
	return elements
}
