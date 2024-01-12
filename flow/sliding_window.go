package flow

import (
	"container/heap"
	"errors"
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// SlidingWindow assigns elements to windows of fixed length configured by the window
// size parameter.
// An additional window slide parameter controls how frequently a sliding window is started.
// Hence, sliding windows can be overlapping if the slide is smaller than the window size.
// In this case elements are assigned to multiple windows.
// T indicates the incoming element type, and the outgoing element type is []T.
type SlidingWindow[T any] struct {
	sync.Mutex
	windowSize         time.Duration
	slidingInterval    time.Duration
	queue              *PriorityQueue
	in                 chan any
	out                chan any
	done               chan struct{}
	timestampExtractor func(T) int64
}

// Verify SlidingWindow satisfies the Flow interface.
var _ streams.Flow = (*SlidingWindow[any])(nil)

// NewSlidingWindow returns a new SlidingWindow operator based on processing time.
// Processing time refers to the system time of the machine that is executing the
// respective operation.
// T specifies the incoming element type, and the outgoing element type is []T.
//
// windowSize is the Duration of generated windows.
// slidingInterval is the sliding interval of generated windows.
func NewSlidingWindow[T any](
	windowSize time.Duration,
	slidingInterval time.Duration) (*SlidingWindow[T], error) {
	return NewSlidingWindowWithTSExtractor[T](windowSize, slidingInterval, nil)
}

// NewSlidingWindowWithTSExtractor returns a new SlidingWindow operator based on event time.
// Event time is the time that each individual event occurred on its producing device.
// Gives correct results on out-of-order events, late events, or on replays of data.
// T specifies the incoming element type, and the outgoing element type is []T.
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
		in:                 make(chan any),
		out:                make(chan any),
		done:               make(chan struct{}),
		timestampExtractor: timestampExtractor,
	}
	go slidingWindow.receive()

	return slidingWindow, nil
}

// Via streams data to a specified Flow and returns it.
func (sw *SlidingWindow[T]) Via(flow streams.Flow) streams.Flow {
	go sw.emit()
	go sw.transmit(flow)
	return flow
}

// To streams data to a specified Sink.
func (sw *SlidingWindow[T]) To(sink streams.Sink) {
	go sw.emit()
	sw.transmit(sink)
}

// Out returns the output channel of the SlidingWindow operator.
func (sw *SlidingWindow[T]) Out() <-chan any {
	return sw.out
}

// In returns the input channel of the SlidingWindow operator.
func (sw *SlidingWindow[T]) In() chan<- any {
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
		return time.Now().UnixNano()
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

	lastTick := time.Now()
	ticker := time.NewTicker(sw.slidingInterval)
	defer ticker.Stop()

	for {
		select {
		case lastTick = <-ticker.C:
			sw.dispatchWindow(lastTick)

		case <-sw.done:
			sw.dispatchWindow(lastTick.Add(sw.slidingInterval))
			close(sw.out)
			return
		}
	}
}

// dispatchWindow creates a new window and slides the elements queue.
// It sends the slice of elements to the output channel if the window is not empty.
func (sw *SlidingWindow[T]) dispatchWindow(tick time.Time) {
	sw.Lock()
	// build a window of elements
	var windowBottomIndex int
	now := tick.UnixNano()
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
	windowElements := extractWindowElements[T](sw.queue.Slice(windowBottomIndex, windowUpperIndex))
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
func extractWindowElements[T any](items []*Item) []T {
	elements := make([]T, len(items))
	for i, item := range items {
		elements[i] = item.Msg.(T)
	}
	return elements
}
