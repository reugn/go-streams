package flow

import (
	"sort"
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// SlidingWindowOpts represents SlidingWindow configuration options.
type SlidingWindowOpts[T any] struct {
	// EventTimeExtractor is a function that extracts the event time from an element.
	// Event time is the time at which the event occurred on its producing device.
	// Using event time enables correct windowing even when events arrive out of order
	// or with delays.
	//
	// If EventTimeExtractor is not specified, processing time is used. Processing time
	// refers to the system time of the machine executing the window operation.
	EventTimeExtractor func(T) time.Time
	// EmitPartialWindow determines whether to emit window elements before the first
	// full window duration has elapsed. If false, the first window will only be
	// emitted after the full window duration.
	EmitPartialWindow bool
}

// timedElement stores an incoming element along with its event time.
type timedElement[T any] struct {
	element   T
	eventTime time.Time
}

// SlidingWindow assigns elements to windows of fixed length configured by the window
// size parameter.
// An additional window slide parameter controls how frequently a sliding window is started.
// Hence, sliding windows can be overlapping if the slide is smaller than the window size.
// In this case elements are assigned to multiple windows.
// T indicates the incoming element type, and the outgoing element type is []T.
type SlidingWindow[T any] struct {
	mu              sync.Mutex
	windowSize      time.Duration
	slidingInterval time.Duration
	queue           []timedElement[T]

	in   chan any
	out  chan any
	done chan struct{}

	opts SlidingWindowOpts[T]
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
//
// NewSlidingWindow panics if slidingInterval is larger than windowSize.
func NewSlidingWindow[T any](windowSize, slidingInterval time.Duration) *SlidingWindow[T] {
	return NewSlidingWindowWithOpts(windowSize, slidingInterval, SlidingWindowOpts[T]{})
}

// NewSlidingWindowWithOpts returns a new SlidingWindow operator configured with the
// provided configuration options.
// T specifies the incoming element type, and the outgoing element type is []T.
//
// windowSize is the Duration of generated windows.
// slidingInterval is the sliding interval of generated windows.
// opts are the sliding window configuration options.
//
// NewSlidingWindowWithOpts panics if slidingInterval is larger than windowSize.
func NewSlidingWindowWithOpts[T any](
	windowSize, slidingInterval time.Duration, opts SlidingWindowOpts[T]) *SlidingWindow[T] {

	if windowSize < slidingInterval {
		panic("sliding interval is larger than window size")
	}

	slidingWindow := &SlidingWindow[T]{
		windowSize:      windowSize,
		slidingInterval: slidingInterval,
		in:              make(chan any),
		out:             make(chan any),
		done:            make(chan struct{}),
		opts:            opts,
	}

	// start buffering incoming stream elements
	go slidingWindow.receive()
	// capture and emit a new window every sliding interval
	go slidingWindow.emit()

	return slidingWindow
}

// Via asynchronously streams data to the given Flow and returns it.
func (sw *SlidingWindow[T]) Via(flow streams.Flow) streams.Flow {
	go sw.transmit(flow)
	return flow
}

// To streams data to the given Sink and blocks until the Sink has completed
// processing all data.
func (sw *SlidingWindow[T]) To(sink streams.Sink) {
	sw.transmit(sink)
	sink.AwaitCompletion()
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

// eventTime extracts the time from an element if the EventTimeExtractor is set.
// Otherwise, the processing time is returned.
func (sw *SlidingWindow[T]) eventTime(element T) time.Time {
	if sw.opts.EventTimeExtractor == nil {
		return time.Now()
	}
	return sw.opts.EventTimeExtractor(element)
}

// receive buffers the incoming elements by pushing them into the queue,
// wrapping the original item into a timedElement along with its event time.
func (sw *SlidingWindow[T]) receive() {
	for element := range sw.in {
		eventTime := sw.eventTime(element.(T))

		sw.mu.Lock()
		timed := timedElement[T]{
			element:   element.(T),
			eventTime: eventTime,
		}
		sw.queue = append(sw.queue, timed)
		sw.mu.Unlock()
	}
	close(sw.done)
}

// emit captures and emits a new window every sw.slidingInterval.
func (sw *SlidingWindow[T]) emit() {
	defer close(sw.out)

	if !sw.opts.EmitPartialWindow {
		timer := time.NewTimer(sw.windowSize - sw.slidingInterval)
		select {
		case <-timer.C:
		case <-sw.done:
			timer.Stop()
			return
		}
	}

	lastTick := time.Now()
	ticker := time.NewTicker(sw.slidingInterval)
	defer ticker.Stop()

	for {
		select {
		case lastTick = <-ticker.C:
			sw.dispatchWindow(lastTick)

		case <-sw.done:
			sw.dispatchWindow(lastTick.Add(sw.slidingInterval))
			return
		}
	}
}

// dispatchWindow is responsible for sending the elements in the current
// window to the output channel and moving the window to the next position.
func (sw *SlidingWindow[T]) dispatchWindow(tick time.Time) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// sort elements in the queue by their time
	sort.Slice(sw.queue, func(i, j int) bool {
		return sw.queue[i].eventTime.Before(sw.queue[j].eventTime)
	})

	// extract current window elements
	windowElements := sw.extractWindowElements(tick)

	// send elements downstream if the current window is not empty
	if len(windowElements) > 0 {
		sw.out <- windowElements
	}
}

// extractWindowElements extracts and returns elements from the sliding window that
// fall within the current window. Elements newer than tick will not be included.
// The sliding window queue is updated to remove previous interval elements.
func (sw *SlidingWindow[T]) extractWindowElements(tick time.Time) []T {
	// calculate the next window start time
	nextWindowStartTime := tick.Add(-sw.windowSize).Add(sw.slidingInterval)

	elements := make([]T, 0, len(sw.queue))
	var remainingElements []timedElement[T]
queueLoop:
	for i, element := range sw.queue {
		if remainingElements == nil && element.eventTime.After(nextWindowStartTime) {
			// copy remaining elements
			remainingElements = make([]timedElement[T], len(sw.queue)-i)
			_ = copy(remainingElements, sw.queue[i:])
		}
		switch {
		case element.eventTime.Before(tick):
			elements = append(elements, element.element)
		default:
			break queueLoop // we can break since the queue is ordered
		}
	}

	// move the window
	sw.queue = remainingElements

	return elements
}
