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
	// AllowedLateness provides a grace period after the window closes, during which
	// late data is still processed. This prevents data loss and improves the
	// completeness of results. If AllowedLateness is not specified, records belonging
	// to a closed window that arrive late will be discarded.
	//
	// The specified value must be no larger than the window sliding interval.
	AllowedLateness time.Duration
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

	lowerBoundary time.Time
	queue         []timedElement[T]

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
// windowSize is the duration of each full window.
// slidingInterval is the interval at which new windows are created and emitted.
//
// NewSlidingWindow panics if slidingInterval is larger than windowSize.
func NewSlidingWindow[T any](windowSize, slidingInterval time.Duration) *SlidingWindow[T] {
	return NewSlidingWindowWithOpts(windowSize, slidingInterval, SlidingWindowOpts[T]{})
}

// NewSlidingWindowWithOpts returns a new SlidingWindow operator configured with the
// provided configuration options.
// T specifies the incoming element type, and the outgoing element type is []T.
//
// windowSize is the duration of each full window.
// slidingInterval is the interval at which new windows are created and emitted.
// opts are the sliding window configuration options.
//
// NewSlidingWindowWithOpts panics if slidingInterval is larger than windowSize,
// or the allowed lateness is larger than slidingInterval.
func NewSlidingWindowWithOpts[T any](
	windowSize, slidingInterval time.Duration, opts SlidingWindowOpts[T]) *SlidingWindow[T] {
	if slidingInterval > windowSize {
		panic("sliding interval is larger than window size")
	}
	if opts.AllowedLateness > slidingInterval {
		panic("allowed lateness is larger than sliding interval")
	}

	slidingWindow := &SlidingWindow[T]{
		windowSize:      windowSize,
		slidingInterval: slidingInterval,
		in:              make(chan any),
		out:             make(chan any),
		done:            make(chan struct{}),
		opts:            opts,
	}

	// start processing stream elements
	go slidingWindow.stream()

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

// stream buffers the incoming elements by pushing them into the internal queue,
// wrapping the original item into a timedElement along with its event time.
// It starts a goroutine to capture and emit a new window every sliding interval
// after receiving the first element.
func (sw *SlidingWindow[T]) stream() {
	processElement := func(element T) {
		eventTime := sw.eventTime(element)

		sw.mu.Lock()
		defer sw.mu.Unlock()

		// skip events older than the window lower boundary
		if eventTime.Before(sw.lowerBoundary) {
			return
		}

		// add the element to the internal queue
		timed := timedElement[T]{
			element:   element,
			eventTime: eventTime,
		}
		sw.queue = append(sw.queue, timed)
	}

	// Read the first element from the input channel. Its event time will determine
	// the lower boundary for the first sliding window.
	element, ok := <-sw.in
	if !ok {
		// The input channel has been closed by the upstream operator, indicating
		// that no more data will be received. Signal the completion of the sliding
		// window by closing the output channel and return.
		close(sw.out)
		return
	}

	// calculate the window start time and process the element
	eventTime := sw.eventTime(element.(T))
	var delta time.Duration
	sw.lowerBoundary, delta = sw.calculateWindowStart(eventTime)
	processElement(element.(T))

	// start a goroutine to capture and emit a new window every
	// sliding interval
	go sw.emit(delta)

	// process incoming stream elements
	for element := range sw.in {
		processElement(element.(T))
	}

	// signal upstream completion
	close(sw.done)
}

// emit periodically captures and emits completed sliding windows every
// sw.slidingInterval.
// The emission process begins after an initial delay calculated based on
// AllowedLateness, EmitPartialWindow, and the time difference between the
// start of the first window and the current time (delta).
func (sw *SlidingWindow[T]) emit(delta time.Duration) {
	defer close(sw.out)

	// calculate the initial delay
	initialDelay := sw.opts.AllowedLateness
	if !sw.opts.EmitPartialWindow {
		initialDelay += sw.windowSize - sw.slidingInterval - delta
	}

	// Wait for the first window iteration, using a timer.
	// If sw.done is signaled before the timer expires, the function returns.
	timer := time.NewTimer(initialDelay)
	select {
	case <-timer.C:
	case <-sw.done:
		timer.Stop()
		if sw.opts.EmitPartialWindow {
			sw.dispatchWindow()
		}
		return
	}

	// create a ticker for periodic emission of sliding windows
	ticker := time.NewTicker(sw.slidingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// dispatch the current window
			sw.dispatchWindow()

		case <-sw.done:
			// on shutdown, dispatch one final window to ensure all remaining
			// data is processed and return
			sw.dispatchWindow()
			return
		}
	}
}

// dispatchWindow is responsible for sending the elements in the current
// window to the output channel and moving the window to the next position.
func (sw *SlidingWindow[T]) dispatchWindow() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// sort elements in the queue by their time
	sort.Slice(sw.queue, func(i, j int) bool {
		return sw.queue[i].eventTime.Before(sw.queue[j].eventTime)
	})

	// extract current window elements
	windowElements := sw.extractWindowElements()

	// send elements downstream if the current window is not empty
	if len(windowElements) > 0 {
		sw.out <- windowElements
	}
}

// extractWindowElements extracts and returns elements from the sliding window that
// fall within the current window. Elements newer than tick will not be included.
// The sliding window queue is updated to remove previous interval elements.
func (sw *SlidingWindow[T]) extractWindowElements() []T {
	// Calculate the upper boundary of the current sliding window.
	// Elements with the event time less than or equal to this boundary will be
	// included.
	upperBoundary := sw.lowerBoundary.Add(sw.windowSize)
	// Advance the lower boundary of the sliding window by the sliding interval to
	// define the start of the next window.
	sw.lowerBoundary = sw.lowerBoundary.Add(sw.slidingInterval)

	elements := make([]T, 0, len(sw.queue))
	var remainingElements []timedElement[T]
queueLoop:
	for i, element := range sw.queue {
		if remainingElements == nil && element.eventTime.After(sw.lowerBoundary) {
			// copy remaining elements
			remainingElements = make([]timedElement[T], len(sw.queue)-i)
			_ = copy(remainingElements, sw.queue[i:])
		}
		switch {
		case !element.eventTime.After(upperBoundary):
			elements = append(elements, element.element)
		default:
			break queueLoop // we can break since the queue is ordered
		}
	}

	// move the window
	sw.queue = remainingElements

	return elements
}

// calculateWindowStart calculates the start time of the sliding window to
// which the event belongs, and the duration between the event time and the
// start time of that window.
func (sw *SlidingWindow[T]) calculateWindowStart(
	eventTime time.Time,
) (time.Time, time.Duration) {
	if eventTime.IsZero() {
		return eventTime, 0
	}

	// convert the event time to a Unix Nano timestamp
	// (nanoseconds since epoch)
	eventTimeNanos := eventTime.UnixNano()

	// calculate the window start in nanoseconds
	delta := eventTimeNanos % sw.slidingInterval.Nanoseconds()
	windowStartNanos := eventTimeNanos - delta

	return time.Unix(0, windowStartNanos).In(eventTime.Location()),
		time.Duration(delta)
}
