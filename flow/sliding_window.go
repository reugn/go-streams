package flow

import (
	"sort"
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// timedElement stores an incoming element along with its timestamp.
type timedElement[T any] struct {
	element   T
	timestamp int64
}

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
	queue              []timedElement[T]
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
//
// NewSlidingWindow panics if slidingInterval is larger than windowSize.
func NewSlidingWindow[T any](
	windowSize time.Duration,
	slidingInterval time.Duration) *SlidingWindow[T] {
	return NewSlidingWindowWithExtractor[T](windowSize, slidingInterval, nil)
}

// NewSlidingWindowWithExtractor returns a new SlidingWindow operator based on event time.
// Event time is the time that each individual event occurred on its producing device.
// Gives correct results on out-of-order events, late events, or on replays of data.
// T specifies the incoming element type, and the outgoing element type is []T.
//
// windowSize is the Duration of generated windows.
// slidingInterval is the sliding interval of generated windows.
// timestampExtractor is the record timestamp (in nanoseconds) extractor.
//
// NewSlidingWindowWithExtractor panics if slidingInterval is larger than windowSize.
func NewSlidingWindowWithExtractor[T any](
	windowSize time.Duration,
	slidingInterval time.Duration,
	timestampExtractor func(T) int64) *SlidingWindow[T] {

	if windowSize < slidingInterval {
		panic("sliding interval is larger than window size")
	}

	slidingWindow := &SlidingWindow[T]{
		windowSize:         windowSize,
		slidingInterval:    slidingInterval,
		queue:              make([]timedElement[T], 0),
		in:                 make(chan any),
		out:                make(chan any),
		done:               make(chan struct{}),
		timestampExtractor: timestampExtractor,
	}
	go slidingWindow.receive()

	return slidingWindow
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
// Otherwise, the system time is returned.
func (sw *SlidingWindow[T]) timestamp(element T) int64 {
	if sw.timestampExtractor == nil {
		return time.Now().UnixNano()
	}
	return sw.timestampExtractor(element)
}

// receive buffers the incoming elements by pushing them into the queue,
// wrapping the original item into a timedElement along with its timestamp.
func (sw *SlidingWindow[T]) receive() {
	for element := range sw.in {
		sw.Lock()
		timed := timedElement[T]{
			element:   element.(T),
			timestamp: sw.timestamp(element.(T)),
		}
		sw.queue = append(sw.queue, timed)
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

// dispatchWindow is responsible for sending the elements in the current
// window to the output channel and moving the window to the next position.
func (sw *SlidingWindow[T]) dispatchWindow(tick time.Time) {
	sw.Lock()

	// sort elements in the queue by their timestamp
	sort.Slice(sw.queue, func(i, j int) bool {
		return sw.queue[i].timestamp < sw.queue[j].timestamp
	})

	// calculate the next window start time
	nextWindowStartTime := tick.Add(-sw.windowSize).Add(sw.slidingInterval).UnixNano()
	// initialize the next window queue
	var nextWindowQueue []timedElement[T]
	for i, element := range sw.queue {
		if element.timestamp > nextWindowStartTime {
			nextWindowQueue = make([]timedElement[T], len(sw.queue)-i)
			_ = copy(nextWindowQueue, sw.queue[i:])
			break
		}
	}

	// extract current window elements
	windowElements := extractWindowElements(sw.queue, tick.UnixNano())
	// move the window
	sw.queue = nextWindowQueue

	sw.Unlock()

	// send elements downstream if the current window is not empty
	if len(windowElements) > 0 {
		sw.out <- windowElements
	}
}

// extractWindowElements extracts current window elements from the given slice
// of timedElement. Elements newer than now will not be included.
func extractWindowElements[T any](timed []timedElement[T], now int64) []T {
	elements := make([]T, 0, len(timed))
	for _, timedElement := range timed {
		if timedElement.timestamp < now {
			elements = append(elements, timedElement.element)
		} else {
			break // we can break since the input is an ordered slice
		}
	}
	return elements
}
