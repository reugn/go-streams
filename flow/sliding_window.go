package flow

import (
	"container/heap"
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// SlidingWindow assigns elements to windows of fixed length configured by the window size parameter.
// An additional window slide parameter controls how frequently a sliding window is started.
// Hence, sliding windows can be overlapping if the slide is smaller than the window size.
// In this case elements are assigned to multiple windows.
type SlidingWindow struct {
	sync.Mutex
	size               time.Duration
	slide              time.Duration
	queue              *PriorityQueue
	in                 chan interface{}
	out                chan interface{}
	done               chan struct{}
	timestampExtractor func(interface{}) int64
}

// Verify SlidingWindow satisfies the Flow interface.
var _ streams.Flow = (*SlidingWindow)(nil)

// NewSlidingWindow returns a new processing time based SlidingWindow.
// Processing time refers to the system time of the machine that is executing the respective operation.
// size is the size of the generated windows.
// slide is the sliding interval of the generated windows.
func NewSlidingWindow(size time.Duration, slide time.Duration) *SlidingWindow {
	return NewSlidingWindowWithTsExtractor(size, slide, nil)
}

// NewSlidingWindowWithTsExtractor returns a new event time based SlidingWindow.
// Event time is the time that each individual event occurred on its producing device.
// Gives correct results on out-of-order events, late events, or on replays of data.
// size is the size of the generated windows.
// slide is the sliding interval of the generated windows.
// timestampExtractor is the record timestamp (in nanoseconds) extractor.
func NewSlidingWindowWithTsExtractor(size time.Duration, slide time.Duration,
	timestampExtractor func(interface{}) int64) *SlidingWindow {
	window := &SlidingWindow{
		size:               size,
		slide:              slide,
		queue:              &PriorityQueue{},
		in:                 make(chan interface{}),
		out:                make(chan interface{}), // windows channel
		done:               make(chan struct{}),
		timestampExtractor: timestampExtractor,
	}
	go window.receive()
	go window.emit()
	return window
}

// Via streams data through the given flow
func (sw *SlidingWindow) Via(flow streams.Flow) streams.Flow {
	go sw.transmit(flow)
	return flow
}

// To streams data to the given sink
func (sw *SlidingWindow) To(sink streams.Sink) {
	sw.transmit(sink)
}

// Out returns an output channel for sending data
func (sw *SlidingWindow) Out() <-chan interface{} {
	return sw.out
}

// In returns an input channel for receiving data
func (sw *SlidingWindow) In() chan<- interface{} {
	return sw.in
}

// submit emitted windows to the next Inlet
func (sw *SlidingWindow) transmit(inlet streams.Inlet) {
	for elem := range sw.Out() {
		inlet.In() <- elem
	}
	close(inlet.In())
}

// extract timestamp from the record if the timestampExtractor is set
// return the system clock time otherwise
func (sw *SlidingWindow) timestamp(elem interface{}) int64 {
	if sw.timestampExtractor == nil {
		return streams.NowNano()
	}
	return sw.timestampExtractor(elem)
}

func (sw *SlidingWindow) receive() {
	for elem := range sw.in {
		item := &Item{elem, sw.timestamp(elem), 0}
		sw.Lock()
		heap.Push(sw.queue, item)
		sw.Unlock()
	}
	close(sw.done)
	close(sw.out)
}

// emit is triggered by the sliding interval
func (sw *SlidingWindow) emit() {
	for {
		select {
		case <-time.After(sw.slide):
			sw.Lock()
			// build a window slice and send it to the out chan
			var windowBottomIndex int
			now := streams.NowNano()
			windowUpperIndex := sw.queue.Len()
			slideUpperIndex := windowUpperIndex
			slideUpperTime := now - sw.size.Nanoseconds() + sw.slide.Nanoseconds()
			windowBottomTime := now - sw.size.Nanoseconds()
			for i, item := range *sw.queue {
				if item.epoch < windowBottomTime {
					windowBottomIndex = i
				}
				if item.epoch > slideUpperTime {
					slideUpperIndex = i
					break
				}
			}
			windowSlice := extract(sw.queue.Slice(windowBottomIndex, windowUpperIndex))
			if windowUpperIndex > 0 {
				s := sw.queue.Slice(slideUpperIndex, windowUpperIndex)
				// reset the queue
				sw.queue = &s
				heap.Init(sw.queue)
			}
			sw.Unlock()

			// send window slice to the out chan
			if len(windowSlice) > 0 {
				sw.out <- windowSlice
			}

		case <-sw.done:
			return
		}
	}
}

// generate a window
func extract(items []*Item) []interface{} {
	rt := make([]interface{}, len(items))
	for i, item := range items {
		rt[i] = item.Msg
	}
	return rt
}
