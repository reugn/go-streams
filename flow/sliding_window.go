package flow

import (
	"container/heap"
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// SlidingWindow flow
// size  - The size of the generated windows
// slide - The slide interval of the generated windows
type SlidingWindow struct {
	sync.Mutex
	size  time.Duration
	slide time.Duration
	queue *PriorityQueue
	in    chan interface{}
	out   chan interface{}
}

func NewSlidingWindow(size time.Duration, slide time.Duration) *SlidingWindow {
	window := &SlidingWindow{
		size:  size,
		slide: slide,
		queue: &PriorityQueue{},
		in:    make(chan interface{}),
		out:   make(chan interface{}), //windows channel
	}
	go window.receive()
	go window.emit()
	return window
}

func (sw *SlidingWindow) Via(flow streams.Flow) streams.Flow {
	go sw.transmit(flow)
	return flow
}

func (sw *SlidingWindow) To(sink streams.Sink) {
	sw.transmit(sink)
}

func (sw *SlidingWindow) Out() <-chan interface{} {
	return sw.out
}

func (sw *SlidingWindow) In() chan<- interface{} {
	return sw.in
}

// retransmit emitted window to the next Inlet
func (sw *SlidingWindow) transmit(inlet streams.Inlet) {
	for elem := range sw.Out() {
		inlet.In() <- elem
	}
	close(inlet.In())
}

func (sw *SlidingWindow) receive() {
	for elem := range sw.in {
		item := &Item{elem, streams.NowNano(), 0}
		sw.Lock()
		heap.Push(sw.queue, item)
		sw.Unlock()
	}
	close(sw.out)
}

//triggered by slide interval
func (sw *SlidingWindow) emit() {
	for {
		select {
		case <-time.After(sw.slide):
			sw.Lock()
			//build window slice and send to out chan
			var slideUpperIndex, windowBottomIndex int
			now := streams.NowNano()
			windowUpperIndex := sw.queue.Len()
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
				s := sw.queue.Slice(slideUpperIndex+1, windowUpperIndex)
				//reset queue
				sw.queue = &s
				heap.Init(sw.queue)
			}
			sw.Unlock()
			//send to out chan
			if len(windowSlice) > 0 {
				sw.out <- windowSlice
			}
		}
	}
}

func extract(items []*Item) []interface{} {
	rt := make([]interface{}, len(items))
	for i, item := range items {
		rt[i] = item.Msg
	}
	return rt
}
