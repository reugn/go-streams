package flow

import (
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

//ThrottleMode defines Throttler behavior on buffer overflow
type ThrottleMode int8

const (
	// Backpressure on overflow mode
	Backpressure ThrottleMode = iota
	// Discard on overflow mode
	Discard
)

//Throttler limits the throughput to a specific number of elements per time unit
type Throttler struct {
	sync.Mutex
	elements uint
	per      time.Duration
	mode     ThrottleMode
	in       chan interface{}
	out      chan interface{}
	notify   chan interface{}
	counter  uint
}

//NewThrottler returns new Throttler instance
//elements - number of elements
//per - time unit
//buffer - buffer channel size
//mode - flow behavior on buffer overflow
func NewThrottler(elements uint, per time.Duration, buffer uint, mode ThrottleMode) *Throttler {
	throttler := &Throttler{
		elements: elements,
		per:      per,
		mode:     mode,
		in:       make(chan interface{}),
		out:      make(chan interface{}, buffer),
		notify:   make(chan interface{}),
		counter:  0,
	}
	go throttler.resetCounterLoop(per)
	go throttler.bufferize()
	return throttler
}

//count next element
func (th *Throttler) incrementCounter() {
	th.Lock()
	defer th.Unlock()
	th.counter++
}

func (th *Throttler) quotaHit() bool {
	return th.counter == th.elements
}

//scheduled elements counter refresher
func (th *Throttler) resetCounterLoop(after time.Duration) {
	for {
		select {
		case <-time.After(after):
			th.Lock()
			if th.quotaHit() {
				th.doNotify()
			}
			th.counter = 0
			th.Unlock()
		}
	}
}

func (th *Throttler) doNotify() {
	select {
	case th.notify <- struct{}{}:
	default:
	}
}

//prefill buffer
func (th *Throttler) bufferize() {
	defer close(th.out)
	if th.mode == Discard {
		for e := range th.in {
			select {
			case th.out <- e:
			default:
			}
		}
	} else if th.mode == Backpressure {
		for e := range th.in {
			th.out <- e
		}
	} else {
		panic("Unsupported ThrottleMode")
	}
}

// Via streams data through given flow
func (th *Throttler) Via(flow streams.Flow) streams.Flow {
	go th.doStream(flow)
	return flow
}

// To streams data to given sink
func (th *Throttler) To(sink streams.Sink) {
	th.doStream(sink)
}

// Out returns channel for sending data
func (th *Throttler) Out() <-chan interface{} {
	return th.out
}

// In returns channel for receiving data
func (th *Throttler) In() chan<- interface{} {
	return th.in
}

func (th *Throttler) doStream(inlet streams.Inlet) {
	for elem := range th.Out() {
		if th.quotaHit() {
			<-th.notify
		}
		th.incrementCounter()
		inlet.In() <- elem
	}
	close(inlet.In())
}
