package flow

import (
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// ThrottleMode defines the Throttler behavior on buffer overflow.
type ThrottleMode int8

const (
	// Backpressure on overflow mode.
	Backpressure ThrottleMode = iota
	// Discard elements on overflow mode.
	Discard
)

// Throttler limits the throughput to a specific number of elements per time unit.
type Throttler struct {
	sync.Mutex
	elements uint
	period   time.Duration
	mode     ThrottleMode
	in       chan interface{}
	out      chan interface{}
	notify   chan interface{}
	counter  uint
}

// Verify Throttler satisfies the Flow interface.
var _ streams.Flow = (*Throttler)(nil)

// NewThrottler returns a new Throttler instance.
// elements is the maximum number of elements to be produced per the given period of time.
// bufferSize defines the incoming elements buffer size.
// mode defines the Throttler flow behavior on elements buffer overflow.
func NewThrottler(elements uint, period time.Duration, bufferSize uint, mode ThrottleMode) *Throttler {
	throttler := &Throttler{
		elements: elements,
		period:   period,
		mode:     mode,
		in:       make(chan interface{}),
		out:      make(chan interface{}, bufferSize),
		notify:   make(chan interface{}),
		counter:  0,
	}
	go throttler.resetCounterLoop(period)
	go throttler.bufferize()
	return throttler
}

// count the next element.
func (th *Throttler) incrementCounter() {
	th.Lock()
	defer th.Unlock()
	th.counter++
}

func (th *Throttler) quotaHit() bool {
	return th.counter == th.elements
}

// the scheduled elements counter refresher.
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

// start buffering incoming elements.
// panics on unsupported ThrottleMode.
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

// Via streams data through the given flow
func (th *Throttler) Via(flow streams.Flow) streams.Flow {
	go th.doStream(flow)
	return flow
}

// To streams data to the given sink
func (th *Throttler) To(sink streams.Sink) {
	th.doStream(sink)
}

// Out returns an output channel for sending data
func (th *Throttler) Out() <-chan interface{} {
	return th.out
}

// In returns an input channel for receiving data
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
