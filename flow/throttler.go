package flow

import (
	"sync/atomic"
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
	maxElements uint64
	period      time.Duration
	mode        ThrottleMode
	in          chan interface{}
	out         chan interface{}
	notify      chan struct{}
	done        chan struct{}
	counter     uint64
}

// Verify Throttler satisfies the Flow interface.
var _ streams.Flow = (*Throttler)(nil)

// NewThrottler returns a new Throttler instance.
//
// elements is the maximum number of elements to be produced per the given period of time.
// bufferSize defines the incoming elements buffer size.
// mode defines the Throttler flow behavior on elements buffer overflow.
func NewThrottler(elements uint, period time.Duration, bufferSize uint, mode ThrottleMode) *Throttler {
	throttler := &Throttler{
		maxElements: uint64(elements),
		period:      period,
		mode:        mode,
		in:          make(chan interface{}),
		out:         make(chan interface{}, bufferSize),
		notify:      make(chan struct{}),
		done:        make(chan struct{}),
		counter:     0,
	}
	go throttler.resetCounterLoop(period)
	go throttler.bufferize()

	return throttler
}

// incrementCounter increments the elements counter.
func (th *Throttler) incrementCounter() {
	atomic.AddUint64(&th.counter, 1)
}

// quotaHit verifies if the quota per time unit is exceeded.
func (th *Throttler) quotaHit() bool {
	return atomic.LoadUint64(&th.counter) >= th.maxElements
}

// resetCounterLoop is the scheduled quota refresher.
func (th *Throttler) resetCounterLoop(after time.Duration) {
	ticker := time.NewTicker(after)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if th.quotaHit() {
				atomic.StoreUint64(&th.counter, 0)
				th.doNotify()
			}

		case <-th.done:
			return
		}
	}
}

// doNotify notifies the producer goroutine with quota reset.
func (th *Throttler) doNotify() {
	select {
	case th.notify <- struct{}{}:
	default:
	}
}

// bufferize starts buffering incoming elements.
// panics on an unsupported ThrottleMode.
func (th *Throttler) bufferize() {
	switch th.mode {
	case Discard:
		for e := range th.in {
			select {
			case th.out <- e:
			default:
			}
		}
	case Backpressure:
		for e := range th.in {
			th.out <- e
		}
	default:
		panic("Unsupported ThrottleMode")
	}
	close(th.done)
	close(th.out)
	close(th.notify)
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

// doStream streams data to the next Inlet.
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
