package flow

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/reugn/go-streams"
)

// ThrottleMode represents Throttler's processing behavior when its element
// buffer overflows.
type ThrottleMode int8

const (
	// Backpressure slows down upstream ingestion when the element buffer overflows.
	Backpressure ThrottleMode = iota

	// Discard drops incoming elements when the element buffer overflows.
	Discard
)

// Throttler limits the throughput to a specific number of elements per time unit.
type Throttler struct {
	maxElements uint64
	period      time.Duration
	mode        ThrottleMode
	in          chan any
	out         chan any
	quotaSignal chan struct{}
	done        chan struct{}
	counter     uint64
}

// Verify Throttler satisfies the Flow interface.
var _ streams.Flow = (*Throttler)(nil)

// NewThrottler returns a new Throttler instance.
//
// elements is the maximum number of elements to be produced per the given period of time.
// bufferSize specifies the buffer size for incoming elements.
// mode specifies the processing behavior when the elements buffer overflows.
func NewThrottler(elements uint, period time.Duration, bufferSize uint, mode ThrottleMode) *Throttler {
	throttler := &Throttler{
		maxElements: uint64(elements),
		period:      period,
		mode:        mode,
		in:          make(chan any),
		out:         make(chan any, bufferSize),
		quotaSignal: make(chan struct{}),
		done:        make(chan struct{}),
	}
	go throttler.resetQuotaCounterLoop()
	go throttler.bufferize()

	return throttler
}

// quotaExceeded checks whether the quota per time unit has been exceeded.
func (th *Throttler) quotaExceeded() bool {
	return atomic.LoadUint64(&th.counter) >= th.maxElements
}

// resetQuotaCounterLoop resets the throttler quota counter every th.period
// and sends a release notification to the downstream processor.
func (th *Throttler) resetQuotaCounterLoop() {
	ticker := time.NewTicker(th.period)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			atomic.StoreUint64(&th.counter, 0)
			th.notifyQuotaReset() // send quota reset

		case <-th.done:
			return
		}
	}
}

// notifyQuotaReset notifies the downstream processor with quota reset.
func (th *Throttler) notifyQuotaReset() {
	select {
	case th.quotaSignal <- struct{}{}:
	default:
	}
}

// bufferize starts buffering incoming elements.
// The method will panic if an unsupported ThrottleMode is specified.
func (th *Throttler) bufferize() {
	switch th.mode {
	case Discard:
		for element := range th.in {
			select {
			case th.out <- element:
			default:
			}
		}
	case Backpressure:
		for element := range th.in {
			th.out <- element
		}
	default:
		panic(fmt.Sprintf("Unsupported ThrottleMode: %d", th.mode))
	}
	close(th.out)
}

// Via streams data through the given flow
func (th *Throttler) Via(flow streams.Flow) streams.Flow {
	go th.streamPortioned(flow)
	return flow
}

// To streams data to the given sink
func (th *Throttler) To(sink streams.Sink) {
	th.streamPortioned(sink)
}

// Out returns an output channel for sending data
func (th *Throttler) Out() <-chan any {
	return th.out
}

// In returns an input channel for receiving data
func (th *Throttler) In() chan<- any {
	return th.in
}

// streamPortioned streams elements to the next Inlet.
// Subsequent processing of elements will be suspended when the quota limit is reached
// until the next quota reset event.
func (th *Throttler) streamPortioned(inlet streams.Inlet) {
	for element := range th.out {
		if th.quotaExceeded() {
			<-th.quotaSignal // wait for quota reset
		}
		atomic.AddUint64(&th.counter, 1)
		inlet.In() <- element
	}
	close(th.done)
	close(inlet.In())
}
