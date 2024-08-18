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
	maxElements int64
	period      time.Duration
	mode        ThrottleMode
	in          chan any
	out         chan any
	quotaSignal chan struct{}
	done        chan struct{}
	counter     int64
}

// Verify Throttler satisfies the Flow interface.
var _ streams.Flow = (*Throttler)(nil)

// NewThrottler returns a new Throttler operator.
//
// elements is the maximum number of elements to be produced per the given period of time.
// bufferSize specifies the buffer size for incoming elements.
// mode specifies the processing behavior when the elements buffer overflows.
//
// If elements or bufferSize are not positive, NewThrottler will panic.
func NewThrottler(elements int, period time.Duration, bufferSize int, mode ThrottleMode) *Throttler {
	if elements < 1 {
		panic(fmt.Sprintf("nonpositive elements number: %d", elements))
	}
	if bufferSize < 1 {
		panic(fmt.Sprintf("nonpositive buffer size: %d", bufferSize))
	}
	throttler := &Throttler{
		maxElements: int64(elements),
		period:      period,
		mode:        mode,
		in:          make(chan any),
		out:         make(chan any, bufferSize),
		quotaSignal: make(chan struct{}),
		done:        make(chan struct{}),
	}
	go throttler.resetQuotaCounterLoop()
	go throttler.buffer()

	return throttler
}

// quotaExceeded checks whether the quota per time unit has been exceeded.
func (th *Throttler) quotaExceeded() bool {
	return atomic.LoadInt64(&th.counter) >= th.maxElements
}

// resetQuotaCounterLoop resets the throttler quota counter every th.period
// and sends a release notification to the downstream processor.
func (th *Throttler) resetQuotaCounterLoop() {
	ticker := time.NewTicker(th.period)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			atomic.StoreInt64(&th.counter, 0)
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

// buffer starts buffering incoming elements.
// If an unsupported ThrottleMode was specified, buffer will panic.
func (th *Throttler) buffer() {
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

// Via streams data to a specified Flow and returns it.
func (th *Throttler) Via(flow streams.Flow) streams.Flow {
	go th.streamPortioned(flow)
	return flow
}

// To streams data to a specified Sink.
func (th *Throttler) To(sink streams.Sink) {
	th.streamPortioned(sink)
}

// Out returns the output channel of the Throttler operator.
func (th *Throttler) Out() <-chan any {
	return th.out
}

// In returns the input channel of the Throttler operator.
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
		atomic.AddInt64(&th.counter, 1)
		inlet.In() <- element
	}
	close(th.done)
	close(inlet.In())
}
