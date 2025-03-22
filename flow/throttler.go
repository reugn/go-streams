package flow

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/reugn/go-streams"
)

// ThrottleMode defines the behavior of the Throttler when its internal buffer is full.
type ThrottleMode int8

const (
	// Backpressure instructs the Throttler to block upstream ingestion when its internal
	// buffer is full. This effectively slows down the producer, preventing data loss
	// and ensuring all elements are eventually processed, albeit at a reduced rate. This
	// mode can cause upstream operations to block indefinitely if the downstream consumer
	// cannot keep up.
	Backpressure ThrottleMode = iota
	// Discard instructs the Throttler to drop incoming elements when its internal buffer
	// is full. This mode prioritizes maintaining the target throughput rate, even at the
	// cost of data loss. Elements are silently dropped without any indication to the
	// upstream producer. Use this mode when data loss is acceptable.
	Discard
)

// Throttler limits the throughput to a specific number of elements per time unit.
type Throttler struct {
	period      time.Duration
	mode        ThrottleMode
	maxElements int64
	counter     atomic.Int64

	in          chan any
	out         chan any
	quotaSignal chan struct{}
	done        chan struct{}
}

// Verify Throttler satisfies the Flow interface.
var _ streams.Flow = (*Throttler)(nil)

// NewThrottler returns a new Throttler operator.
//
// The Throttler operator limits the rate at which elements are produced. It allows a
// maximum of 'elements' number of elements to be processed within a specified 'period'
// of time.
//
// elements is the maximum number of elements to be produced per the given period of time.
// bufferSize is the size of the internal buffer for incoming elements. This buffer
// temporarily holds elements waiting to be processed.
// mode specifies the processing behavior when the internal elements buffer is full.
// See [ThrottleMode] for available options.
//
// If elements or bufferSize are not positive, or if mode is not a supported
// ThrottleMode, NewThrottler will panic.
func NewThrottler(elements int, period time.Duration, bufferSize int, mode ThrottleMode) *Throttler {
	if elements < 1 {
		panic(fmt.Sprintf("nonpositive elements number: %d", elements))
	}
	if bufferSize < 1 {
		panic(fmt.Sprintf("nonpositive buffer size: %d", bufferSize))
	}
	if mode != Discard && mode != Backpressure {
		panic(fmt.Sprintf("unsupported ThrottleMode: %d", mode))
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
	return th.counter.Load() >= th.maxElements
}

// resetQuotaCounterLoop resets the throttler quota counter every th.period
// and notifies the downstream processing goroutine of the quota reset.
func (th *Throttler) resetQuotaCounterLoop() {
	ticker := time.NewTicker(th.period)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			th.counter.Store(0)
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

// buffer buffers incoming elements from the in channel by sending them
// to the out channel, adhering to the configured ThrottleMode.
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
	}
	close(th.out)
}

// Via asynchronously streams data to the given Flow and returns it.
func (th *Throttler) Via(flow streams.Flow) streams.Flow {
	go th.streamPortioned(flow)
	return flow
}

// To streams data to the given Sink and blocks until the Sink has completed
// processing all data.
func (th *Throttler) To(sink streams.Sink) {
	th.streamPortioned(sink)
	sink.AwaitCompletion()
}

// Out returns the output channel of the Throttler operator.
func (th *Throttler) Out() <-chan any {
	return th.out
}

// In returns the input channel of the Throttler operator.
func (th *Throttler) In() chan<- any {
	return th.in
}

// streamPortioned streams elements to the given Inlet, enforcing a quota.
// Elements are sent to inlet.In() until th.out is closed. If the quota is exceeded,
// the function blocks until a quota reset signal is received on th.quotaSignal.
func (th *Throttler) streamPortioned(inlet streams.Inlet) {
	for element := range th.out {
		if th.quotaExceeded() {
			<-th.quotaSignal // wait for quota reset
		}
		th.counter.Add(1)
		inlet.In() <- element
	}
	close(th.done)
	close(inlet.In())
}
