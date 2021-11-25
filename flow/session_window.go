package flow

import (
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// SessionWindow generates groups of elements by sessions of activity.
// Session windows do not overlap and do not have a fixed start and end time.
type SessionWindow struct {
	sync.Mutex
	inactivityGap time.Duration
	timer         *time.Timer
	in            chan interface{}
	out           chan interface{}
	done          chan struct{}
	buffer        []interface{}
}

// Verify SessionWindow satisfies the Flow interface.
var _ streams.Flow = (*SessionWindow)(nil)

// NewSessionWindow returns a new SessionWindow instance.
//
// inactivityGap is the gap of inactivity that closes a session window when occurred.
func NewSessionWindow(inactivityGap time.Duration) *SessionWindow {
	window := &SessionWindow{
		inactivityGap: inactivityGap,
		timer:         time.NewTimer(inactivityGap),
		in:            make(chan interface{}),
		out:           make(chan interface{}),
		done:          make(chan struct{}),
	}
	go window.emit()
	go window.receive()

	return window
}

// Via streams data through the given flow
func (sw *SessionWindow) Via(flow streams.Flow) streams.Flow {
	go sw.transmit(flow)
	return flow
}

// To streams data to the given sink
func (sw *SessionWindow) To(sink streams.Sink) {
	sw.transmit(sink)
}

// Out returns an output channel for sending data
func (sw *SessionWindow) Out() <-chan interface{} {
	return sw.out
}

// In returns an input channel for receiving data
func (sw *SessionWindow) In() chan<- interface{} {
	return sw.in
}

// submit emitted windows to the next Inlet
func (sw *SessionWindow) transmit(inlet streams.Inlet) {
	for elem := range sw.Out() {
		inlet.In() <- elem
	}
	close(inlet.In())
}

func (sw *SessionWindow) receive() {
	for elem := range sw.in {
		sw.Lock()
		sw.buffer = append(sw.buffer, elem)
		sw.timer.Reset(sw.inactivityGap)
		sw.Unlock()
	}
	close(sw.done)
	close(sw.out)
}

// emit generates and emits a new window.
func (sw *SessionWindow) emit() {
	defer sw.timer.Stop()

	for {
		select {
		case <-sw.timer.C:
			sw.Lock()
			windowSlice := sw.buffer
			sw.buffer = nil
			sw.Unlock()

			// send the window slice to the out chan
			if len(windowSlice) > 0 {
				sw.out <- windowSlice
			}

		case <-sw.done:
			return
		}
	}
}
