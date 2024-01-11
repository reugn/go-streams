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
	in            chan interface{}
	out           chan interface{}
	reset         chan struct{}
	done          chan struct{}
	buffer        []interface{}
}

// Verify SessionWindow satisfies the Flow interface.
var _ streams.Flow = (*SessionWindow)(nil)

// NewSessionWindow returns a new SessionWindow instance.
//
// inactivityGap is the gap of inactivity that closes a session window when occurred.
func NewSessionWindow(inactivityGap time.Duration) *SessionWindow {
	sessionWindow := &SessionWindow{
		inactivityGap: inactivityGap,
		in:            make(chan interface{}),
		out:           make(chan interface{}),
		reset:         make(chan struct{}),
		done:          make(chan struct{}),
	}
	go sessionWindow.emit()
	go sessionWindow.receive()

	return sessionWindow
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

// transmit submits closed windows to the next Inlet.
func (sw *SessionWindow) transmit(inlet streams.Inlet) {
	for window := range sw.out {
		inlet.In() <- window
	}
	close(inlet.In())
}

// receive buffers the incoming elements.
// It resets the inactivity timer on each new element.
func (sw *SessionWindow) receive() {
	for element := range sw.in {
		sw.Lock()
		sw.buffer = append(sw.buffer, element)
		sw.Unlock()
		sw.notifyTimerReset() // signal to reset the inactivity timer
	}
	close(sw.done)
}

// notifyTimerReset sends a notification to reset the inactivity timer.
func (sw *SessionWindow) notifyTimerReset() {
	select {
	case sw.reset <- struct{}{}:
	default:
	}
}

// emit captures and emits a session window based on the gap of inactivity.
// When this period expires, the current session closes and subsequent elements
// are assigned to a new session window.
func (sw *SessionWindow) emit() {
	timer := time.NewTimer(sw.inactivityGap)
	for {
		select {
		case <-timer.C:
			sw.dispatchWindow()

		case <-sw.reset:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(sw.inactivityGap)

		case <-sw.done:
			timer.Stop()
			sw.dispatchWindow()
			close(sw.out)
			return
		}
	}
}

// dispatchWindow creates a window from buffered elements and resets the buffer.
// It sends the slice of elements to the output channel if the window is not empty.
func (sw *SessionWindow) dispatchWindow() {
	sw.Lock()
	windowElements := sw.buffer
	sw.buffer = nil
	sw.Unlock()

	// send elements if the window is not empty
	if len(windowElements) > 0 {
		sw.out <- windowElements
	}
}
