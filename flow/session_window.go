package flow

import (
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// SessionWindow generates groups of elements by sessions of activity.
// Session windows do not overlap and do not have a fixed start and end time.
// T indicates the incoming element type, and the outgoing element type is []T.
type SessionWindow[T any] struct {
	mu            sync.Mutex
	inactivityGap time.Duration
	in            chan any
	out           chan any
	reset         chan struct{}
	done          chan struct{}
	buffer        []T
}

// Verify SessionWindow satisfies the Flow interface.
var _ streams.Flow = (*SessionWindow[any])(nil)

// NewSessionWindow returns a new SessionWindow operator.
// T specifies the incoming element type, and the outgoing element type is []T.
//
// inactivityGap is the gap of inactivity that closes a session window when occurred.
func NewSessionWindow[T any](inactivityGap time.Duration) *SessionWindow[T] {
	sessionWindow := &SessionWindow[T]{
		inactivityGap: inactivityGap,
		in:            make(chan any),
		out:           make(chan any),
		reset:         make(chan struct{}),
		done:          make(chan struct{}),
	}
	go sessionWindow.emit()
	go sessionWindow.receive()

	return sessionWindow
}

// Via streams data to a specified Flow and returns it.
func (sw *SessionWindow[T]) Via(flow streams.Flow) streams.Flow {
	go sw.transmit(flow)
	return flow
}

// To streams data to a specified Sink.
func (sw *SessionWindow[T]) To(sink streams.Sink) {
	sw.transmit(sink)
}

// Out returns the output channel of the SessionWindow operator.
func (sw *SessionWindow[T]) Out() <-chan any {
	return sw.out
}

// In returns the input channel of the SessionWindow operator.
func (sw *SessionWindow[T]) In() chan<- any {
	return sw.in
}

// transmit submits closed windows to the next Inlet.
func (sw *SessionWindow[T]) transmit(inlet streams.Inlet) {
	for window := range sw.out {
		inlet.In() <- window
	}
	close(inlet.In())
}

// receive buffers the incoming elements.
// It resets the inactivity timer on each new element.
func (sw *SessionWindow[T]) receive() {
	for element := range sw.in {
		sw.mu.Lock()
		sw.buffer = append(sw.buffer, element.(T))
		sw.mu.Unlock()
		sw.notifyTimerReset() // signal to reset the inactivity timer
	}
	close(sw.done)
}

// notifyTimerReset sends a notification to reset the inactivity timer.
func (sw *SessionWindow[T]) notifyTimerReset() {
	select {
	case sw.reset <- struct{}{}:
	default:
	}
}

// emit captures and emits a session window based on the gap of inactivity.
// When this period expires, the current session closes and subsequent elements
// are assigned to a new session window.
func (sw *SessionWindow[T]) emit() {
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
func (sw *SessionWindow[T]) dispatchWindow() {
	sw.mu.Lock()
	windowElements := sw.buffer
	sw.buffer = nil
	sw.mu.Unlock()

	// send elements if the window is not empty
	if len(windowElements) > 0 {
		sw.out <- windowElements
	}
}
