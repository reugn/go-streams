package flow

import (
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// TumblingWindow assigns each element to a window of a specified window size.
// Tumbling windows have a fixed size and do not overlap.
type TumblingWindow struct {
	sync.Mutex
	windowSize time.Duration
	in         chan interface{}
	out        chan interface{}
	done       chan struct{}
	buffer     []interface{}
}

// Verify TumblingWindow satisfies the Flow interface.
var _ streams.Flow = (*TumblingWindow)(nil)

// NewTumblingWindow returns a new TumblingWindow instance.
//
// size is the Duration of generated windows.
func NewTumblingWindow(size time.Duration) *TumblingWindow {
	tumblingWindow := &TumblingWindow{
		windowSize: size,
		in:         make(chan interface{}),
		out:        make(chan interface{}),
		done:       make(chan struct{}),
	}
	go tumblingWindow.receive()
	go tumblingWindow.emit()

	return tumblingWindow
}

// Via streams data through the given flow
func (tw *TumblingWindow) Via(flow streams.Flow) streams.Flow {
	go tw.transmit(flow)
	return flow
}

// To streams data to the given sink
func (tw *TumblingWindow) To(sink streams.Sink) {
	tw.transmit(sink)
}

// Out returns an output channel for sending data
func (tw *TumblingWindow) Out() <-chan interface{} {
	return tw.out
}

// In returns an input channel for receiving data
func (tw *TumblingWindow) In() chan<- interface{} {
	return tw.in
}

// transmit submits closed windows to the next Inlet.
func (tw *TumblingWindow) transmit(inlet streams.Inlet) {
	for window := range tw.out {
		inlet.In() <- window
	}
	close(inlet.In())
}

// receive buffers the incoming elements.
func (tw *TumblingWindow) receive() {
	for element := range tw.in {
		tw.Lock()
		tw.buffer = append(tw.buffer, element)
		tw.Unlock()
	}
	close(tw.done)
}

// emit captures and emits a new window based on the fixed time interval.
func (tw *TumblingWindow) emit() {
	ticker := time.NewTicker(tw.windowSize)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tw.dispatchWindow()

		case <-tw.done:
			tw.dispatchWindow()
			close(tw.out)
			return
		}
	}
}

// dispatchWindow creates a window from buffered elements and resets the buffer.
// It sends the slice of elements to the output channel if the window is not empty.
func (tw *TumblingWindow) dispatchWindow() {
	tw.Lock()
	windowElements := tw.buffer
	tw.buffer = nil
	tw.Unlock()

	// send elements if the window is not empty
	if len(windowElements) > 0 {
		tw.out <- windowElements
	}
}
