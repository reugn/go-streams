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
	window := &TumblingWindow{
		windowSize: size,
		in:         make(chan interface{}),
		out:        make(chan interface{}),
		done:       make(chan struct{}),
	}
	go window.receive()
	go window.emit()

	return window
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

// submit emitted windows to the next Inlet
func (tw *TumblingWindow) transmit(inlet streams.Inlet) {
	for elem := range tw.Out() {
		inlet.In() <- elem
	}
	close(inlet.In())
}

func (tw *TumblingWindow) receive() {
	for elem := range tw.in {
		tw.Lock()
		tw.buffer = append(tw.buffer, elem)
		tw.Unlock()
	}
	close(tw.done)
	close(tw.out)
}

// emit generates and emits a new window.
func (tw *TumblingWindow) emit() {
	ticker := time.NewTicker(tw.windowSize)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tw.Lock()
			windowSlice := tw.buffer
			tw.buffer = nil
			tw.Unlock()

			// send the window slice to the out chan
			if len(windowSlice) > 0 {
				tw.out <- windowSlice
			}

		case <-tw.done:
			return
		}
	}
}
